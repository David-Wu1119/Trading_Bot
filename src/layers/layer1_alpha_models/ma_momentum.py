#!/usr/bin/env python3
"""
Moving-Average Momentum Alpha Model (Layer 1)

MA momentum strategy:
- Keep in-memory deque of last N=30 mids per symbol
- When we have ≥30 points:
  - ma_short = mean(last 5)
  - ma_long = mean(last 30)
  - z = (ma_short-ma_long)/ma_long
  - edge_bps = 40 * z (cap at ±40 bp)
  - confidence = clip(0.55 + 10*|z|, 0.55, 0.9)
"""

import numpy as np
import logging
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime, timezone
from collections import deque
from dataclasses import dataclass

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class AlphaSignal:
    """Alpha signal output"""

    symbol: str
    edge_bps: float
    confidence: float
    timestamp: str
    reasoning: str


class MovingAverageMomentumAlpha:
    """
    Moving Average Momentum Alpha Model

    Generates alpha signals based on MA crossover momentum:
    - When short MA > long MA, expect continued upward momentum
    - Edge = 40 * z where z = (ma_short - ma_long) / ma_long
    - Edge capped at ±40 bp
    - Confidence increases with absolute z-score
    """

    def __init__(
        self,
        short_period: int = 5,
        long_period: int = 30,
        min_samples: int = 30,
        edge_scaling: float = 40.0,
        min_confidence: float = 0.55,
        max_confidence: float = 0.9,
    ):
        """
        Initialize MA momentum model.

        Args:
            short_period: Short MA period
            long_period: Long MA period (also min samples needed)
            min_samples: Minimum samples needed to generate signal
            edge_scaling: Scaling factor to convert z-score to basis points
            min_confidence: Minimum confidence level
            max_confidence: Maximum confidence level
        """
        self.short_period = short_period
        self.long_period = long_period
        self.min_samples = min_samples
        self.edge_scaling = edge_scaling
        self.min_confidence = min_confidence
        self.max_confidence = max_confidence

        # Price history for each symbol (keep last N=30 mids)
        self.mid_history: Dict[str, deque] = {}

        # Performance tracking
        self.signal_count = 0
        self.hit_count = 0

        logger.info(
            f"MA momentum alpha initialized: short={short_period}, long={long_period}, "
            f"min_samples={min_samples}, scaling={edge_scaling}"
        )

    def update_price(
        self, symbol: str, price: float, timestamp: str
    ) -> Optional[AlphaSignal]:
        """
        Update price and generate alpha signal if conditions are met.

        Args:
            symbol: Trading symbol
            price: Current mid price
            timestamp: Timestamp string

        Returns:
            AlphaSignal if signal generated, None otherwise
        """
        try:
            # Initialize history for new symbols
            if symbol not in self.mid_history:
                self.mid_history[symbol] = deque(maxlen=self.long_period)

            # Add current mid price
            self.mid_history[symbol].append(price)

            # Generate signal if we have enough data
            if len(self.mid_history[symbol]) >= self.min_samples:
                return self._generate_signal(symbol, timestamp)

        except Exception as e:
            logger.error(f"Error updating price for {symbol}: {e}")

        return None

    def predict(self, tick: Any) -> Tuple[float, float]:
        """Convenience wrapper to support legacy tests that pass tick objects."""

        symbol = getattr(tick, "symbol", "UNKNOWN")
        price = getattr(tick, "price", None)
        if price is None:
            price = getattr(tick, "last", None)
        if price is None:
            return 0.0, 0.0

        try:
            price_value = float(price)
        except (TypeError, ValueError):
            return 0.0, 0.0

        timestamp = getattr(tick, "timestamp", None)
        if isinstance(timestamp, datetime):
            timestamp_str = timestamp.astimezone(timezone.utc).isoformat()
        elif timestamp is not None:
            timestamp_str = str(timestamp)
        else:
            timestamp_str = datetime.now(timezone.utc).isoformat()

        signal = self.update_price(symbol, price_value, timestamp_str)
        if signal is None:
            # Do not emit synthetic alpha before warmup is complete.
            return 0.0, 0.0
        return signal.edge_bps, signal.confidence

    def _generate_signal(self, symbol: str, timestamp: str) -> Optional[AlphaSignal]:
        """Generate MA momentum signal based on MA crossover."""
        try:
            mids = list(self.mid_history[symbol])

            # Need at least min_samples points
            if len(mids) < self.min_samples:
                return None

            # Calculate moving averages
            ma_short = np.mean(mids[-self.short_period :])  # Last 5
            ma_long = np.mean(mids)  # All 30 points

            # Calculate z-score
            if ma_long == 0:
                return None  # Avoid division by zero

            z = (ma_short - ma_long) / ma_long

            # Calculate edge in basis points (cap at ±40 bp)
            edge_bps = self.edge_scaling * z
            edge_bps = np.clip(edge_bps, -self.edge_scaling, self.edge_scaling)

            # Calculate confidence
            confidence = self.min_confidence + 10 * abs(z)
            confidence = np.clip(confidence, self.min_confidence, self.max_confidence)

            # Only generate signal if edge is meaningful
            if abs(edge_bps) < 0.5:  # Reduced from 1.0bp to 0.5bp edge
                return None

            reasoning = (
                f"ma_short={ma_short:.2f}, ma_long={ma_long:.2f}, "
                f"z_score={z:.4f}, momentum={'UP' if z > 0 else 'DOWN'}"
            )

            signal = AlphaSignal(
                symbol=symbol,
                edge_bps=edge_bps,
                confidence=confidence,
                timestamp=timestamp,
                reasoning=reasoning,
            )

            self.signal_count += 1

            logger.debug(
                f"MA momentum signal for {symbol}: edge={edge_bps:.1f}bps, "
                f"confidence={confidence:.2f}, z_score={z:.4f}"
            )

            return signal

        except Exception as e:
            logger.error(f"Error generating signal for {symbol}: {e}")
            return None

    def update_from_feature_snapshot(self, feature_snapshot) -> Optional[AlphaSignal]:
        """
        Generate signal from FeatureBus snapshot.

        Args:
            feature_snapshot: FeatureSnapshot object with price data

        Returns:
            AlphaSignal if signal generated, None otherwise
        """
        try:
            # Extract mid price from feature snapshot
            if (
                hasattr(feature_snapshot, "mid_price")
                and feature_snapshot.mid_price is not None
            ):
                mid_price = float(feature_snapshot.mid_price)
            else:
                # No mid price available
                return None

            # Update with mid price and generate signal
            return self.update_price(
                symbol=feature_snapshot.symbol,
                price=mid_price,
                timestamp=feature_snapshot.timestamp.isoformat(),
            )

        except Exception as e:
            logger.error(f"Error generating signal from feature snapshot: {e}")
            return None

    def update_performance(
        self, symbol: str, realized_return_bps: float, predicted_edge_bps: float
    ):
        """Update performance tracking based on realized returns."""
        try:
            # Check if prediction was correct
            if (predicted_edge_bps > 0 and realized_return_bps > 0) or (
                predicted_edge_bps < 0 and realized_return_bps < 0
            ):
                self.hit_count += 1

            logger.debug(
                f"Performance update for {symbol}: predicted={predicted_edge_bps:.1f}bps, "
                f"realized={realized_return_bps:.1f}bps, hit_rate={self.get_hit_rate():.2%}"
            )

        except Exception as e:
            logger.error(f"Error updating performance: {e}")

    def get_hit_rate(self) -> float:
        """Get current hit rate."""
        if self.signal_count == 0:
            return 0.6  # Default optimistic for momentum
        return self.hit_count / self.signal_count

    def get_stats(self) -> Dict[str, any]:
        """Get model statistics."""
        return {
            "model_name": "ma_momentum_v0",
            "signal_count": self.signal_count,
            "hit_count": self.hit_count,
            "hit_rate": self.get_hit_rate(),
            "short_period": self.short_period,
            "long_period": self.long_period,
            "active_symbols": len(self.mid_history),
        }

    def reset(self):
        """Reset model state."""
        self.mid_history.clear()
        self.signal_count = 0
        self.hit_count = 0
        logger.info("MA momentum alpha model reset")


def create_ma_momentum_alpha(**kwargs) -> MovingAverageMomentumAlpha:
    """Factory function to create MA momentum alpha."""
    return MovingAverageMomentumAlpha(**kwargs)


def main():
    """Test the MA momentum alpha model."""
    logger.info("🚀 Testing MA Momentum Alpha Model...")

    # Initialize model
    alpha = MovingAverageMomentumAlpha(
        short_period=5, long_period=30, edge_scaling=40.0
    )

    # Simulate price data with momentum pattern
    symbols = ["BTC-USD", "ETH-USD", "SOL-USD"]
    base_prices = {"BTC-USD": 50000, "ETH-USD": 3000, "SOL-USD": 100}

    # Generate test data: trending upward with some noise
    all_signals = []

    for symbol in symbols:
        base_price = base_prices[symbol]
        signals = []

        # Create upward trending price series
        for i in range(40):  # Need 30+ points to generate signals
            # Add trend + noise
            trend = i * 0.01  # 1% trend per period
            noise = np.random.normal(0, 0.005)  # 0.5% noise
            price = base_price * (1 + trend + noise)

            timestamp = f"2025-01-15T10:{i:02d}:00Z"
            signal = alpha.update_price(symbol, price, timestamp)

            if signal:
                signals.append(signal)
                print(
                    f"📊 {symbol} Signal {len(signals)}: edge={signal.edge_bps:.1f}bps "
                    f"conf={signal.confidence:.2f} @ ${price:.2f}"
                )

        all_signals.extend(signals)

        # Test performance tracking
        if signals:
            # Simulate some performance updates (assume momentum worked)
            for signal in signals[:3]:
                realized_return = (
                    signal.edge_bps * 0.7
                )  # 70% of predicted edge realized
                alpha.update_performance(
                    signal.symbol, realized_return, signal.edge_bps
                )

    # Print stats
    stats = alpha.get_stats()
    print(f"\n📈 Model Stats:")
    for key, value in stats.items():
        print(f"  {key}: {value}")

    print(f"\n✅ Generated {len(all_signals)} signals across {len(symbols)} symbols")
    print(f"📊 Hit rate: {alpha.get_hit_rate():.1%}")

    # Test edge bounds
    edge_values = [abs(s.edge_bps) for s in all_signals]
    if edge_values:
        print(f"🎯 Edge range: {min(edge_values):.1f} to {max(edge_values):.1f} bps")
        print(f"📊 Max edge within ±40bp: {max(edge_values) <= 40}")

    # Test confidence bounds
    conf_values = [s.confidence for s in all_signals]
    if conf_values:
        print(f"🎯 Confidence range: {min(conf_values):.2f} to {max(conf_values):.2f}")
        print(
            f"📊 Confidence in [0.55,0.9]: {all(0.55 <= c <= 0.9 for c in conf_values)}"
        )

    print("🎉 MA Momentum Alpha test completed!")


if __name__ == "__main__":
    main()
