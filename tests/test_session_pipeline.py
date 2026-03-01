"""Unit tests for the trading session pipeline."""

import unittest
from unittest.mock import Mock, patch
import numpy as np

import pytest

pytest.importorskip(
    "alpaca_trade_api",
    reason="alpaca_trade_api optional dependency required for executor",
)

from src.layers.layer1_alpha_models.ob_pressure import OrderBookPressureAlpha
from src.layers.layer1_alpha_models.ma_momentum import MovingAverageMomentumAlpha
from src.layers.layer2_ensemble.meta_learner import MetaLearner
from src.layers.layer3_position_sizing.kelly_sizing import KellySizing
from src.layers.layer4_execution.alpaca_executor import AlpacaExecutor


class TestSessionPipeline(unittest.TestCase):
    """Test the full trading session pipeline."""

    def setUp(self):
        """Set up test fixtures."""
        self.obp = OrderBookPressureAlpha()
        self.mam = MovingAverageMomentumAlpha()
        self.meta = MetaLearner()
        self.sizer = KellySizing()
        self.executor = AlpacaExecutor(dry_run=True)

    def test_obp_mam_to_fill(self):
        """Test that OBP & MAM signals flow through to execution."""
        # Mock market data tick
        tick1 = Mock()
        tick1.symbol = "AAPL"
        tick1.bid_size = 1000
        tick1.ask_size = 800
        tick1.price = 175.0
        tick1.timestamp = "2024-04-12 10:30:00"

        tick2 = Mock()
        tick2.symbol = "AAPL"
        tick2.bid_size = 1200
        tick2.ask_size = 900
        tick2.price = 175.5
        tick2.timestamp = "2024-04-12 10:30:01"

        # Process first tick through OBP
        obp_edge1, obp_conf1 = self.obp.predict(tick1)
        self.assertNotEqual(obp_edge1, 0)
        self.assertGreaterEqual(obp_conf1, 0.5)
        self.assertLessEqual(obp_conf1, 1.0)

        # Process first tick through MAM
        mam_edge1, mam_conf1 = self.mam.predict(tick1)
        self.assertEqual(mam_edge1, 0)
        self.assertEqual(mam_conf1, 0)

        # Warm up MAM so it emits a real signal.
        warm_edge, warm_conf = self.mam.predict(tick2)
        for i in range(3, 36):
            tick = Mock()
            tick.symbol = "AAPL"
            tick.bid_size = 1000 + i * 5
            tick.ask_size = 800 + i * 5
            tick.price = 175.0 + i * 0.1
            tick.timestamp = f"2024-04-12 10:30:{i:02d}"
            warm_edge, warm_conf = self.mam.predict(tick)

        self.assertNotEqual(warm_edge, 0)
        self.assertGreaterEqual(warm_conf, 0.55)
        self.assertLessEqual(warm_conf, 0.9)

        # Combine signals in meta-learner
        alpha_signals = {
            "ob_pressure": (obp_edge1, obp_conf1),
            "ma_momentum": (warm_edge, warm_conf),
        }
        meta_edge, meta_conf = self.meta.predict(alpha_signals, tick1)
        self.assertNotEqual(meta_edge, 0)
        self.assertGreater(meta_edge * meta_conf, 0)

        # Size the position
        notional = self.sizer.get_position_notional(
            edge_bps=meta_edge,
            confidence=meta_conf,
            price=tick1.price,
            volatility=0.02,  # Mock 2% vol
        )
        self.assertGreater(notional, 0)

        # Execute the trade
        with patch.object(self.executor, "submit_order") as mock_submit:
            self.executor.execute(
                symbol=tick1.symbol,
                notional=notional,
                price=tick1.price,
                timestamp=tick1.timestamp,
            )
            mock_submit.assert_called_once()
