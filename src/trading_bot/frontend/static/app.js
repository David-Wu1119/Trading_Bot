const POLL_INTERVAL_MS = 5000;

const elements = {
  heartbeat: document.getElementById("heartbeat"),
  controlState: document.getElementById("control-state"),
  killState: document.getElementById("kill-state"),
  missionCount: document.getElementById("mission-count"),
  traceCount: document.getElementById("trace-count"),
  portfolioValue: document.getElementById("portfolio-value"),
  portfolioPnl: document.getElementById("portfolio-pnl"),
  missionForm: document.getElementById("mission-form"),
  missionList: document.getElementById("mission-list"),
  killForm: document.getElementById("kill-form"),
  resetForm: document.getElementById("reset-form"),
  scratchpadList: document.getElementById("scratchpad-list"),
  refreshNow: document.getElementById("refresh-now"),
  lastUpdated: document.getElementById("last-updated"),
  kpiNetPnl: document.getElementById("kpi-net-pnl"),
  kpiCostRatio: document.getElementById("kpi-cost-ratio"),
  kpiApprovalRate: document.getElementById("kpi-approval-rate"),
  kpiBlocked: document.getElementById("kpi-blocked"),
  kpiRamp: document.getElementById("kpi-ramp"),
  kpiControl: document.getElementById("kpi-control"),
  econTimestamp: document.getElementById("econ-timestamp"),
  econGross: document.getElementById("econ-gross"),
  econNet: document.getElementById("econ-net"),
  econFees: document.getElementById("econ-fees"),
  econInfra: document.getElementById("econ-infra"),
  econMargin: document.getElementById("econ-margin"),
  econAssets: document.getElementById("econ-assets"),
  rampPill: document.getElementById("ramp-pill"),
  rampDecision: document.getElementById("ramp-decision"),
  rampEntropy: document.getElementById("ramp-entropy"),
  rampQspread: document.getElementById("ramp-qspread"),
  rampDrawdown: document.getElementById("ramp-drawdown"),
  rampCostRatio: document.getElementById("ramp-cost-ratio"),
  rampCostPass: document.getElementById("ramp-cost-pass"),
  rampReasons: document.getElementById("ramp-reasons"),
  marketTimestamp: document.getElementById("market-timestamp"),
  liveSessionValue: document.getElementById("live-session-value"),
  liveSessionPnl: document.getElementById("live-session-pnl"),
  liveSessionUnrealized: document.getElementById("live-session-unrealized"),
  liveSessionRealized: document.getElementById("live-session-realized"),
  liveSessionFees: document.getElementById("live-session-fees"),
  livePriceList: document.getElementById("live-price-list"),
  liveTradeBody: document.getElementById("live-trade-body"),
  advisorPosturePill: document.getElementById("advisor-posture-pill"),
  advisorPosture: document.getElementById("advisor-posture"),
  advisorAction: document.getElementById("advisor-action"),
  advisorScore: document.getElementById("advisor-score"),
  advisorTimestamp: document.getElementById("advisor-timestamp"),
  advisorReasons: document.getElementById("advisor-reasons"),
  advisorGates: document.getElementById("advisor-gates"),
  advisorDecisionLock: document.getElementById("advisor-decision-lock"),
  advisorEvaluated: document.getElementById("advisor-evaluated"),
  advisorHitRate: document.getElementById("advisor-hit-rate"),
  advisorWeightedHitRate: document.getElementById("advisor-weighted-hit-rate"),
  advisorNotifications: document.getElementById("advisor-notifications"),
  advisorTableBody: document.getElementById("advisor-table-body"),
  exposureTableBody: document.getElementById("exposure-table-body"),
};

let refreshInFlight = false;

function ensureFlashNode() {
  let flash = document.getElementById("flash");
  if (flash) {
    return flash;
  }
  flash = document.createElement("div");
  flash.id = "flash";
  flash.className = "flash";
  flash.hidden = true;
  const shell = document.querySelector(".shell");
  shell.prepend(flash);
  return flash;
}

function showFlash(message, level = "info") {
  const flash = ensureFlashNode();
  flash.hidden = false;
  flash.className = `flash flash-${level}`;
  flash.textContent = message;
  window.setTimeout(() => {
    flash.hidden = true;
  }, 3000);
}

function setText(node, value) {
  if (!node) {
    return;
  }
  node.textContent = value;
}

function formatCurrency(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return new Intl.NumberFormat("en-US", {
    style: "currency",
    currency: "USD",
    maximumFractionDigits: 2,
  }).format(Number(value));
}

function formatPercent(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

function formatDirectPercent(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return `${Number(value).toFixed(digits)}%`;
}

function formatNumber(value, digits = 0) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  return Number(value).toFixed(digits);
}

function formatAgeSeconds(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  const seconds = Number(value);
  if (seconds < 60) {
    return `${seconds.toFixed(1)}s`;
  }
  return `${(seconds / 60).toFixed(1)}m`;
}

function formatTrendPercent(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return "--";
  }
  const numeric = Number(value);
  const prefix = numeric > 0 ? "+" : "";
  return `${prefix}${numeric.toFixed(2)}%`;
}

function setSignedCurrency(node, value) {
  if (!node) {
    return;
  }
  node.classList.remove("kpi-positive", "kpi-negative", "kpi-warning");
  node.textContent = formatCurrency(value);
  if (value === null || value === undefined || Number.isNaN(Number(value))) {
    return;
  }
  const numeric = Number(value);
  if (numeric > 0) {
    node.classList.add("kpi-positive");
  } else if (numeric < 0) {
    node.classList.add("kpi-negative");
  }
}

function setKpi(node, text, tone = "neutral") {
  if (!node) {
    return;
  }
  node.classList.remove("kpi-positive", "kpi-negative", "kpi-warning");
  if (tone === "positive") {
    node.classList.add("kpi-positive");
  } else if (tone === "negative") {
    node.classList.add("kpi-negative");
  } else if (tone === "warning") {
    node.classList.add("kpi-warning");
  }
  node.textContent = text;
}

function updateHeartbeat(healthy, label) {
  const heartbeat = elements.heartbeat;
  heartbeat.textContent = label;
  heartbeat.classList.remove("pill-neutral", "pill-accent", "pill-danger");
  heartbeat.classList.add(healthy ? "pill-accent" : "pill-danger");
}

async function requestJson(path, options = {}) {
  const response = await fetch(path, {
    method: options.method || "GET",
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });

  let payload = {};
  try {
    payload = await response.json();
  } catch {
    payload = {};
  }

  if (!response.ok) {
    const detail = payload.detail || payload.message || response.statusText;
    throw new Error(typeof detail === "string" ? detail : JSON.stringify(detail));
  }

  return payload;
}

function renderMissionCard(mission) {
  const card = document.createElement("article");
  card.className = "mission-card";

  const title = document.createElement("p");
  title.className = "mission-title";
  title.textContent = mission.title;
  card.appendChild(title);

  const meta = document.createElement("div");
  meta.className = "mission-meta";
  const statusPill = document.createElement("span");
  statusPill.className = `pill pill-${mission.status.replace("_", "-")}`;
  statusPill.textContent = mission.status.replace("_", " ");
  meta.appendChild(statusPill);

  const priorityPill = document.createElement("span");
  priorityPill.className = `pill pill-priority-${mission.priority}`;
  priorityPill.textContent = `${mission.priority} priority`;
  meta.appendChild(priorityPill);

  if (mission.symbol) {
    const symbolPill = document.createElement("span");
    symbolPill.className = "pill pill-neutral";
    symbolPill.textContent = mission.symbol;
    meta.appendChild(symbolPill);
  }

  card.appendChild(meta);

  if (mission.objective) {
    const objective = document.createElement("p");
    objective.className = "mission-copy";
    objective.textContent = mission.objective;
    card.appendChild(objective);
  }

  if (mission.notes) {
    const notes = document.createElement("p");
    notes.className = "mission-notes";
    notes.textContent = `Notes: ${mission.notes}`;
    card.appendChild(notes);
  }

  const updated = document.createElement("p");
  updated.className = "mission-updated";
  updated.textContent = `Updated ${new Date(mission.updated_at).toLocaleString()}`;
  card.appendChild(updated);

  const actions = document.createElement("div");
  actions.className = "mission-actions";

  ["planned", "in_progress", "blocked", "done"].forEach((status) => {
    if (status === mission.status) {
      return;
    }
    const button = document.createElement("button");
    button.type = "button";
    button.dataset.missionId = mission.id;
    button.dataset.status = status;
    button.textContent = status.replace("_", " ");
    actions.appendChild(button);
  });

  card.appendChild(actions);
  return card;
}

function renderMissions(missions) {
  const container = elements.missionList;
  container.replaceChildren();

  if (missions.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No missions yet. Add the first mission to start the runbook.";
    container.appendChild(empty);
    return;
  }

  missions.forEach((mission) => {
    container.appendChild(renderMissionCard(mission));
  });
}

function renderTrace(entries) {
  const container = elements.scratchpadList;
  container.replaceChildren();

  if (entries.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No trace events yet.";
    container.appendChild(empty);
    return;
  }

  entries.slice(0, 60).forEach((entry) => {
    const item = document.createElement("article");
    item.className = "trace-item";

    const meta = document.createElement("p");
    meta.className = "trace-meta";
    const timestamp = new Date(entry.timestamp);
    meta.textContent = `${timestamp.toLocaleString()}  ${entry.type}`;
    item.appendChild(meta);

    const body = document.createElement("pre");
    body.textContent = JSON.stringify(entry.payload || {}, null, 2);
    item.appendChild(body);

    container.appendChild(item);
  });
}

function renderAssets(assets) {
  const container = elements.econAssets;
  container.replaceChildren();

  if (!assets || assets.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No asset-level economics found.";
    container.appendChild(empty);
    return;
  }

  assets.slice(0, 6).forEach((asset) => {
    const row = document.createElement("div");
    row.className = "asset-item";

    const left = document.createElement("strong");
    left.textContent = asset.symbol;

    const right = document.createElement("span");
    const fills = Number(asset.fill_count || 0);
    right.textContent = `${formatCurrency(asset.net_pnl_usd)} | fills ${fills}`;

    row.appendChild(left);
    row.appendChild(right);
    container.appendChild(row);
  });
}

function renderRampReasons(reasons) {
  const container = elements.rampReasons;
  container.replaceChildren();

  if (!reasons || reasons.length === 0) {
    const empty = document.createElement("li");
    empty.textContent = "No explicit reasons available.";
    container.appendChild(empty);
    return;
  }

  reasons.slice(0, 6).forEach((reason) => {
    const item = document.createElement("li");
    item.textContent = reason;
    container.appendChild(item);
  });
}

function renderExposureRows(events) {
  const body = elements.exposureTableBody;
  body.replaceChildren();

  if (!events || events.length === 0) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 8;
    cell.className = "empty-state";
    cell.textContent = "No exposure decisions found in audit artifacts.";
    row.appendChild(cell);
    body.appendChild(row);
    return;
  }

  events.forEach((event) => {
    const row = document.createElement("tr");
    row.className = event.allowed ? "row-allow" : "row-block";

    const columns = [
      new Date(event.timestamp).toLocaleTimeString(),
      event.symbol,
      event.side,
      formatCurrency(event.notional_usd),
      event.venue,
      `${formatNumber(event.influence_pct, 1)}%`,
      event.allowed ? "ALLOW" : "BLOCK",
      event.reason || "--",
    ];

    columns.forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = value;
      row.appendChild(cell);
    });

    body.appendChild(row);
  });
}

function renderLivePrices(prices) {
  const container = elements.livePriceList;
  if (!container) {
    return;
  }
  container.replaceChildren();

  if (!prices || prices.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No live prices available yet.";
    container.appendChild(empty);
    return;
  }

  prices.forEach((row) => {
    const item = document.createElement("div");
    item.className = "asset-item";

    const left = document.createElement("strong");
    left.textContent = row.symbol || "UNKNOWN";

    const right = document.createElement("span");
    right.textContent = `${formatCurrency(row.price)} (${formatAgeSeconds(row.age_seconds)} old)`;

    item.appendChild(left);
    item.appendChild(right);
    container.appendChild(item);
  });
}

function renderLiveTradeRows(trades) {
  const body = elements.liveTradeBody;
  if (!body) {
    return;
  }
  body.replaceChildren();

  if (!trades || trades.length === 0) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 7;
    cell.className = "empty-state";
    cell.textContent = "No recent trade events yet.";
    row.appendChild(cell);
    body.appendChild(row);
    return;
  }

  trades.forEach((trade) => {
    const row = document.createElement("tr");
    const status = String(trade.status || "").toUpperCase();
    row.className = status === "EXECUTED" ? "row-allow" : status === "BLOCKED" ? "row-block" : "row-pending";

    const columns = [
      new Date(trade.timestamp).toLocaleTimeString(),
      trade.symbol || "UNKNOWN",
      trade.side || "n/a",
      formatNumber(trade.quantity, 4),
      status || "--",
      trade.source || "--",
      trade.reason || "--",
    ];

    columns.forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = value;
      row.appendChild(cell);
    });

    body.appendChild(row);
  });
}

function renderAdvisorReasons(reasons) {
  const container = elements.advisorReasons;
  if (!container) {
    return;
  }
  container.replaceChildren();

  if (!reasons || reasons.length === 0) {
    const empty = document.createElement("li");
    empty.textContent = "No posture reasons available.";
    container.appendChild(empty);
    return;
  }

  reasons.slice(0, 6).forEach((reason) => {
    const item = document.createElement("li");
    item.textContent = reason;
    container.appendChild(item);
  });
}

function renderAdvisorRows(recommendations) {
  const body = elements.advisorTableBody;
  if (!body) {
    return;
  }
  body.replaceChildren();

  if (!recommendations || recommendations.length === 0) {
    const row = document.createElement("tr");
    const cell = document.createElement("td");
    cell.colSpan = 7;
    cell.className = "empty-state";
    cell.textContent = "No advisor recommendations available yet.";
    row.appendChild(cell);
    body.appendChild(row);
    return;
  }

  recommendations.forEach((item) => {
    const row = document.createElement("tr");
    const rec = String(item.recommendation || "").toUpperCase();
    row.className =
      rec === "BUY_BIAS"
        ? "row-allow"
        : rec === "AVOID" || rec === "HOLD_LOCKED"
          ? "row-block"
          : "row-pending";

    const rationaleList = Array.isArray(item.rationale) ? item.rationale : [];
    const contributions =
      item.provenance && item.provenance.contributions ? item.provenance.contributions : {};
    const drivers = Object.entries(contributions)
      .filter(([, value]) => Number(value) !== 0)
      .slice(0, 2)
      .map(([key, value]) => `${key}:${Number(value) > 0 ? "+" : ""}${Number(value).toFixed(1)}`);
    const rationale = [...rationaleList, ...drivers].join(" | ") || "--";
    const columns = [
      item.symbol || "UNKNOWN",
      formatCurrency(item.price),
      formatTrendPercent(item.trend_pct),
      rec || "--",
      formatPercent(item.confidence, 1),
      formatAgeSeconds(item.age_seconds),
      rationale,
    ];

    columns.forEach((value) => {
      const cell = document.createElement("td");
      cell.textContent = value;
      row.appendChild(cell);
    });

    body.appendChild(row);
  });
}

function renderAdvisorGates(gatesPayload) {
  const container = elements.advisorGates;
  if (!container) {
    return;
  }
  container.replaceChildren();

  const gates = gatesPayload && Array.isArray(gatesPayload.gates) ? gatesPayload.gates : [];
  if (gates.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No acceptance gate data.";
    container.appendChild(empty);
    return;
  }

  gates.forEach((gate) => {
    const item = document.createElement("div");
    item.className = `asset-item ${gate.passed ? "gate-pass" : "gate-fail"}`;

    const left = document.createElement("strong");
    left.textContent = gate.name || "gate";

    const right = document.createElement("span");
    const status = gate.passed ? "PASS" : "FAIL";
    right.textContent = `${status} | ${gate.threshold || "--"}`;

    item.appendChild(left);
    item.appendChild(right);
    container.appendChild(item);
  });
}

function renderAdvisorNotifications(events) {
  const container = elements.advisorNotifications;
  if (!container) {
    return;
  }
  container.replaceChildren();

  if (!events || events.length === 0) {
    const empty = document.createElement("p");
    empty.className = "empty-state";
    empty.textContent = "No notification events in this cycle.";
    container.appendChild(empty);
    return;
  }

  events.forEach((event) => {
    const item = document.createElement("div");
    item.className = `asset-item ${event.sent ? "gate-pass" : "gate-fail"}`;

    const left = document.createElement("strong");
    left.textContent = event.type || "notification";

    const right = document.createElement("span");
    const channels = [];
    if (event.wechat) channels.push("wechat");
    if (event.email) channels.push("email");
    const channelText = channels.length > 0 ? channels.join(",") : "none";
    right.textContent = `${event.sent ? "SENT" : "SKIP"} | ${channelText}`;

    item.appendChild(left);
    item.appendChild(right);
    container.appendChild(item);
  });
}

function updateAdvisor(advisor) {
  if (!advisor) {
    setText(elements.advisorPosture, "--");
    setText(elements.advisorAction, "--");
    setText(elements.advisorScore, "--");
    setText(elements.advisorTimestamp, "--");
    if (elements.advisorPosturePill) {
      elements.advisorPosturePill.className = "pill pill-neutral";
      elements.advisorPosturePill.textContent = "Unknown";
    }
    renderAdvisorReasons([]);
    renderAdvisorGates(null);
    setText(elements.advisorDecisionLock, "--");
    setText(elements.advisorEvaluated, "--");
    setText(elements.advisorHitRate, "--");
    setText(elements.advisorWeightedHitRate, "--");
    renderAdvisorNotifications([]);
    renderAdvisorRows([]);
    return;
  }

  const posture = String(advisor.portfolio_posture || "UNKNOWN").toUpperCase();
  setText(elements.advisorPosture, posture);
  setText(elements.advisorAction, advisor.portfolio_action || "--");
  setText(elements.advisorScore, formatNumber(advisor.posture_score, 0));
  setText(elements.advisorTimestamp, new Date(advisor.timestamp).toLocaleString());

  if (elements.advisorPosturePill) {
    if (posture === "RISK_OFF" || posture === "DEFENSIVE") {
      elements.advisorPosturePill.className = "pill pill-danger";
    } else if (posture === "OFFENSIVE") {
      elements.advisorPosturePill.className = "pill pill-accent";
    } else {
      elements.advisorPosturePill.className = "pill pill-neutral";
    }
    elements.advisorPosturePill.textContent = posture;
  }

  renderAdvisorReasons(advisor.posture_reasons || []);
  renderAdvisorGates(advisor.acceptance_gates || null);
  const lockLabel = advisor.decision_locked
    ? `LOCKED (${Number(advisor.locked_recommendation_count || 0)})`
    : "OPEN";
  setText(elements.advisorDecisionLock, lockLabel);
  const quality = advisor.quality || {};
  setText(elements.advisorEvaluated, String(quality.evaluated ?? "--"));
  setText(elements.advisorHitRate, formatPercent(quality.hit_rate, 1));
  setText(elements.advisorWeightedHitRate, formatPercent(quality.weighted_hit_rate, 1));
  renderAdvisorNotifications(advisor.notifications || []);
  renderAdvisorRows(advisor.recommendations || []);
}

function updateMarket(market) {
  if (!market) {
    setText(elements.marketTimestamp, "--");
    setText(elements.liveSessionValue, "--");
    setText(elements.liveSessionPnl, "--");
    setText(elements.liveSessionUnrealized, "--");
    setText(elements.liveSessionRealized, "--");
    setText(elements.liveSessionFees, "--");
    renderLivePrices([]);
    renderLiveTradeRows([]);
    return;
  }
  setText(elements.marketTimestamp, new Date(market.timestamp).toLocaleString());
  const livePnl = market.live_pnl || null;
  if (livePnl) {
    setText(elements.liveSessionValue, formatCurrency(livePnl.portfolio_value_usd));
    setSignedCurrency(elements.liveSessionPnl, livePnl.portfolio_pnl_usd);
    setSignedCurrency(elements.liveSessionUnrealized, livePnl.unrealized_pnl_usd);
    setSignedCurrency(elements.liveSessionRealized, livePnl.realized_pnl_usd);
    setText(elements.liveSessionFees, formatCurrency(livePnl.total_fees_usd));
  } else {
    setText(elements.liveSessionValue, "--");
    setText(elements.liveSessionPnl, "--");
    setText(elements.liveSessionUnrealized, "--");
    setText(elements.liveSessionRealized, "--");
    setText(elements.liveSessionFees, "--");
  }
  renderLivePrices(market.prices || []);
  renderLiveTradeRows(market.trades || []);
}

function updateEconomics(economics) {
  if (!economics || !economics.portfolio) {
    setText(elements.econTimestamp, "--");
    setText(elements.econGross, "--");
    setText(elements.econNet, "--");
    setText(elements.econFees, "--");
    setText(elements.econInfra, "--");
    setText(elements.econMargin, "--");
    renderAssets([]);
    return;
  }

  const portfolio = economics.portfolio;

  setText(elements.econTimestamp, new Date(economics.timestamp).toLocaleString());
  setText(elements.econGross, formatCurrency(portfolio.gross_pnl_usd));
  setText(elements.econNet, formatCurrency(portfolio.net_pnl_final_usd));
  setText(elements.econFees, formatCurrency(portfolio.total_fees_usd));
  setText(elements.econInfra, formatCurrency(portfolio.total_infra_cost_usd));
  setText(elements.econMargin, formatDirectPercent(portfolio.net_margin_pct));

  renderAssets(economics.top_assets || []);
}

function updateRamp(ramp) {
  if (!ramp) {
    setText(elements.rampDecision, "--");
    setText(elements.rampEntropy, "--");
    setText(elements.rampQspread, "--");
    setText(elements.rampDrawdown, "--");
    setText(elements.rampCostRatio, "--");
    setText(elements.rampCostPass, "--");
    elements.rampPill.className = "pill pill-neutral";
    elements.rampPill.textContent = "Unknown";
    renderRampReasons([]);
    return;
  }

  setText(elements.rampDecision, ramp.decision || "UNKNOWN");
  setText(elements.rampEntropy, formatNumber(ramp.kri.entropy, 2));
  setText(elements.rampQspread, formatNumber(ramp.kri.qspread_ratio, 2));
  setText(elements.rampDrawdown, formatDirectPercent(ramp.kri.daily_drawdown_pct, 2));
  setText(elements.rampCostRatio, formatPercent(ramp.cost_gates.cost_ratio, 1));
  setText(elements.rampCostPass, ramp.cost_gates.overall_pass ? "PASS" : "FAIL");

  const decision = String(ramp.decision || "UNKNOWN").toUpperCase();
  if (decision.includes("NO")) {
    elements.rampPill.className = "pill pill-danger";
  } else if (decision.includes("RAMP") || decision.includes("GO")) {
    elements.rampPill.className = "pill pill-accent";
  } else {
    elements.rampPill.className = "pill pill-neutral";
  }
  elements.rampPill.textContent = decision;

  renderRampReasons(ramp.reasons || []);
}

function updateKpis(status, telemetry) {
  const exposureSummary = telemetry && telemetry.exposure ? telemetry.exposure.summary || {} : {};
  const economics = telemetry ? telemetry.economics : null;
  const ramp = telemetry ? telemetry.ramp : null;
  const market = telemetry ? telemetry.market : null;
  const livePnl = market ? market.live_pnl || null : null;
  const metrics = telemetry && telemetry.metrics ? telemetry.metrics : {};

  const netPnl = livePnl
    ? livePnl.portfolio_pnl_usd
    : economics && economics.portfolio
      ? economics.portfolio.net_pnl_final_usd
      : null;
  const costRatio = economics && economics.portfolio ? economics.portfolio.cost_ratio : null;
  const approvalRate = exposureSummary.approval_rate;
  const blocked = exposureSummary.blocked;
  const rampDecision = ramp ? String(ramp.decision || "UNKNOWN") : "UNKNOWN";

  setKpi(
    elements.kpiNetPnl,
    formatCurrency(netPnl),
    Number(netPnl) > 0 ? "positive" : Number(netPnl) < 0 ? "negative" : "neutral",
  );
  setKpi(
    elements.kpiCostRatio,
    formatPercent(costRatio, 1),
    Number(costRatio) > 0.5 ? "negative" : Number(costRatio) > 0.3 ? "warning" : "positive",
  );
  setKpi(
    elements.kpiApprovalRate,
    formatPercent(approvalRate, 1),
    Number(approvalRate) >= 0.7 ? "positive" : Number(approvalRate) >= 0.4 ? "warning" : "negative",
  );
  setKpi(
    elements.kpiBlocked,
    blocked === undefined ? "--" : String(blocked),
    Number(blocked) > 0 ? "negative" : "positive",
  );

  const rampUpper = rampDecision.toUpperCase();
  setKpi(
    elements.kpiRamp,
    rampUpper,
    rampUpper.includes("NO") ? "negative" : rampUpper.includes("GO") || rampUpper.includes("RAMP") ? "positive" : "warning",
  );

  const controlReachable = status && status.control_api ? status.control_api.reachable : false;
  setKpi(elements.kpiControl, controlReachable ? "ONLINE" : "OFFLINE", controlReachable ? "positive" : "negative");

  const prometheusMetrics = metrics.metrics || {};
  const metricPortfolioValue = prometheusMetrics.portfolio_value_usd;
  const metricPortfolioPnl = prometheusMetrics.portfolio_pnl_usd;
  const livePortfolioValue = livePnl ? livePnl.portfolio_value_usd : null;
  const livePortfolioPnl = livePnl ? livePnl.portfolio_pnl_usd : null;

  if (metricPortfolioValue !== undefined && metricPortfolioValue !== null) {
    setText(elements.portfolioValue, formatCurrency(metricPortfolioValue));
  } else if (livePortfolioValue !== null && livePortfolioValue !== undefined) {
    setText(elements.portfolioValue, formatCurrency(livePortfolioValue));
  } else {
    setText(elements.portfolioValue, "--");
  }

  if (metricPortfolioPnl !== undefined && metricPortfolioPnl !== null) {
    setText(elements.portfolioPnl, formatCurrency(metricPortfolioPnl));
  } else if (livePortfolioPnl !== null && livePortfolioPnl !== undefined) {
    setText(elements.portfolioPnl, formatCurrency(livePortfolioPnl));
  } else {
    setText(elements.portfolioPnl, formatCurrency(netPnl));
  }
}

function updateStatus(statusData) {
  const reachable = Boolean(statusData.control_api && statusData.control_api.reachable);
  const controlState = statusData.control_api && statusData.control_api.state ? statusData.control_api.state : {};

  updateHeartbeat(reachable, reachable ? "Connected" : "Offline");
  setText(elements.controlState, reachable ? "Reachable" : "Unreachable");
  setText(elements.killState, controlState.kill_switch_active ? "ACTIVE" : "OFF");
  setText(elements.missionCount, String(statusData.missions.total || 0));
  setText(elements.traceCount, String(statusData.scratchpad_entries || 0));
}

async function refreshAll(options = {}) {
  const force = Boolean(options.force);

  if (refreshInFlight) {
    return;
  }
  if (document.hidden && !force) {
    return;
  }

  refreshInFlight = true;
  try {
    const [statusData, missionsData, scratchpadData, telemetryData] = await Promise.all([
      requestJson("/api/system/status"),
      requestJson("/api/missions"),
      requestJson("/api/scratchpad?limit=120"),
      requestJson("/api/telemetry/overview?exposure_limit=120&include_events=24"),
    ]);

    updateStatus(statusData);
    renderMissions(missionsData.missions || []);
    renderTrace(scratchpadData.entries || []);

    updateEconomics(telemetryData.economics || null);
    updateRamp(telemetryData.ramp || null);
    updateMarket(telemetryData.market || null);
    updateAdvisor(telemetryData.advisor || null);
    renderExposureRows(telemetryData.exposure ? telemetryData.exposure.events || [] : []);
    updateKpis(statusData, telemetryData);

    setText(elements.lastUpdated, `Last update: ${new Date().toLocaleTimeString()}`);
  } catch (error) {
    showFlash(`Refresh failed: ${error.message}`, "error");
  } finally {
    refreshInFlight = false;
  }
}

async function submitMission(event) {
  event.preventDefault();
  const payload = {
    title: document.getElementById("mission-title").value,
    symbol: document.getElementById("mission-symbol").value || null,
    priority: document.getElementById("mission-priority").value,
    objective: document.getElementById("mission-objective").value || null,
  };

  try {
    await requestJson("/api/missions", { method: "POST", body: payload });
    event.target.reset();
    showFlash("Mission added", "success");
    await refreshAll({ force: true });
  } catch (error) {
    showFlash(`Mission failed: ${error.message}`, "error");
  }
}

async function submitKillSwitch(event) {
  event.preventDefault();
  const reason = document.getElementById("kill-reason").value;
  if (!window.confirm("Activate kill switch and halt trading?")) {
    return;
  }

  try {
    await requestJson("/api/control/kill", {
      method: "POST",
      body: { reason },
    });
    showFlash("Kill switch activated", "warning");
    await refreshAll({ force: true });
  } catch (error) {
    showFlash(`Kill switch failed: ${error.message}`, "error");
  }
}

async function submitReset(event) {
  event.preventDefault();
  const authorizedBy = document.getElementById("authorized-by").value;

  try {
    await requestJson("/api/control/reset", {
      method: "POST",
      body: { authorized_by: authorizedBy },
    });
    showFlash("Kill switch reset", "success");
    await refreshAll({ force: true });
  } catch (error) {
    showFlash(`Reset failed: ${error.message}`, "error");
  }
}

async function handleMissionAction(event) {
  const button = event.target.closest("button[data-mission-id]");
  if (!button) {
    return;
  }

  const missionId = button.dataset.missionId;
  const status = button.dataset.status;

  try {
    await requestJson(`/api/missions/${missionId}`, {
      method: "PATCH",
      body: { status },
    });
    showFlash(`Mission moved to ${status.replace("_", " ")}`, "success");
    await refreshAll({ force: true });
  } catch (error) {
    showFlash(`Update failed: ${error.message}`, "error");
  }
}

function bindEvents() {
  elements.missionForm.addEventListener("submit", submitMission);
  elements.killForm.addEventListener("submit", submitKillSwitch);
  elements.resetForm.addEventListener("submit", submitReset);
  elements.missionList.addEventListener("click", handleMissionAction);

  if (elements.refreshNow) {
    elements.refreshNow.addEventListener("click", () => {
      refreshAll({ force: true });
    });
  }

  document.addEventListener("visibilitychange", () => {
    if (!document.hidden) {
      refreshAll({ force: true });
    }
  });
}

function boot() {
  bindEvents();
  refreshAll({ force: true });
  window.setInterval(() => {
    refreshAll();
  }, POLL_INTERVAL_MS);
}

boot();
