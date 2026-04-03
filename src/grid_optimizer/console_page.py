from __future__ import annotations

__all__ = ["build_console_page"]


def build_console_page() -> str:
    return """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Grid Console</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #efe8dc;
      --bg-strong: #e8decd;
      --panel: rgba(255, 251, 245, 0.92);
      --panel-strong: #fff8ef;
      --panel-soft: #f7efe2;
      --ink: #1f1d1a;
      --muted: #71685c;
      --muted-soft: #9a8e80;
      --accent: #b1682a;
      --accent-strong: #8d4912;
      --line: rgba(76, 58, 39, 0.12);
      --shadow: 0 18px 42px rgba(71, 48, 25, 0.12);
      --healthy-bg: #daf2e2;
      --healthy-ink: #0f6b3b;
      --degraded-bg: #ffedcc;
      --degraded-ink: #9a5600;
      --offline-bg: #ffe0db;
      --offline-ink: #b33c26;
    }
    * { box-sizing: border-box; }
    html { background: var(--bg); }
    body {
      margin: 0;
      min-height: 100vh;
      color: var(--ink);
      font: 15px/1.45 "IBM Plex Sans", "PingFang SC", "Microsoft YaHei", sans-serif;
      background:
        radial-gradient(circle at top right, rgba(177, 104, 42, 0.18), transparent 30%),
        radial-gradient(circle at top left, rgba(126, 98, 67, 0.12), transparent 32%),
        linear-gradient(180deg, #f8f2e9 0%, var(--bg) 100%);
    }
    body::before {
      content: "";
      position: fixed;
      inset: 0;
      pointer-events: none;
      background-image:
        linear-gradient(rgba(115, 94, 66, 0.05) 1px, transparent 1px),
        linear-gradient(90deg, rgba(115, 94, 66, 0.05) 1px, transparent 1px);
      background-size: 22px 22px;
      mask-image: linear-gradient(180deg, rgba(0, 0, 0, 0.4), transparent 70%);
    }
    button,
    a {
      font: inherit;
    }
    main {
      position: relative;
      z-index: 1;
      max-width: 760px;
      margin: 0 auto;
      padding: 88px 16px 160px;
    }
    .terminal-bar {
      position: sticky;
      top: 0;
      z-index: 20;
      display: flex;
      gap: 12px;
      align-items: center;
      justify-content: space-between;
      padding: 14px 16px;
      background: rgba(246, 238, 227, 0.9);
      backdrop-filter: blur(18px);
      border-bottom: 1px solid var(--line);
      box-shadow: 0 10px 30px rgba(64, 43, 23, 0.08);
    }
    .terminal-meta {
      min-width: 0;
      display: grid;
      gap: 6px;
    }
    .terminal-title-row {
      display: flex;
      gap: 8px;
      align-items: center;
      flex-wrap: wrap;
    }
    .terminal-title {
      margin: 0;
      font-size: 1rem;
      font-weight: 700;
      letter-spacing: 0.03em;
    }
    .terminal-subtitle {
      color: var(--muted);
      font-size: 0.86rem;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }
    .status-chip {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 0 10px;
      border-radius: 999px;
      font-size: 0.76rem;
      font-weight: 700;
      letter-spacing: 0.06em;
      text-transform: uppercase;
    }
    .status-chip.is-healthy {
      background: var(--healthy-bg);
      color: var(--healthy-ink);
    }
    .status-chip.is-degraded {
      background: var(--degraded-bg);
      color: var(--degraded-ink);
    }
    .status-chip.is-offline {
      background: var(--offline-bg);
      color: var(--offline-ink);
    }
    .terminal-actions {
      display: flex;
      align-items: center;
      gap: 8px;
      flex-shrink: 0;
    }
    .terminal-actions button {
      min-height: 40px;
      padding: 0 14px;
      border-radius: 999px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.74);
      color: var(--ink);
      box-shadow: 0 4px 16px rgba(68, 47, 26, 0.05);
    }
    .terminal-actions button.primary-action {
      background: linear-gradient(180deg, #d28d4f 0%, var(--accent) 100%);
      border-color: rgba(132, 74, 24, 0.35);
      color: #fffaf4;
    }
    .card {
      margin: 0 0 14px;
      padding: 16px;
      border-radius: 22px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, var(--panel) 0%, rgba(255, 255, 255, 0.98) 100%);
      box-shadow: var(--shadow);
    }
    .terminal-overview {
      background:
        linear-gradient(160deg, rgba(255, 247, 236, 0.98) 0%, rgba(255, 251, 246, 0.98) 65%),
        linear-gradient(180deg, var(--panel) 0%, rgba(255, 255, 255, 0.98) 100%);
      border-color: rgba(177, 104, 42, 0.22);
    }
    .section-head {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      margin: 0 0 14px;
    }
    .section-title {
      margin: 0;
      font-size: 0.86rem;
      font-weight: 800;
      letter-spacing: 0.11em;
      text-transform: uppercase;
    }
    .section-caption {
      color: var(--muted);
      font-size: 0.76rem;
    }
    .overview-grid {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .overview-tile {
      padding: 14px;
      border-radius: 18px;
      border: 1px solid rgba(177, 104, 42, 0.12);
      background: linear-gradient(180deg, var(--panel-strong) 0%, rgba(255, 255, 255, 0.96) 100%);
    }
    .overview-tile-label,
    .meta-line,
    .terminal-item-note,
    .account-option-note,
    .ghost-note,
    .toggle-button {
      color: var(--muted);
    }
    .overview-tile-label {
      display: block;
      margin-bottom: 4px;
      font-size: 0.76rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .overview-tile strong {
      display: block;
      font-family: "IBM Plex Mono", "SFMono-Regular", "Consolas", monospace;
      font-size: 1.08rem;
      line-height: 1.25;
      word-break: break-word;
    }
    .overview-meta {
      display: grid;
      gap: 6px;
      margin-top: 12px;
    }
    .meta-line {
      display: flex;
      justify-content: space-between;
      gap: 10px;
      font-size: 0.82rem;
    }
    .meta-line strong {
      color: var(--ink);
      font-size: 0.82rem;
      font-weight: 700;
      font-family: "IBM Plex Sans", "PingFang SC", "Microsoft YaHei", sans-serif;
    }
    .runtime-shell {
      display: grid;
      gap: 10px;
    }
    .runtime-primary {
      padding: 14px;
      border-radius: 18px;
      border: 1px solid rgba(39, 32, 23, 0.08);
      background: linear-gradient(180deg, #fffefb 0%, var(--panel-soft) 100%);
    }
    .runtime-primary-top {
      display: flex;
      justify-content: space-between;
      align-items: baseline;
      gap: 10px;
      margin-bottom: 10px;
    }
    .runtime-primary strong {
      display: block;
      font-size: 1.1rem;
      font-weight: 700;
    }
    .runtime-badge {
      display: inline-flex;
      align-items: center;
      min-height: 24px;
      padding: 0 10px;
      border-radius: 999px;
      background: rgba(24, 115, 67, 0.11);
      color: #17613c;
      font-size: 0.75rem;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .runtime-stats {
      display: grid;
      gap: 8px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .runtime-stat {
      padding: 10px 12px;
      border-radius: 14px;
      background: rgba(255, 255, 255, 0.82);
      border: 1px solid rgba(79, 60, 39, 0.08);
    }
    .runtime-stat small {
      display: block;
      color: var(--muted-soft);
      font-size: 0.74rem;
      margin-bottom: 2px;
      text-transform: uppercase;
      letter-spacing: 0.06em;
    }
    .runtime-stat strong {
      font-size: 0.96rem;
      font-family: "IBM Plex Mono", "SFMono-Regular", "Consolas", monospace;
    }
    .runtime-grid,
    .competition-list,
    .account-list {
      display: grid;
      gap: 10px;
    }
    .runtime-card,
    .competition-card,
    .account-option {
      padding: 12px;
      border-radius: 16px;
      border: 1px solid var(--line);
      background: rgba(255, 255, 255, 0.84);
    }
    .runtime-card strong,
    .competition-card strong,
    .account-option strong {
      display: block;
      margin-bottom: 4px;
    }
    .terminal-item-grid {
      display: grid;
      gap: 6px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .terminal-item-note {
      font-size: 0.8rem;
    }
    .toggle-button {
      width: 100%;
      min-height: 36px;
      margin-top: 6px;
      border-radius: 14px;
      border: 1px solid rgba(79, 60, 39, 0.12);
      background: rgba(255, 255, 255, 0.78);
    }
    .quick-actions-grid {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .quick-action {
      display: block;
      padding: 14px;
      border-radius: 18px;
      border: 1px solid var(--line);
      background: linear-gradient(180deg, #fffdf9 0%, rgba(255, 248, 240, 0.96) 100%);
      box-shadow: 0 8px 18px rgba(68, 47, 26, 0.06);
      color: var(--ink);
      text-decoration: none;
    }
    .quick-action strong {
      display: block;
      margin-bottom: 4px;
      font-size: 0.98rem;
    }
    .quick-action small {
      display: block;
      color: var(--muted);
      font-size: 0.78rem;
    }
    .quick-actions-footer {
      display: grid;
      gap: 6px;
      margin-top: 12px;
    }
    .account-sheet-backdrop {
      position: fixed;
      inset: 0;
      z-index: 25;
      background: rgba(29, 24, 18, 0.34);
      opacity: 0;
      pointer-events: none;
      transition: opacity 160ms ease;
    }
    .account-sheet-backdrop[data-open="true"] {
      opacity: 1;
      pointer-events: auto;
    }
    .account-sheet-shell {
      position: fixed;
      left: 0;
      right: 0;
      bottom: 0;
      z-index: 30;
      display: flex;
      justify-content: center;
      align-items: flex-end;
      pointer-events: none;
      padding: 0 12px 12px;
    }
    .account-sheet {
      width: min(760px, 100%);
      padding: 16px;
      border-radius: 24px 24px 0 0;
      background: linear-gradient(180deg, rgba(255, 251, 245, 0.98) 0%, rgba(255, 255, 255, 0.98) 100%);
      border: 1px solid var(--line);
      box-shadow: 0 -18px 40px rgba(58, 39, 18, 0.18);
      pointer-events: auto;
      transform: translateY(calc(100% + 16px));
      transition: transform 180ms ease;
    }
    .account-sheet-shell[data-open="true"] .account-sheet {
      transform: translateY(0);
    }
    .account-sheet-title {
      margin: 0 0 4px;
      font-size: 1rem;
      font-weight: 700;
    }
    .account-option {
      width: 100%;
      text-align: left;
      color: var(--ink);
    }
    .account-option.is-active {
      border-color: rgba(177, 104, 42, 0.36);
      background: linear-gradient(180deg, #fff7ea 0%, #fffdf8 100%);
    }
    .account-option-note {
      font-size: 0.8rem;
    }
    @media (min-width: 700px) {
      .overview-grid,
      .quick-actions-grid {
        grid-template-columns: repeat(4, minmax(0, 1fr));
      }
      .runtime-shell {
        grid-template-columns: minmax(0, 1.2fr) minmax(0, 0.8fr);
      }
      .runtime-secondary {
        align-content: start;
      }
    }
  </style>
</head>
<body>
  <header class="terminal-bar">
    <div class="terminal-meta">
      <div class="terminal-title-row">
        <strong class="terminal-title" id="current-account-label">Console</strong>
        <span class="status-chip is-degraded" id="health-chip">Booting</span>
      </div>
      <div class="terminal-subtitle" id="console-subtitle">Waiting for account context</div>
    </div>
    <div class="terminal-actions">
      <button id="open-account-sheet" type="button">Accounts</button>
      <button class="primary-action" id="refresh-console" type="button">Refresh</button>
    </div>
  </header>
  <main>
    <section class="card terminal-overview" id="overview" aria-labelledby="overview-title">
      <div class="section-head">
        <h1 class="section-title" id="overview-title">Overview</h1>
        <span class="section-caption" id="overview-caption">Terminal state</span>
      </div>
      <div class="overview-grid" id="overview-panel"></div>
      <div class="overview-meta" id="overview-meta"></div>
    </section>

    <section class="card" id="runtime" aria-labelledby="runtime-title">
      <div class="section-head">
        <h2 class="section-title" id="runtime-title">Runtime</h2>
        <span class="section-caption">Primary symbols first</span>
      </div>
      <div class="runtime-shell" id="runtime-shell">
        <div class="runtime-primary" id="runtime-primary"></div>
        <div class="runtime-secondary runtime-grid" id="runtime-panel"></div>
      </div>
    </section>

    <section class="card" id="competition" aria-labelledby="competition-title">
      <div class="section-head">
        <h2 class="section-title" id="competition-title">Competition</h2>
        <span class="section-caption">Tracked opportunities</span>
      </div>
      <div class="competition-list" id="competition-panel"></div>
    </section>

    <section class="card" id="quick-actions" aria-labelledby="quick-actions-title">
      <div class="section-head">
        <h2 class="section-title" id="quick-actions-title">Quick Actions</h2>
        <span class="section-caption">Deep links and server context</span>
      </div>
      <div class="quick-actions-grid" id="quick-actions-panel"></div>
      <div class="quick-actions-footer" id="server-meta"></div>
    </section>

    <div class="account-sheet-backdrop" id="account-sheet-backdrop" data-open="false"></div>
    <div class="account-sheet-shell" id="account-sheet-shell" data-sheet="account-picker" data-open="false">
      <div class="account-sheet" id="account-sheet" role="dialog" aria-modal="false" aria-label="Account picker">
        <h2 class="account-sheet-title">Choose account</h2>
        <div class="ghost-note">Tap an account slot to reload the console.</div>
        <div class="account-list" id="account-list"></div>
      </div>
    </div>
  </main>
  <script>
    let currentRegistry = null;
    let currentAccountId = "";
    let currentOverview = null;
    let competitionExpanded = false;

    async function fetchJson(url) {
      const response = await fetch(url, { headers: { Accept: "application/json" } });
      if (!response.ok) {
        throw new Error("Request failed for " + url + ": " + response.status);
      }
      return await response.json();
    }

    function escapeHtml(value) {
      return String(value || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;")
        .replace(/'/g, "&#39;");
    }

    function normalizeStatus(value) {
      const status = String(value || "degraded").trim().toLowerCase();
      if (status === "healthy" || status === "online" || status === "ready" || status === "running") {
        return "healthy";
      }
      if (status === "offline" || status === "failed" || status === "error") {
        return "offline";
      }
      return "degraded";
    }

    function statusChipHtml(label, status) {
      const normalized = normalizeStatus(status);
      return '<span class="status-chip is-' + normalized + '">' + escapeHtml(label) + "</span>";
    }

    function openAccountSheet() {
      document.getElementById("account-sheet-shell").setAttribute("data-open", "true");
      document.getElementById("account-sheet-backdrop").setAttribute("data-open", "true");
    }

    function closeAccountSheet() {
      document.getElementById("account-sheet-shell").setAttribute("data-open", "false");
      document.getElementById("account-sheet-backdrop").setAttribute("data-open", "false");
    }

    function formatDateTime(value) {
      if (!value) {
        return "--";
      }
      try {
        const date = new Date(value);
        if (Number.isNaN(date.getTime())) {
          return String(value);
        }
        return date.toLocaleString("zh-CN", {
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          hour12: false,
        });
      } catch (error) {
        return String(value);
      }
    }

    function titleFromLinkKey(key) {
      const mapping = {
        monitor: "Monitor",
        strategies: "Strategies",
        competition_board: "Competition",
        spot_runner: "Spot Runner",
        spot_strategies: "Spot Strategies",
        basis: "Basis",
      };
      return mapping[key] || key.replace(/_/g, " ");
    }

    function renderTopBar(overview) {
      const account = overview && overview.account ? overview.account : {};
      const server = overview && overview.server ? overview.server : {};
      const summary = overview && overview.summary ? overview.summary : {};
      const currentAccountLabelEl = document.getElementById("current-account-label");
      const subtitleEl = document.getElementById("console-subtitle");
      const chipEl = document.getElementById("health-chip");
      currentAccountLabelEl.textContent = account.label || account.id || "Console";
      subtitleEl.textContent =
        (server.label || server.id || "Unknown server") +
        " · " +
        (account.kind || "unknown") +
        " · " +
        String((overview && overview.fetched_at) || "--");
      chipEl.className = "status-chip is-" + normalizeStatus(summary.primary_status || overview && overview.health && overview.health.status);
      chipEl.textContent = summary.primary_status || (overview && overview.health && overview.health.status) || "unknown";
    }

    function renderOverviewPanel(overview) {
      const panel = document.getElementById("overview-panel");
      const meta = document.getElementById("overview-meta");
      const account = overview && overview.account ? overview.account : {};
      const server = overview && overview.server ? overview.server : {};
      const summary = overview && overview.summary ? overview.summary : {};
      const warnings = overview && overview.warnings ? overview.warnings : [];

      panel.innerHTML = [
        { label: "Status", value: summary.primary_status || "unknown" },
        { label: "Warnings", value: String(summary.warning_count || 0) },
        { label: "Runtime", value: formatDateTime(overview && overview.fetched_at) },
        { label: "Competitions", value: String(summary.competition_count || 0) },
      ].map(function (item) {
        return (
          '<div class="overview-tile">' +
          '<span class="overview-tile-label">' + escapeHtml(item.label) + "</span>" +
          "<strong>" + escapeHtml(item.value) + "</strong>" +
          "</div>"
        );
      }).join("");

      meta.innerHTML =
        '<div class="meta-line"><span>Account</span><strong>' + escapeHtml(account.label || account.id || "--") + "</strong></div>" +
        '<div class="meta-line"><span>Server</span><strong>' + escapeHtml(server.label || server.id || "--") + "</strong></div>" +
        '<div class="meta-line"><span>Health</span><strong>' + escapeHtml(summary.health_status || overview && overview.health && overview.health.status || "--") + "</strong></div>" +
        '<div class="meta-line"><span>Latest warning</span><strong>' + escapeHtml(warnings[0] || "none") + "</strong></div>";
    }

    function renderRuntimeDesk(overview) {
      const primary = document.getElementById("runtime-primary");
      const panel = document.getElementById("runtime-panel");
      const runtimeItems = []
        .concat(overview && overview.futures ? overview.futures : [])
        .concat(overview && overview.spot ? overview.spot : []);

      if (!runtimeItems.length) {
        primary.innerHTML = '<div class="ghost-note">No runtime summaries available for this account.</div>';
        panel.innerHTML = "";
        return;
      }

      const mainItem = runtimeItems[0];
      const mainSnapshot = mainItem.snapshot || {};
      const extraItems = runtimeItems.slice(1);
      const warningCount = (overview && overview.warnings && overview.warnings.length) || 0;

      primary.innerHTML =
        '<div class="runtime-primary-top">' +
        '<div><strong>' + escapeHtml(mainItem.symbol || "Unknown") + '</strong><div class="ghost-note">Primary tracked symbol</div></div>' +
        '<span class="runtime-badge">' + escapeHtml(mainItem.status || "unknown") + "</span>" +
        "</div>" +
        '<div class="runtime-stats">' +
        '<div class="runtime-stat"><small>Open orders</small><strong>' + escapeHtml(String(((mainSnapshot.open_orders || []).length || 0))) + "</strong></div>" +
        '<div class="runtime-stat"><small>Warnings</small><strong>' + escapeHtml(String(warningCount)) + "</strong></div>" +
        '<div class="runtime-stat"><small>Runner</small><strong>' + escapeHtml(mainSnapshot.runner_status || mainItem.status || "--") + "</strong></div>" +
        '<div class="runtime-stat"><small>Mode</small><strong>' + escapeHtml(mainSnapshot.strategy_mode || mainSnapshot.status || "--") + "</strong></div>" +
        "</div>";

      if (!extraItems.length) {
        panel.innerHTML = '<div class="ghost-note">No secondary runtime items.</div>';
        return;
      }

      panel.innerHTML = extraItems.map(function (item) {
        const snapshot = item.snapshot || {};
        return (
          '<div class="runtime-card">' +
          '<strong>' + escapeHtml(item.symbol || "--") + '</strong>' +
          '<div class="terminal-item-grid">' +
          '<div class="terminal-item-note">Status: ' + escapeHtml(item.status || "unknown") + "</div>" +
          '<div class="terminal-item-note">Open orders: ' + escapeHtml(String(((snapshot.open_orders || []).length || 0))) + "</div>" +
          "</div>" +
          "</div>"
        );
      }).join("");
    }

    function renderCompetitionPanel(overview) {
      const panel = document.getElementById("competition-panel");
      const competitions = overview && overview.competitions ? overview.competitions : [];
      if (!competitions.length) {
        panel.innerHTML = '<div class="ghost-note">No active competition items for this account.</div>';
        return;
      }

      const visibleItems = competitionExpanded ? competitions : competitions.slice(0, 4);
      panel.innerHTML = visibleItems.map(function (item) {
        return (
          '<div class="competition-card">' +
          '<strong>' + escapeHtml(item.label || item.symbol) + '</strong>' +
          '<div class="terminal-item-grid">' +
          '<div class="terminal-item-note">Symbol: ' + escapeHtml(item.symbol || "--") + "</div>" +
          '<div class="terminal-item-note">Market: ' + escapeHtml(item.market || "--") + "</div>" +
          '<div class="terminal-item-note">Floor: ' + escapeHtml(item.current_floor || "--") + "</div>" +
          '<div class="terminal-item-note">Ends: ' + escapeHtml(formatDateTime(item.activity_end_at)) + "</div>" +
          "</div>" +
          "</div>"
        );
      }).join("");

      if (competitions.length > 4) {
        panel.innerHTML +=
          '<button class="toggle-button" id="competition-toggle" type="button">' +
          escapeHtml(competitionExpanded ? "Show less" : "Show more") +
          "</button>";
        document.getElementById("competition-toggle").addEventListener("click", function () {
          competitionExpanded = !competitionExpanded;
          renderCompetitionPanel(currentOverview);
        });
      }
    }

    function renderQuickActions(overview) {
      const panel = document.getElementById("quick-actions-panel");
      const serverMeta = document.getElementById("server-meta");
      const links = overview && overview.links ? overview.links : {};
      const entries = Object.entries(links);
      const server = overview && overview.server ? overview.server : {};
      const health = overview && overview.health ? overview.health : {};

      if (!entries.length) {
        panel.innerHTML = '<div class="ghost-note">No quick actions configured for this account.</div>';
      } else {
        panel.innerHTML = entries.map(function (entry) {
          const key = entry[0];
          const href = entry[1];
          return (
            '<a class="quick-action" href="' + escapeHtml(href) + '" target="_blank" rel="noopener noreferrer">' +
            '<strong>' + escapeHtml(titleFromLinkKey(key)) + '</strong>' +
            '<small>' + escapeHtml("Open " + titleFromLinkKey(key).toLowerCase()) + '</small>' +
            "</a>"
          );
        }).join("");
      }

      serverMeta.innerHTML =
        '<div class="meta-line"><span>Server</span><strong>' + escapeHtml(server.label || server.id || "--") + "</strong></div>" +
        '<div class="meta-line"><span>Status</span><strong>' + escapeHtml(health.status || "--") + "</strong></div>" +
        '<div class="meta-line"><span>Base URL</span><strong>' + escapeHtml(server.base_url || "--") + "</strong></div>" +
        '<div class="meta-line"><span>Capabilities</span><strong>' + escapeHtml((server.capabilities || []).join(", ") || "--") + "</strong></div>";
    }

    function renderAccountList(registry, activeAccountId) {
      const accounts = registry && registry.accounts ? registry.accounts : [];
      const accountList = document.getElementById("account-list");
      accountList.innerHTML = accounts.map(function (account) {
        const activeClass = account.id === activeAccountId ? " is-active" : "";
        return (
          '<button class="account-option' + activeClass + '" type="button" data-account-id="' + escapeHtml(account.id) + '">' +
          '<strong>' + escapeHtml(account.label || account.id) + '</strong>' +
          '<div class="account-option-note">Server: ' + escapeHtml(account.server_id || "--") + "</div>" +
          '<div class="account-option-note">Kind: ' + escapeHtml(account.kind || "--") + "</div>" +
          "</button>"
        );
      }).join("");

      accountList.querySelectorAll("[data-account-id]").forEach(function (button) {
        button.addEventListener("click", function () {
          const accountId = button.getAttribute("data-account-id");
          closeAccountSheet();
          loadOverview(accountId);
        });
      });
    }

    function renderOverview(overview) {
      currentOverview = overview;
      renderTopBar(overview);
      renderOverviewPanel(overview);
      renderRuntimeDesk(overview);
      renderCompetitionPanel(overview);
      renderQuickActions(overview);
    }

    function renderConsoleFailure(error) {
      currentOverview = null;
      document.getElementById("health-chip").className = "status-chip is-offline";
      document.getElementById("health-chip").textContent = "Offline";
      document.getElementById("console-subtitle").textContent = error.message || "Overview unavailable";
      document.getElementById("overview-panel").innerHTML = '<div class="ghost-note">Overview unavailable.</div>';
      document.getElementById("overview-meta").innerHTML = "";
      document.getElementById("runtime-primary").innerHTML = '<div class="ghost-note">Overview unavailable.</div>';
      document.getElementById("runtime-panel").innerHTML = "";
      document.getElementById("competition-panel").innerHTML = "";
      document.getElementById("quick-actions-panel").innerHTML = "";
      document.getElementById("server-meta").innerHTML = "";
    }

    async function loadOverview(accountId) {
      const statusEl = document.getElementById("console-subtitle");
      try {
        statusEl.textContent = "Refreshing…";
        currentAccountId = accountId || currentAccountId;
        competitionExpanded = false;
        const overview = await fetchJson("/api/console/overview?account_id=" + encodeURIComponent(accountId));
        renderOverview(overview);
        renderAccountList(currentRegistry, currentAccountId);
        statusEl.textContent =
          (overview.server && (overview.server.label || overview.server.id) || "Unknown server") +
          " · " +
          (overview.account && overview.account.kind || "unknown") +
          " · Ready";
      } catch (error) {
        renderConsoleFailure(error);
      }
    }

    async function bootstrapConsole() {
      const statusEl = document.getElementById("console-subtitle");
      try {
        statusEl.textContent = "Loading registry";
        const registry = await fetchJson("/api/console/registry");
        currentRegistry = registry;
        currentAccountId =
          registry.default_account_id ||
          (registry.default_account && registry.default_account.id) ||
          ((registry.accounts && registry.accounts[0] && registry.accounts[0].id) || "");
        renderAccountList(currentRegistry, currentAccountId);
        await loadOverview(currentAccountId);
      } catch (error) {
        renderConsoleFailure(error);
      }
    }

    document.getElementById("open-account-sheet").addEventListener("click", openAccountSheet);
    document.getElementById("refresh-console").addEventListener("click", function () {
      if (currentAccountId) {
        loadOverview(currentAccountId);
      }
    });
    document.getElementById("account-sheet-backdrop").addEventListener("click", closeAccountSheet);

    bootstrapConsole();
  </script>
</body>
</html>
"""
