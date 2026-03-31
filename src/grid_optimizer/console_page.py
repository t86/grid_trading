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
      --bg: #f6efe6;
      --panel: #fff9f1;
      --panel-strong: #fff3e4;
      --ink: #2c241d;
      --muted: #6f5c4c;
      --accent: #b8642f;
      --accent-soft: rgba(184, 100, 47, 0.12);
      --border: rgba(92, 67, 47, 0.14);
      --shadow: 0 14px 40px rgba(77, 50, 28, 0.12);
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font: 16px/1.45 system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--ink);
      background:
        radial-gradient(circle at top, rgba(184, 100, 47, 0.12), transparent 34%),
        linear-gradient(180deg, #fbf5ed 0%, var(--bg) 100%);
    }
    main {
      max-width: 720px;
      margin: 0 auto;
      padding: 76px 16px 176px;
    }
    .sticky-context-bar {
      position: sticky;
      top: 0;
      z-index: 20;
      display: flex;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
      padding: 14px 16px;
      background: rgba(255, 250, 244, 0.92);
      backdrop-filter: blur(12px);
      border-bottom: 1px solid var(--border);
      box-shadow: 0 6px 24px rgba(77, 50, 28, 0.06);
    }
    .context-meta {
      display: grid;
      gap: 2px;
      min-width: 0;
    }
    .sticky-context-bar strong { font-size: 0.96rem; letter-spacing: 0.02em; }
    .sticky-context-bar span { color: var(--muted); font-size: 0.92rem; }
    .context-actions {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .context-actions button {
      border: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.84);
      color: var(--ink);
      border-radius: 999px;
      min-height: 40px;
      padding: 0 14px;
      font: inherit;
    }
    .card {
      margin: 0 0 14px;
      padding: 16px;
      border: 1px solid var(--border);
      border-radius: 20px;
      background: linear-gradient(180deg, var(--panel) 0%, #fff 100%);
      box-shadow: var(--shadow);
    }
    .hero-summary {
      background: linear-gradient(180deg, #fff6ea 0%, #fff 100%);
      border-color: rgba(184, 100, 47, 0.22);
    }
    .section-title {
      margin: 0 0 12px;
      font-size: 1rem;
      letter-spacing: 0.04em;
      text-transform: uppercase;
    }
    .metric-grid,
    .link-grid {
      display: grid;
      gap: 10px;
      grid-template-columns: repeat(2, minmax(0, 1fr));
    }
    .metric {
      padding: 12px;
      border-radius: 16px;
      background: var(--panel-strong);
      border: 1px solid rgba(184, 100, 47, 0.14);
    }
    .metric small,
    .link-grid a,
    .ghost-note {
      color: var(--muted);
    }
    .metric strong { display: block; margin-top: 2px; font-size: 1.1rem; }
    .link-grid a {
      display: block;
      padding: 12px;
      border-radius: 16px;
      border: 1px solid var(--border);
      background: #fff;
      text-decoration: none;
    }
    .link-card-title {
      display: block;
      color: var(--ink);
      font-weight: 700;
      margin-bottom: 4px;
    }
    .stack { display: grid; gap: 10px; }
    .runtime-card,
    .competition-card,
    .server-card,
    .account-option {
      padding: 12px;
      border-radius: 16px;
      border: 1px solid var(--border);
      background: rgba(255, 255, 255, 0.9);
    }
    .runtime-card strong,
    .competition-card strong,
    .server-card strong,
    .account-option strong {
      display: block;
      margin-bottom: 4px;
    }
    .account-sheet-backdrop {
      position: fixed;
      inset: 0;
      background: rgba(44, 36, 29, 0.28);
      opacity: 0;
      pointer-events: none;
      transition: opacity 160ms ease;
      z-index: 25;
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
      width: min(720px, 100%);
      padding: 16px;
      border-radius: 22px 22px 0 0;
      background: #fff;
      border: 1px solid var(--border);
      box-shadow: 0 -12px 36px rgba(77, 50, 28, 0.12);
      pointer-events: auto;
      transform: translateY(calc(100% + 16px));
      transition: transform 160ms ease;
    }
    .account-sheet-shell[data-open="true"] .account-sheet {
      transform: translateY(0);
    }
    .account-sheet-backdrop[data-open="true"] {
      opacity: 1;
      pointer-events: auto;
    }
    .account-list {
      display: grid;
      gap: 10px;
      margin-top: 14px;
    }
    .account-option {
      width: 100%;
      text-align: left;
      font: inherit;
      color: var(--ink);
    }
    .account-option.is-active {
      border-color: rgba(184, 100, 47, 0.5);
      background: var(--panel-strong);
    }
    @media (min-width: 700px) {
      .metric-grid,
      .link-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <header class="sticky-context-bar">
    <div class="context-meta">
      <strong id="current-account-label">Console</strong>
      <span id="console-status">Booting</span>
    </div>
    <div class="context-actions">
      <button id="open-account-sheet" type="button">Accounts</button>
      <button id="refresh-console" type="button">Refresh</button>
    </div>
  </header>
  <main>
    <section class="card hero-summary" id="hero-summary" aria-labelledby="hero-summary-title">
      <h1 class="section-title" id="hero-summary-title">Hero Summary</h1>
      <div class="metric-grid">
        <div class="metric"><small>Account</small><strong id="hero-account">Loading</strong></div>
        <div class="metric"><small>Status</small><strong id="hero-status">Loading</strong></div>
        <div class="metric"><small>Runtime</small><strong id="hero-runtime">--</strong></div>
        <div class="metric"><small>Warnings</small><strong id="hero-warnings">0</strong></div>
      </div>
    </section>

    <section class="card" id="competition" aria-labelledby="competition-title">
      <h2 class="section-title" id="competition-title">Competition</h2>
      <div class="stack" id="competition-panel"></div>
    </section>

    <section class="card" id="runtime" aria-labelledby="runtime-title">
      <h2 class="section-title" id="runtime-title">Runtime</h2>
      <div class="stack" id="runtime-panel">
        <div class="ghost-note">Runtime checks and latest state appear here.</div>
      </div>
    </section>

    <section class="card" id="legacy-entries" aria-labelledby="legacy-entries-title">
      <h2 class="section-title" id="legacy-entries-title">Legacy Entries</h2>
      <div class="link-grid" id="legacy-links"></div>
    </section>

    <section class="card" id="server" aria-labelledby="server-title">
      <h2 class="section-title" id="server-title">Server</h2>
      <div class="stack" id="server-panel">
        <div class="ghost-note">Server status and links will be hydrated here.</div>
      </div>
    </section>
    <div class="account-sheet-backdrop" id="account-sheet-backdrop" data-open="false"></div>
    <div class="account-sheet-shell" id="account-sheet-shell" data-sheet="account-picker" data-open="false">
      <div class="account-sheet" id="account-sheet" role="dialog" aria-modal="false" aria-label="Account picker">
        <strong>Choose account</strong>
        <div class="ghost-note">Tap an account context to reload the console.</div>
        <div class="account-list" id="account-list"></div>
      </div>
    </div>
  </main>
  <script>
    let currentRegistry = null;
    let currentAccountId = "";

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

    function openAccountSheet() {
      document.getElementById("account-sheet-shell").setAttribute("data-open", "true");
      document.getElementById("account-sheet-backdrop").setAttribute("data-open", "true");
    }

    function closeAccountSheet() {
      document.getElementById("account-sheet-shell").setAttribute("data-open", "false");
      document.getElementById("account-sheet-backdrop").setAttribute("data-open", "false");
    }

    function renderHero(overview) {
      const accountEl = document.getElementById("hero-account");
      const statusEl = document.getElementById("hero-status");
      const runtimeEl = document.getElementById("hero-runtime");
      const warningsEl = document.getElementById("hero-warnings");
      const currentAccountLabelEl = document.getElementById("current-account-label");
      const account = overview && overview.account ? overview.account : {};
      const summary = overview && overview.summary ? overview.summary : {};
      accountEl.textContent = account.label || account.id || "Unknown";
      currentAccountLabelEl.textContent = account.label || account.id || "Console";
      statusEl.textContent = summary.primary_status || "unknown";
      runtimeEl.textContent = overview && overview.fetched_at ? overview.fetched_at : "--";
      warningsEl.textContent = String((overview && overview.warnings && overview.warnings.length) || 0);
    }

    function renderCompetitionPanel(overview) {
      const panel = document.getElementById("competition-panel");
      const competitions = overview && overview.competitions ? overview.competitions : [];
      if (!competitions.length) {
        panel.innerHTML = '<div class="ghost-note">No active competition items for this account.</div>';
        return;
      }
      panel.innerHTML = competitions.map(function (item) {
        return (
          '<div class="competition-card">' +
          '<strong>' + escapeHtml(item.label || item.symbol) + '</strong>' +
          '<div class="ghost-note">Symbol: ' + escapeHtml(item.symbol) + '</div>' +
          '<div class="ghost-note">Market: ' + escapeHtml(item.market || "unknown") + '</div>' +
          "</div>"
        );
      }).join("");
    }

    function renderRuntimeSection(overview) {
      const panel = document.getElementById("runtime-panel");
      const runtimeItems = []
        .concat(overview && overview.futures ? overview.futures : [])
        .concat(overview && overview.spot ? overview.spot : []);
      if (!runtimeItems.length) {
        panel.innerHTML = '<div class="ghost-note">No runtime summaries available for this account.</div>';
        return;
      }
      panel.innerHTML = runtimeItems.map(function (item) {
        const snapshot = item.snapshot || {};
        return (
          '<div class="runtime-card">' +
          '<strong>' + escapeHtml(item.symbol) + '</strong>' +
          '<div class="ghost-note">Status: ' + escapeHtml(item.status || "unknown") + '</div>' +
          '<div class="ghost-note">Open orders: ' + escapeHtml((snapshot.open_orders || []).length || 0) + '</div>' +
          "</div>"
        );
      }).join("");
    }

    function renderLegacyLinks(overview) {
      const container = document.getElementById("legacy-links");
      const links = overview && overview.links ? overview.links : {};
      const entries = Object.entries(links);
      if (!entries.length) {
        container.innerHTML = '<div class="ghost-note">No legacy entries configured for this account.</div>';
        return;
      }
      container.innerHTML = entries.map(function (entry) {
        const key = entry[0];
        const href = entry[1];
        return (
          '<a href="' + escapeHtml(href) + '" target="_blank" rel="noopener noreferrer">' +
          '<span class="link-card-title">' + escapeHtml(key.replace(/_/g, " ")) + '</span>' +
          '<span class="ghost-note">' + escapeHtml(href) + '</span>' +
          "</a>"
        );
      }).join("");
    }

    function renderServerSection(overview) {
      const panel = document.getElementById("server-panel");
      const server = overview && overview.server ? overview.server : {};
      const health = overview && overview.health ? overview.health : {};
      panel.innerHTML =
        '<div class="server-card">' +
        '<strong>' + escapeHtml(server.label || server.id || "Unknown server") + '</strong>' +
        '<div class="ghost-note">Status: ' + escapeHtml(health.status || "unknown") + '</div>' +
        '<div class="ghost-note">Base URL: ' + escapeHtml(server.base_url || "--") + '</div>' +
        '<div class="ghost-note">Capabilities: ' + escapeHtml((server.capabilities || []).join(", ") || "--") + '</div>' +
        "</div>";
    }

    function renderAccountList(registry, activeAccountId) {
      const accounts = registry && registry.accounts ? registry.accounts : [];
      const accountList = document.getElementById("account-list");
      accountList.innerHTML = accounts.map(function (account) {
        const activeClass = account.id === activeAccountId ? " is-active" : "";
        return (
          '<button class="account-option' + activeClass + '" type="button" data-account-id="' + escapeHtml(account.id) + '">' +
          '<strong>' + escapeHtml(account.label || account.id) + '</strong>' +
          '<div class="ghost-note">Kind: ' + escapeHtml(account.kind || "unknown") + '</div>' +
          '<div class="ghost-note">Server: ' + escapeHtml(account.server_id || "--") + '</div>' +
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
      renderHero(overview);
      renderCompetitionPanel(overview);
      renderRuntimeSection(overview);
      renderLegacyLinks(overview);
      renderServerSection(overview);
    }

    async function loadOverview(accountId) {
      const statusEl = document.getElementById("console-status");
      try {
        statusEl.textContent = "Loading overview";
        currentAccountId = accountId || currentAccountId;
        const overview = await fetchJson("/api/console/overview?account_id=" + encodeURIComponent(accountId));
        renderOverview(overview);
        renderAccountList(currentRegistry, currentAccountId);
        statusEl.textContent = "Ready";
      } catch (error) {
        statusEl.textContent = "Offline";
        document.getElementById("runtime-panel").innerHTML =
          '<div class="ghost-note">Overview unavailable: ' + escapeHtml(error.message) + '</div>';
      }
    }

    async function bootstrapConsole() {
      const statusEl = document.getElementById("console-status");
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
        statusEl.textContent = "Offline";
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
