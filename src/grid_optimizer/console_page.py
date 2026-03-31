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
      padding: 76px 16px 24px;
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
    .sticky-context-bar strong { font-size: 0.96rem; letter-spacing: 0.02em; }
    .sticky-context-bar span { color: var(--muted); font-size: 0.92rem; }
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
    .stack { display: grid; gap: 10px; }
    .account-sheet-shell {
      position: fixed;
      left: 0;
      right: 0;
      bottom: 0;
      z-index: 30;
      display: flex;
      justify-content: center;
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
    }
    @media (min-width: 700px) {
      .metric-grid,
      .link-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <header class="sticky-context-bar">
    <strong>Console</strong>
    <span id="console-status">Booting</span>
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
      <div class="link-grid" id="competition-links">
        <a href="/competition_board">Competition board</a>
        <a href="/api/console/overview">Overview payload</a>
      </div>
    </section>

    <section class="card" id="runtime" aria-labelledby="runtime-title">
      <h2 class="section-title" id="runtime-title">Runtime</h2>
      <div class="stack" id="runtime-panel">
        <div class="ghost-note">Runtime checks and latest state appear here.</div>
      </div>
    </section>

    <section class="card" id="legacy-entries" aria-labelledby="legacy-entries-title">
      <h2 class="section-title" id="legacy-entries-title">Legacy Entries</h2>
      <div class="stack" id="legacy-entries-panel">
        <div class="ghost-note">Historical entries remain available for review.</div>
      </div>
    </section>

    <section class="card" id="server" aria-labelledby="server-title">
      <h2 class="section-title" id="server-title">Server</h2>
      <div class="stack" id="server-panel">
        <div class="ghost-note">Server status and links will be hydrated here.</div>
      </div>
    </section>

    <div class="account-sheet-shell" id="account-sheet-shell" data-sheet="account-picker">
      <div class="account-sheet" id="account-sheet" role="dialog" aria-modal="false" aria-label="Account picker">
        <div class="ghost-note">Bottom sheet for account selection.</div>
      </div>
    </div>
  </main>
  <script>
    async function fetchJson(url) {
      const response = await fetch(url, { headers: { Accept: "application/json" } });
      if (!response.ok) {
        throw new Error("Request failed for " + url + ": " + response.status);
      }
      return await response.json();
    }

    function updateHero(overview) {
      const accountEl = document.getElementById("hero-account");
      const statusEl = document.getElementById("hero-status");
      const runtimeEl = document.getElementById("hero-runtime");
      const warningsEl = document.getElementById("hero-warnings");
      const account = overview && overview.account ? overview.account : {};
      const summary = overview && overview.summary ? overview.summary : {};
      accountEl.textContent = account.label || account.id || "Unknown";
      statusEl.textContent = summary.primary_status || "unknown";
      runtimeEl.textContent = overview && overview.fetched_at ? overview.fetched_at : "--";
      warningsEl.textContent = String((overview && overview.warnings && overview.warnings.length) || 0);
    }

    async function bootstrapConsole() {
      const statusEl = document.getElementById("console-status");
      try {
        statusEl.textContent = "Loading registry";
        const registry = await fetchJson("/api/console/registry");
        const accountId = registry.default_account_id || (registry.default_account && registry.default_account.id) || "";
        const overviewUrl = accountId
          ? "/api/console/overview?account_id=" + encodeURIComponent(accountId)
          : "/api/console/overview";
        statusEl.textContent = "Loading overview";
        const overview = await fetchJson(overviewUrl);
        updateHero(overview);
        statusEl.textContent = "Ready";
      } catch (error) {
        statusEl.textContent = "Offline";
      }
    }

    bootstrapConsole();
  </script>
</body>
</html>
"""
