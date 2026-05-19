# Lightweight Strategy Editor Design

## Goal

Build a separate strategy parameter editor page that does not auto-refresh and does not load heavy monitoring datasets. The page should let us choose a symbol, load a preset or current runner config, edit parameters, save them, and manually refresh a small status summary without dragging down `grid_web`.

## Background

The current monitor page mixes several responsibilities:

- live strategy monitoring
- charts
- recent trades
- hourly statistics
- alert rendering
- preset selection
- runner parameter editing
- start / stop actions

That makes the page useful as a dashboard, but too heavy as an everyday parameter editor. Because it auto-refreshes and calls the same monitor snapshot path repeatedly, leaving the page open can keep triggering expensive local and remote reads. The new page should split out the editing workflow and make every server request user initiated.

## Selected Approach

Use a new standalone route:

- `GET /strategy_editor`
- `GET /strategy_editor.html`

The page is not a monitoring dashboard. It is a manual parameter editor with a small status panel.

First version includes:

- symbol selector with a manual input fallback
- preset selector
- manual "refresh status" button
- "load current config" button
- "load preset" button
- editable field form
- advanced JSON editor
- parameter explanation panel
- "save params" button

First version excludes:

- auto refresh
- charts
- hourly stats
- recent trade table
- full open order table
- alert stack
- start / stop buttons
- quick flatten
- automatic runner restart after save

The missing action buttons are intentional. Editing parameters should be safe to leave open. Starting and stopping remains on the existing monitor page until this lightweight editor proves stable.

## User Experience

The page opens idle. It should not fetch runner status, monitor snapshots, trade history, or symbol metrics until the user clicks a button.

Recommended initial screen:

- header: "策略参数编辑器"
- small note: "不会自动刷新；点击按钮才读取服务器数据。"
- symbol selector with a manual input fallback
- preset selector
- action row:
  - `刷新状态`
  - `载入当前参数`
  - `载入预设参数`
  - `保存参数`
- status summary panel
- parameter form and JSON editor
- explanation panel

Status summary should be small:

- symbol
- runner running / stopped
- pid if present
- current `strategy_profile`
- current `strategy_mode`
- current long / short position notional summary if available cheaply
- strategy open order count if available cheaply
- latest loop event state if available from local summary file
- last refreshed timestamp

No data should update on a timer.

## Backend API

Add a lightweight status endpoint:

- `GET /api/strategy_editor/status?symbol=AIGENSYNUSDT`

The response should contain only the editor needs:

```json
{
  "ok": true,
  "symbol": "AIGENSYNUSDT",
  "runner": {
    "running": true,
    "pid": 12345,
    "config": {
      "strategy_profile": "aigensynusdt_best_quote_maker_volume_v1",
      "strategy_mode": "one_way_long"
    }
  },
  "position": {
    "summary": "LONG 742.1U",
    "long_notional": 742.1,
    "short_notional": 0.0
  },
  "orders": {
    "strategy_open_order_count": 2
  },
  "latest_loop": {
    "strategy_intent": "make_volume",
    "active_state": "NORMAL",
    "repair_ladder_level": "",
    "error_message": ""
  },
  "updated_at": "2026-05-19T12:00:00+00:00"
}
```

Implementation should prefer local files and existing lightweight helpers:

- saved control config from `_load_runner_control_config(symbol)`
- runner process status from existing process helpers
- latest summary line from `output/<symbol>_loop_runner_summary.jsonl`
- latest submit or plan file only if it is already cheap to read

The endpoint must not call the full monitor snapshot builder, hourly stats builder, trade history aggregation, remote running status overview, or Binance-heavy paths unless those are already part of a cheap local helper. If a field is unavailable cheaply, return `null` or `"--"` instead of doing expensive work.

Existing mutation endpoints can be reused:

- `POST /api/runner/save`

The first version does not need a new save endpoint.

## Frontend Data Flow

On page load:

1. Render static page.
2. Populate preset data from embedded local preset definitions or a cheap preset summary endpoint.
3. Do not call status automatically.
4. Do not start any timer.

When user clicks `刷新状态`:

1. Call `/api/strategy_editor/status?symbol=...`.
2. Replace only the status summary panel.
3. Do not overwrite editor fields unless user explicitly clicked `载入当前参数`.

When user clicks `载入当前参数`:

1. Call `/api/strategy_editor/status?symbol=...`.
2. Read `runner.config`.
3. Load config into the field form and JSON editor.
4. Render explanation panel from that config.

When user clicks `载入预设参数`:

1. Load the selected preset config.
2. Set `strategy_profile` to the preset key.
3. Normalize runtime paths for the selected symbol.
4. Render field form, JSON editor, and explanation panel.

When user clicks `保存参数`:

1. Build payload from the editor.
2. Call `POST /api/runner/save`.
3. Show save result.
4. Do not auto refresh status unless the user clicks `刷新状态`.
5. Do not start or restart runner.

## Code Organization

The codebase currently stores large HTML pages as Python string constants in `src/grid_optimizer/web.py`. For the first version, follow the existing pattern to minimize risk:

- Add `STRATEGY_EDITOR_PAGE` as a new HTML string.
- Add route handling in `_Handler.do_GET`.
- Add lightweight API builder functions near existing runner helpers.
- Reuse existing JavaScript helper ideas from `MONITOR_PAGE`, but copy only the editor-specific pieces.

Do not refactor the whole web module in this change. The goal is to reduce runtime load, not restructure every page.

## Safety Rules

- No auto refresh timers.
- No hidden status fetch on page load.
- No full monitor snapshot calls from the editor page.
- No start / stop buttons in first version.
- No quick flatten button in first version.
- Saving parameters must not restart a running runner.
- If JSON is invalid, keep the current text and show the parse error.
- If status fetch fails, keep the editor usable and show the error only in the status panel.

## Tests

Add tests in `tests/test_web_security.py` for:

- `STRATEGY_EDITOR_PAGE` is routed from `/strategy_editor`.
- The page includes no `setInterval(` call.
- The page includes no automatic `loadMonitor()` style initialization.
- The page includes buttons for `刷新状态`, `载入当前参数`, `载入预设参数`, and `保存参数`.
- The status endpoint does not call the heavy monitor snapshot function.
- The status endpoint returns a small config and summary shape for a symbol.
- Saving from the editor still uses `/api/runner/save` and does not call `/api/runner/start`.

Use the existing pytest command style:

```bash
PYTEST_DISABLE_PLUGIN_AUTOLOAD=1 PYTHONPATH=src python -m pytest tests/test_web_security.py -q
```

## Rollout

First deploy the new page without changing the current monitor page. Operators can use:

- heavy monitor page when actively watching a live run
- lightweight strategy editor when only adjusting parameters

After the lightweight page is stable, we can decide whether to remove the parameter editor block from the heavy monitor page or leave it as a fallback.

## Final Decisions

- Route name is `strategy_editor`.
- The symbol control uses the existing monitor symbol list when available and keeps a manual uppercase symbol input fallback.
- Start / stop buttons are deliberately out of scope for first version.
- The status endpoint should return partial status rather than perform expensive fallback queries.
