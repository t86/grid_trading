# Runner Preset Capture And Sync Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a futures runner capability that captures the currently running strategy config as a reusable preset stored in the repo, keeps custom futures presets on the same shared source, and lets start/restart execute strictly from the preset selected in the dropdown.

**Architecture:** Introduce a repo-managed user preset store in `config/runner_user_presets.json`, then route futures preset reads/writes through one shared helper layer so built-in presets, custom-grid presets, and captured-running presets all resolve through the same map. Add a new `save_running` API plus monitor-page controls, and keep runner start semantics explicit by resolving the selected preset before normalizing the final config.

**Tech Stack:** Python 3.11, `unittest`, existing `grid_optimizer.web` server-rendered HTML/JS, JSON config files, Playwright/manual browser verification for the live monitors.

---

### Task 1: Add Repo-Managed Futures User Preset Storage

**Files:**
- Create: `config/runner_user_presets.json`
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing storage tests**

```python
@patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
def test_runner_preset_summaries_read_repo_managed_user_presets(self) -> None:
    path = Path("output/test_runner_user_presets.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "captured_bard": {
                    "key": "captured_bard",
                    "label": "BARD 线上运行参数",
                    "description": "captured",
                    "symbol": "BARDUSDT",
                    "custom": True,
                    "startable": True,
                    "kind": "synthetic",
                    "source": "captured_running_config",
                    "config": {"symbol": "BARDUSDT", "strategy_mode": "synthetic_neutral"},
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    try:
        summaries = _runner_preset_summaries("BARDUSDT")
        preset = next(item for item in summaries if item["key"] == "captured_bard")
        self.assertTrue(preset["custom"])
        self.assertEqual(preset["symbol"], "BARDUSDT")
    finally:
        path.unlink(missing_ok=True)

@patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
def test_custom_grid_runner_preset_is_saved_into_repo_managed_store(self) -> None:
    path = Path("output/test_runner_user_presets.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")
    with patch("grid_optimizer.web._run_grid_preview") as mock_preview:
        mock_preview.return_value = {
            "ok": True,
            "summary": {
                "symbol": "OPNUSDT",
                "strategy_direction": "long",
                "grid_count": 8,
                "current_price": 0.25,
                "position_budget_notional": 200.0,
                "active_buy_orders": 4,
                "active_sell_orders": 4,
                "startup_long_notional": 30.0,
                "full_long_entry_notional": 200.0,
                "symbol_info": {"min_notional": 5.0, "min_qty": 1.0},
            },
            "rows": [],
        }
        result = _create_custom_grid_runner_preset(
            {
                "contract_type": "usdm",
                "symbol": "OPNUSDT",
                "name": "OPN 自定义",
                "strategy_direction": "long",
                "grid_level_mode": "arithmetic",
                "min_price": 0.20,
                "max_price": 0.30,
                "n": 8,
                "margin_amount": 100.0,
                "leverage": 2.0,
            }
        )
    saved = json.loads(path.read_text(encoding="utf-8"))
    self.assertIn(result["preset_key"], saved)
    self.assertEqual(saved[result["preset_key"]]["source"], "custom_grid")
```

- [ ] **Step 2: Run the storage tests to verify they fail**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k "repo_managed_user_presets or custom_grid_runner_preset_is_saved_into_repo_managed_store" -v`
Expected: FAIL because `RUNNER_USER_PRESETS_PATH` and the shared repo-managed preset helpers do not exist yet.

- [ ] **Step 3: Implement the shared repo-managed preset store**

```python
# src/grid_optimizer/web.py
RUNNER_USER_PRESETS_PATH = Path("config/runner_user_presets.json")


def _load_runner_user_presets() -> dict[str, dict[str, Any]]:
    payload = _read_json_dict(RUNNER_USER_PRESETS_PATH)
    if not payload:
        return {}
    return {str(key): value for key, value in payload.items() if isinstance(value, dict)}


def _save_runner_user_presets(presets: dict[str, dict[str, Any]]) -> None:
    RUNNER_USER_PRESETS_PATH.parent.mkdir(parents=True, exist_ok=True)
    RUNNER_USER_PRESETS_PATH.write_text(json.dumps(presets, ensure_ascii=False, indent=2), encoding="utf-8")


def _load_custom_runner_presets() -> dict[str, dict[str, Any]]:
    return _load_runner_user_presets()


def _save_custom_runner_presets(presets: dict[str, dict[str, Any]]) -> None:
    _save_runner_user_presets(presets)


def _runner_preset_map(symbol: str | None = None) -> dict[str, dict[str, Any]]:
    requested_symbol = str(symbol or "").upper().strip()
    merged: dict[str, dict[str, Any]] = {}
    for key, item in RUNNER_STRATEGY_PRESETS.items():
        preset_symbol = str(item.get("symbol", "")).upper().strip()
        if requested_symbol and preset_symbol and preset_symbol != requested_symbol:
            continue
        merged[key] = item
    for key, item in _load_runner_user_presets().items():
        preset_symbol = str(item.get("symbol", "")).upper().strip()
        if requested_symbol and preset_symbol and preset_symbol != requested_symbol:
            continue
        if key in merged:
            continue
        merged[key] = item
    return merged
```

- [ ] **Step 4: Mark custom-grid presets with an explicit source**

```python
# src/grid_optimizer/web.py inside _build_custom_grid_runner_preset
preset = {
    "key": preset_key,
    "label": params["name"],
    "description": description,
    "startable": True,
    "kind": "custom_grid",
    "custom": True,
    "source": "custom_grid",
    "symbol": symbol,
    "created_at": datetime.now(timezone.utc).isoformat(),
    ...
}
```

- [ ] **Step 5: Add the tracked repo file**

```json
{}
```

- [ ] **Step 6: Run the storage tests to verify they pass**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k "repo_managed_user_presets or custom_grid_runner_preset_is_saved_into_repo_managed_store" -v`
Expected: PASS

- [ ] **Step 7: Commit**

```bash
git add config/runner_user_presets.json src/grid_optimizer/web.py tests/test_web_security.py
git commit -m "feat: add repo-managed futures user preset store"
```

### Task 2: Add “Save Running Strategy As Preset” Backend API

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing API tests**

```python
@patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
@patch("grid_optimizer.web._read_runner_process_for_symbol")
def test_save_running_runner_preset_captures_current_config(self, mock_read_runner) -> None:
    path = Path("output/test_runner_user_presets.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("{}", encoding="utf-8")
    mock_read_runner.return_value = {
        "is_running": True,
        "config": {
            "symbol": "BARDUSDT",
            "strategy_profile": "bard_12h_push_neutral_v2",
            "strategy_mode": "synthetic_neutral",
            "step_price": 0.0001,
            "per_order_notional": 100.0,
            "state_path": "output/nightusdt_loop_state.json",
            "plan_json": "output/nightusdt_loop_latest_plan.json",
            "submit_report_json": "output/nightusdt_loop_latest_submit.json",
            "summary_jsonl": "output/nightusdt_loop_events.jsonl",
        },
    }

    result = _save_running_runner_preset(
        {"symbol": "BARDUSDT", "name": "BARD 8788 当前运行", "description": "captured from monitor"}
    )

    self.assertEqual(result["preset"]["source"], "captured_running_config")
    self.assertEqual(result["preset"]["config"]["symbol"], "BARDUSDT")
    self.assertEqual(result["preset"]["config"]["strategy_mode"], "synthetic_neutral")
    self.assertEqual(result["preset"]["config"]["state_path"], "output/bardusdt_loop_state.json")
    self.assertEqual(result["preset"]["config"]["plan_json"], "output/bardusdt_loop_latest_plan.json")
    self.assertEqual(result["preset"]["config"]["submit_report_json"], "output/bardusdt_loop_latest_submit.json")
    self.assertEqual(result["preset"]["config"]["summary_jsonl"], "output/bardusdt_loop_events.jsonl")

@patch("grid_optimizer.web._read_runner_process_for_symbol")
def test_save_running_runner_preset_rejects_missing_running_config(self, mock_read_runner) -> None:
    mock_read_runner.return_value = {"is_running": False, "config": {}}
    with self.assertRaisesRegex(ValueError, "没有运行中策略"):
        _save_running_runner_preset({"symbol": "BARDUSDT", "name": "bad"})
```

- [ ] **Step 2: Run the API tests to verify they fail**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k "save_running_runner_preset" -v`
Expected: FAIL because `_save_running_runner_preset` and the new API route do not exist yet.

- [ ] **Step 3: Implement the capture helper**

```python
def _runner_user_preset_key(symbol: str, name: str, *, prefix: str = "captured") -> str:
    base = f"{prefix}_{_symbol_output_slug(symbol)}_{_slugify_runner_preset_name(name)}"
    existing = set(_runner_preset_map().keys())
    if base not in existing:
        return base
    index = 2
    while f"{base}_{index}" in existing:
        index += 1
    return f"{base}_{index}"


def _preset_kind_from_strategy_mode(strategy_mode: str) -> str:
    normalized = str(strategy_mode or "").strip()
    if normalized in {"synthetic_neutral"}:
        return "synthetic"
    if normalized in {"hedge_neutral"}:
        return "hedge"
    if normalized in {"inventory_target_neutral"}:
        return "target_neutral"
    return "one_way"


def _save_running_runner_preset(payload: dict[str, Any]) -> dict[str, Any]:
    symbol = str(payload.get("symbol", "")).upper().strip()
    name = str(payload.get("name", "")).strip()
    description = str(payload.get("description", "")).strip()
    if not symbol:
        raise ValueError("symbol is required")
    if not name:
        raise ValueError("strategy name is required")
    runner = _read_runner_process_for_symbol(symbol)
    runner_config = dict(runner.get("config") or {})
    if not runner.get("is_running") or not runner_config:
        raise ValueError(f"{symbol} 当前没有运行中策略")
    runner_config.setdefault("symbol", symbol)
    normalized_config = _normalize_runner_control_payload(runner_config)
    normalized_config = _normalize_runner_runtime_paths(normalized_config, symbol)
    preset_key = _runner_user_preset_key(symbol, name)
    preset = {
        "key": preset_key,
        "label": name,
        "description": description or f"{symbol} 当前运行策略抓取",
        "symbol": symbol,
        "custom": True,
        "startable": True,
        "kind": _preset_kind_from_strategy_mode(str(normalized_config.get('strategy_mode', 'one_way_long'))),
        "source": "captured_running_config",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "config": normalized_config,
    }
    presets = _load_runner_user_presets()
    presets[preset_key] = preset
    _save_runner_user_presets(presets)
    return {"ok": True, "preset_key": preset_key, "preset": preset, "runner_presets": _runner_preset_summaries(symbol)}
```

- [ ] **Step 4: Wire the new API route**

```python
# src/grid_optimizer/web.py allowed POST paths
"/api/runner/presets/save_running",

# src/grid_optimizer/web.py request handler
if path == "/api/runner/presets/save_running":
    try:
        result = _save_running_runner_preset(payload)
        self._send_json(result, status=200)
    except ValueError as exc:
        self._send_json({"ok": False, "error": str(exc)}, status=400)
    except Exception as exc:
        self._send_json({"ok": False, "error": f"{type(exc).__name__}: {exc}"}, status=500)
    return
```

- [ ] **Step 5: Run the API tests to verify they pass**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k "save_running_runner_preset" -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/grid_optimizer/web.py tests/test_web_security.py
git commit -m "feat: add running futures preset capture api"
```

### Task 3: Add Futures Monitor UI For Capturing Running Presets

**Files:**
- Modify: `src/grid_optimizer/web.py`
- Test: `tests/test_web_security.py`

- [ ] **Step 1: Write the failing UI tests**

```python
def test_monitor_page_contains_save_running_preset_controls(self) -> None:
    self.assertIn('id="save_running_preset_name"', MONITOR_PAGE)
    self.assertIn('id="save_running_preset_description"', MONITOR_PAGE)
    self.assertIn('id="save_running_preset_btn"', MONITOR_PAGE)
    self.assertIn("/api/runner/presets/save_running", MONITOR_PAGE)
```

- [ ] **Step 2: Run the UI test to verify it fails**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k "save_running_preset_controls" -v`
Expected: FAIL because the monitor page does not yet render the new controls or JS handler.

- [ ] **Step 3: Add the monitor-page controls**

```html
<div class="runner-preset-capture">
  <label>保存当前运行策略为预设</label>
  <input id="save_running_preset_name" placeholder="例如：BARD 8788 当前运行" />
  <input id="save_running_preset_description" placeholder="可选说明：来源 / 用途 / 服务器" />
  <button id="save_running_preset_btn">保存当前运行策略为预设</button>
</div>
```

- [ ] **Step 4: Add the JS handler**

```javascript
async function saveRunningStrategyAsPreset() {
  const selectedSymbol = symbolEl.value.trim().toUpperCase() || "NIGHTUSDT";
  const name = String(saveRunningPresetNameEl.value || "").trim();
  const description = String(saveRunningPresetDescriptionEl.value || "").trim();
  if (!name) {
    runnerParamsMetaEl.textContent = "请先填写预设名称。";
    return;
  }
  saveRunningPresetBtn.disabled = true;
  runnerParamsMetaEl.textContent = `正在保存 ${selectedSymbol} 当前运行策略到预设...`;
  try {
    const resp = await fetch("/api/runner/presets/save_running", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ symbol: selectedSymbol, name, description }),
    });
    const data = await readJsonResponse(resp);
    if (!resp.ok || !data.ok) throw new Error(data.error || `HTTP ${resp.status}`);
    if (Array.isArray(data.runner_presets)) {
      runnerPresets = data.runner_presets;
      strategyPresetEl.innerHTML = runnerPresets.map((item) => `
        <option value="${escapeHtml(item.key)}">${escapeHtml(item.label)}</option>
      `).join("");
      strategyPresetEl.value = data.preset_key;
      renderPresetMeta({ ...(latestMonitorData || {}), runner_presets: runnerPresets });
    }
    runnerParamsMetaEl.textContent = `已保存预设 ${data.preset.label}，后续启动/重启将严格按该预设执行。`;
  } catch (err) {
    runnerParamsMetaEl.textContent = `保存预设失败: ${err}`;
  } finally {
    saveRunningPresetBtn.disabled = false;
  }
}

saveRunningPresetBtn.addEventListener("click", saveRunningStrategyAsPreset);
```

- [ ] **Step 5: Run the UI test to verify it passes**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k "save_running_preset_controls" -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add src/grid_optimizer/web.py tests/test_web_security.py
git commit -m "feat: add runner preset capture controls"
```

### Task 4: Verify Preset Start Semantics And Capture The Two Live BARD Strategies

**Files:**
- Modify: `tests/test_web_security.py`
- Modify: `config/runner_user_presets.json`
- Verify live monitors: `http://43.155.136.111:8788/monitor`, `http://43.131.232.150:8789/monitor`

- [ ] **Step 1: Write the failing start-resolution test**

```python
@patch("grid_optimizer.web.RUNNER_USER_PRESETS_PATH", new=Path("output/test_runner_user_presets.json"))
def test_resolve_runner_start_config_uses_selected_captured_preset(self) -> None:
    path = Path("output/test_runner_user_presets.json")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "captured_bard": {
                    "key": "captured_bard",
                    "label": "BARD captured",
                    "description": "captured",
                    "symbol": "BARDUSDT",
                    "custom": True,
                    "startable": True,
                    "kind": "synthetic",
                    "source": "captured_running_config",
                    "config": {
                        "symbol": "BARDUSDT",
                        "strategy_profile": "captured_bard",
                        "strategy_mode": "synthetic_neutral",
                        "step_price": 0.0001,
                        "per_order_notional": 88.0,
                        "autotune_symbol_enabled": False,
                    },
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    config = _resolve_runner_start_config({"symbol": "BARDUSDT", "strategy_profile": "captured_bard"})
    self.assertEqual(config["strategy_profile"], "captured_bard")
    self.assertEqual(config["strategy_mode"], "synthetic_neutral")
    self.assertEqual(config["per_order_notional"], 88.0)
    self.assertFalse(config["autotune_symbol_enabled"])
```

- [ ] **Step 2: Run the start-resolution test to verify it fails**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k "uses_selected_captured_preset" -v`
Expected: FAIL until the repo-managed user preset flow is fully wired through `_resolve_runner_start_config`.

- [ ] **Step 3: Make preset start semantics explicit and keep runtime-path normalization**

```python
def _resolve_runner_start_config(payload: dict[str, Any]) -> dict[str, Any]:
    raw_payload = dict(payload)
    config = _normalize_runner_control_payload(payload)
    profile = str(config.get("strategy_profile", RUNNER_DEFAULT_CONFIG["strategy_profile"])).strip() or RUNNER_DEFAULT_CONFIG["strategy_profile"]
    preset = _runner_preset_map(str(config.get("symbol", ""))).get(profile)
    if preset is None:
        preset = _runner_preset_map().get(profile)
    if preset is None:
        raise ValueError(f"Unknown strategy_profile: {profile}")
    ...
    raw_payload.setdefault("symbol", config.get("symbol"))
    raw_payload.setdefault("strategy_profile", profile)
    resolved = _normalize_runner_control_payload(_runner_preset_payload(profile, raw_payload))
    runtime_paths = _default_runtime_paths_for_symbol(str(resolved.get("symbol", "")))
    for key, value in runtime_paths.items():
        if not str(payload.get(key, "")).strip():
            resolved[key] = value
    return _autotune_runner_symbol_config(resolved)
```

- [ ] **Step 4: Run the focused preset tests**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -k "repo_managed_user_presets or save_running_runner_preset or save_running_preset_controls or uses_selected_captured_preset" -v`
Expected: PASS

- [ ] **Step 5: Capture the two live BARD configs through the real monitors**

Run:

```bash
export CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
export PWCLI="$CODEX_HOME/skills/playwright/scripts/playwright_cli.sh"
"$PWCLI" open http://43.155.136.111:8788/monitor --headed
"$PWCLI" snapshot
"$PWCLI" open http://43.131.232.150:8789/monitor --headed
"$PWCLI" snapshot
```

Expected: the monitor pages load, show the BARD runner state, and expose the running config or capture controls needed to save two named presets.

- [ ] **Step 6: Save the two captured presets into the repo-managed file**

```json
{
  "captured_bard_8788_2026_04_14": {
    "key": "captured_bard_8788_2026_04_14",
    "label": "BARD 8788 当前运行",
    "source": "captured_running_config",
    "symbol": "BARDUSDT",
    "config": {}
  },
  "captured_bard_8789_2026_04_14": {
    "key": "captured_bard_8789_2026_04_14",
    "label": "BARD 8789 当前运行",
    "source": "captured_running_config",
    "symbol": "BARDUSDT",
    "config": {}
  }
}
```

- [ ] **Step 7: Run the full verification**

Run: `PYTHONPATH=src python -m pytest tests/test_web_security.py -v`
Expected: PASS with the new preset storage, capture API, UI hooks, and start semantics covered.

- [ ] **Step 8: Commit**

```bash
git add config/runner_user_presets.json tests/test_web_security.py src/grid_optimizer/web.py
git commit -m "feat: capture running futures strategies as shared presets"
```
