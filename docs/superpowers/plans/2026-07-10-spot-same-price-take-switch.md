# Spot Same-Price TAKE Switch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a persisted, CLI-backed control that guarantees no same-price IOC TAKE orders when disabled.

**Architecture:** Keep the same-price exit implementation intact and gate only its invocation in `spot_loop_runner`. Thread one backward-compatible boolean through the parser and saved-runner web config/command builder.

**Tech Stack:** Python 3.12, argparse, unittest/pytest.

---

### Task 1: Specify disabled runtime behavior

**Files:**
- Modify: `tests/test_spot_loop_runner.py`
- Modify: `src/grid_optimizer/spot_loop_runner.py`

- [ ] Add a parser assertion for `--no-spot-same-price-take-exit-enabled` and a cycle test that supplies a qualifying maker BUY fill but expects no `post_spot_order` call, `enabled=false`, and `reason=disabled`.
- [ ] Run the two new tests and confirm they fail because the flag does not exist and the IOC is still submitted.
- [ ] Add a default-true argparse boolean pair and gate the existing same-price exit block with it.
- [ ] Re-run the new tests plus the existing enabled-path test and confirm they pass.

### Task 2: Persist and launch the control

**Files:**
- Modify: `tests/test_spot_runner.py`
- Modify: `src/grid_optimizer/web.py`

- [ ] Add a saved-runner command test asserting a false config emits `--no-spot-same-price-take-exit-enabled`.
- [ ] Run the new test and confirm it fails because the command omits the flag.
- [ ] Add the default config key, payload normalization, persisted output field, and explicit positive/negative command flag.
- [ ] Re-run focused runner tests and confirm the new behavior passes while the existing true path remains unchanged.

### Task 3: Verify and deploy

**Files:**
- Modify: production control JSON through the existing config loader/saver.

- [ ] Run focused parser, cycle, and command-builder tests with the retained local pytest command.
- [ ] Commit and push the minimal implementation to `main`, then fast-forward both production repos.
- [ ] Set `spot_same_price_take_exit_enabled=false` in SXT, OPN, and TREE controls on both accounts.
- [ ] Start through each host wrapper and verify three maker BUY plus three maker SELL orders, with no IOC submissions in cycle summaries or exchange order history.
