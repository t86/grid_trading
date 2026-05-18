# Strategy Optimization Log

This log records strategy incidents, decisions, and follow-up items that should be reviewed before future parameter or logic changes.

## 2026-05-18 BILLUSDT Best-Quote Maker Volume

### Goal

- Keep the strategy autonomous during volume brushing.
- Avoid getting stuck at positions where it can neither add nor reduce safely.
- Avoid repeated high buy / low close behavior.
- Keep maker-volume capability, but prevent same-side brush orders from worsening inventory cost.

### Observed Issues

- `per_order_notional=264U` was too large for BILLUSDT. It reached inventory thresholds too quickly and reduced room for autonomous recovery.
- Best-quote order sizing was controlled by `best_quote_maker_volume_cycle_budget_notional`, not by `per_order_notional`, so changing only `per_order_notional` did not affect the live best-quote order size.
- The live runner showed `best_quote_maker_volume_cycle_budget_notional=260.0`, `quote_offset_ticks=20`, `defensive_offset_ticks=30`, and `per_order_notional=130.0`.
- Dynamic offset could tighten the quote distance below the configured offset in low volatility, making entries too close to market.
- Take-profit guard correctly moved reduce/exit prices to a profitable floor, but same-side entry orders stayed active near market. With long inventory this looked like continuous buys; with short inventory the symmetric risk is continuous sells.
- Existing adverse reduce behavior waited for higher pause thresholds, which is too late for "slight adverse move should stop bleeding" behavior in best-quote mode.

### Root Causes

- Best-quote mode still treated both sides as volume flow until soft inventory limits. That preserves volume but can keep accumulating the losing side before a reducer fills.
- The TP guard protects exits but does not constrain same-side entries.
- The previous parameter change missed the effective best-quote sizing parameter path.
- Recovery and adverse-reduce trigger levels were not aligned with best-quote soft inventory thresholds.

### Changes Made

- Added `best_quote_maker_volume.inventory_cost_gate` to the generated plan report.
- When holding long inventory, `best_quote_entry_long` orders are allowed only when their price is not above the current long average cost.
- When holding short inventory, `best_quote_entry_short` orders are allowed only when their price is not below the current short average cost.
- In best-quote mode, adverse reduce now uses the best-quote soft inventory threshold as the activation/target basis instead of waiting for the higher pause threshold.
- Added tests for:
  - Long inventory with buy below cost remains allowed.
  - Long inventory with buy above cost is blocked.
  - Short inventory with sell below cost is blocked.
  - Best-quote adverse reduce can activate using the soft threshold.

### Remaining Risks

- Existing live inventory may still need time to unwind after restart.
- If saved runner control overrides keep `adverse_reduce_enabled=false`, stop-bleed logic will not activate even though code supports it.
- Dynamic offset tightening can still make orders too close; follow-up should decide whether BILLUSDT needs a minimum effective offset floor.
- The current cost gate uses average cost only. It does not yet optimize per-lot exit sequencing or realized-PnL-aware rebuy spacing.

### Next Review Checklist

- Verify runner command and control JSON after deploy: cycle budget, per-order notional, adverse reduce enabled, adverse reduce trigger ratios, offset ticks.
- Confirm latest plan reports `inventory_cost_gate` and expected blocked order counts when inventory is present.
- Confirm adverse reduce activates around best-quote soft inventory, not only hard pause inventory.
- Track whether volume falls too much after cost gating; if so, adjust quote distance rather than allowing cost-worsening same-side entries.
