# BQ Ledger And Deploy Isolation Design

Date: 2026-05-24
Branch: `codex/bq-ledger-isolation-design`

## Problem

PHAROS best-quote maker volume uses two concepts that currently share too many runtime signals:

- normal volume brushing inventory
- frozen inventory used for experiments and later cleanup

The exchange only exposes aggregate hedge positions per `positionSide`. Once frozen inventory grows, exchange-level position quantity, break-even price, unrealized PnL, and account PnL no longer describe the normal brushing strategy. If normal brushing keeps using those aggregate values as decision inputs, frozen inventory can block two-sided orders, distort inventory caps, trigger loss guards, and pollute daily volume accounting.

The desired behavior is stricter: every order and fill belongs to exactly one internal book. Frozen-book fills do not count toward normal brushing volume, and frozen inventory must not reduce normal brushing efficiency.

## Goals

- Make order ownership explicit at placement time.
- Route each fill into exactly one book: `normal_bq`, `frozen_bq`, or `unknown`.
- Keep the normal brushing planner driven only by the normal book.
- Keep frozen cleanup driven only by the frozen book.
- Exclude frozen-book fills from the Beijing 08:00 daily 40k normal brushing cap.
- Use exchange positions only as reconciliation evidence, not as normal brushing strategy input.
- Prevent experiment branches from accidentally replacing production code on 114 or 150.
- Keep server disk use bounded while still supporting isolated experiments.

## Non-Goals

- This design does not change exchange position mode. Binance hedge positions remain shared physical positions.
- This design does not try to make exchange unrealized PnL meaningful after frozen inventory exists.
- This design does not make every experiment deployable to production. Experiments stay in isolated runtimes until promoted.
- This design does not require a full database migration before the first implementation. State JSON can be migrated incrementally with compatibility code.

## Current Behavior

Current `main` already has the start of a split:

- `best_quote_volume_ledger` tracks normal BQ lots and realized/gross accounting.
- `best_quote_frozen_inventory` stores lots transferred from the normal ledger.
- `best_quote_volume_order_refs` records placed order metadata by exchange order id.
- `_transfer_best_quote_volume_to_frozen` moves FIFO lots from the normal ledger into frozen inventory.
- `_best_quote_reduce_freeze_report` can report managed inventory from the BQ volume ledger.
- Submit reconciliation can add frozen quantities back to expected exchange quantities.

The weak boundary is that several decisions still depend on aggregate exchange data or role inference:

- Fill ownership is inferred from role, side, position side, or client id shape.
- Frozen cleanup orders can look like normal BQ orders unless ownership is explicit.
- Runtime guard gross volume is still based on trade/audit scope unless explicitly filtered.
- Normal planner can still be influenced by frozen inventory through exchange-level PnL, total position, or cost-derived guards.
- Config changes can reset state if config signatures do not match, which is dangerous when the state carries ledger ownership.

## Ledger Model

The strategy owns three logical books:

### `normal_bq`

Normal brushing book.

Contains only fills from orders whose placement metadata declares `book = "normal_bq"`.

Used for:

- normal managed long/short quantity
- normal managed average price
- normal brushing gross notional
- normal brushing daily 40k cap
- normal inventory ratio
- normal order planning and reduce caps

Not used for:

- frozen cleanup accounting
- exchange aggregate PnL
- frozen inventory release progress

### `frozen_bq`

Frozen experiment and cleanup book.

Contains:

- lots transferred out of `normal_bq`
- fills from orders whose placement metadata declares `book = "frozen_bq"`

Used for:

- frozen long/short remaining quantity
- frozen cleanup progress
- manual frozen reduce and frozen pair release
- frozen-only diagnostics

Not used for:

- normal daily 40k cap
- normal brushing inventory ratio
- normal brushing loss budget
- normal planner entry or reduce decisions

### `unknown`

Compatibility and safety bucket.

Contains fills that cannot be mapped to a known order ref or a validated legacy rule.

Unknown fills must not be imported into `normal_bq` by default. They should be visible in diagnostics and require manual reconciliation or a one-time migration rule.

## Order Ownership

Every placed BQ order must write an order ref before it can be used for fill classification.

New fields in `best_quote_volume_order_refs`:

```json
{
  "order_id": "41974646",
  "book": "normal_bq",
  "role": "best_quote_entry_short",
  "side": "SELL",
  "position_side": "SHORT",
  "client_order_id": "gx-pharosu-bestquot-1-87716360",
  "strategy_mode": "hedge_best_quote_maker_volume_v1",
  "strategy_profile": "pharosusdt_hedge_best_quote_maker_volume_v1",
  "runner_id": "prod-150-pharos",
  "placed_at": "2026-05-24T01:55:16Z"
}
```

Allowed `book` values:

- `normal_bq`
- `frozen_bq`
- `unknown`

Default behavior:

- normal best-quote entry and normal best-quote reduce orders use `normal_bq`
- frozen manual reduce, frozen manual limit, and frozen pair release orders use `frozen_bq`
- legacy orders without a ref use `unknown` unless a migration rule explicitly classifies them

The `book` field is authoritative. Side, position side, role, and PnL are not allowed to override it.

## Fill Routing

Fill routing should follow this order:

1. Build a stable fill key from order id, side, position side, time, quantity, and price.
2. Look up `best_quote_volume_order_refs[order_id]`.
3. If the ref has `book = "normal_bq"`, apply the fill only to the normal book.
4. If the ref has `book = "frozen_bq"`, apply the fill only to the frozen book.
5. If the ref is missing or invalid, classify the fill as `unknown`.
6. Deduplicate by fill key inside each book.

Fill roles inside `normal_bq`:

- `best_quote_entry_long`: append long lot
- `best_quote_reduce_long`: consume normal long lot
- `best_quote_entry_short`: append short lot
- `best_quote_reduce_short`: consume normal short lot

Fill roles inside `frozen_bq`:

- frozen long reduce: consume frozen long lot
- frozen short reduce: consume frozen short lot
- frozen pair release: consume both frozen long and frozen short according to order refs
- frozen limit cleanup: consume only the explicitly isolated frozen side

Frozen fills must not change normal gross notional, normal realized PnL, normal lots, or normal daily cap usage.

## Normal Planner Inputs

The normal BQ planner should receive:

- normal long quantity from `normal_bq`
- normal short quantity from `normal_bq`
- normal average prices from `normal_bq`
- normal unrealized estimate from `normal_bq` and current mid, if needed for display
- normal gross notional from `normal_bq`
- open normal orders only

The normal BQ planner must not receive:

- frozen lots
- frozen notional
- exchange aggregate unrealized PnL
- exchange aggregate break-even price
- frozen cleanup open orders
- unknown fills

Inventory caps and soft ratios apply to normal inventory only. Exchange total position can still be used as a final sanity check after adding frozen and unknown quantities.

## Frozen Planner Inputs

Frozen cleanup uses only `frozen_bq` and frozen directives:

- frozen manual reduce directive
- frozen manual limit directive
- frozen pair release directive
- frozen long/short lots
- frozen cleanup open orders

Frozen cleanup orders should use a separate order ownership book (`frozen_bq`) and should be excluded from normal order caps, normal daily volume caps, and normal strategy loss guards.

## Runtime Guards And Volume Cap

The Beijing 08:00 daily 40k cap applies to `normal_bq` gross notional only.

Frozen-book fills:

- do not count toward the 40k cap
- do not stop normal brushing when frozen cleanup trades are active
- may have their own diagnostics, but no cap is required for this experiment

Runtime guard input loading should support a BQ isolated scope:

```text
normal_volume_gross = sum(fills where book == "normal_bq")
frozen_volume_gross = sum(fills where book == "frozen_bq")
unknown_volume_gross = sum(fills where book == "unknown")
daily_cap_input = normal_volume_gross
```

Loss-related guards for normal brushing should use normal-book values only or be disabled for the PHAROS brushing profile if the normal-book estimate is not reliable enough.

## Exchange Reconciliation

The exchange position is a physical sum:

```text
exchange_long_qty ~= normal_long_qty + frozen_long_qty + unknown_long_qty
exchange_short_qty ~= normal_short_qty + frozen_short_qty + unknown_short_qty
```

Reconciliation should report:

- normal quantity
- frozen quantity
- unknown/manual quantity
- exchange quantity
- difference

Reconciliation should block only unsafe actions. It should not feed exchange aggregate PnL or aggregate cost back into the normal planner.

Submit validation should use the relevant expected quantity for each action:

- normal reduce orders can reduce only normal available quantity plus existing same-book open reduce orders
- frozen reduce orders can reduce only frozen available quantity plus existing frozen-book open reduce orders
- entry orders are constrained by the book they belong to

## State Migration

Existing state keys should remain readable:

- `best_quote_volume_ledger`
- `best_quote_frozen_inventory`
- `best_quote_volume_order_refs`

Migration should add explicit ownership without deleting old fields:

- Existing `best_quote_volume_ledger` becomes `normal_bq`.
- Existing `best_quote_frozen_inventory` becomes `frozen_bq`.
- Existing order refs without `book` are backfilled conservatively:
  - refs used by normal BQ roles become `normal_bq`
  - refs tied to frozen manual or pair-release directives become `frozen_bq`
  - ambiguous refs become `unknown`

Migration must be idempotent and should write a `ledger_schema_version`.

State reset on config change must preserve ledger keys. A config signature change may reset strategy tuning state, but it must not erase:

- normal book lots
- frozen book lots
- fill dedupe keys
- order refs
- manual frozen directives

## Deploy Isolation Policy

Production directories on 114 and 150 must not be used for branch experiments.

Production rules:

- production runtime directory tracks only `main` or a future `release/prod`
- update wrappers reject any other current branch
- update wrappers reject dirty code files
- update wrappers use `git pull --ff-only`
- production runner status shows branch, commit, strategy profile, state path, and output path

Experiment rules:

- experiments run from server-side git worktrees, not full clones
- experiment worktrees live under a fixed experiment root
- each experiment has its own systemd unit name, output directory, state path, and client order prefix
- experiment runners never reuse production state paths
- experiment deployment requires an explicit experiment name
- experiment cleanup removes the worktree, stops the service, and archives or deletes output

Suggested server layout:

```text
/home/ubuntu/wangge                 production code
/home/ubuntu/wangge_api2            production runtime for 150
/home/ubuntu/wangge_worktrees/      experiment code worktrees
/home/ubuntu/wangge_venvs/          shared virtualenvs
/home/ubuntu/wangge_output_exp/     experiment output and state
```

## Disk Controls

Server disk is limited, so experiments must be bounded.

Controls:

- use git worktrees instead of full clones
- reuse shared virtualenvs when dependencies do not change
- allow at most one or two active experiment worktrees per server by default
- require `df` free-space check before experiment deploy
- reject experiment deploy if free space is below a configured threshold, initially 2 GB
- rotate experiment JSONL logs
- cap state backup retention by count and age
- report `du -sh` for code, venv, and output after deploy

Cleanup command should remove:

- experiment systemd unit
- experiment worktree
- experiment output older than the retention period
- unused experiment virtualenvs

## Merge Gate

No experiment branch can merge into `main` unless it satisfies:

- based on latest `origin/main`
- no production behavior change unless explicitly enabled by config
- strategy-specific code is isolated behind profile or mode checks
- normal/frozen ledger tests pass
- runtime guard volume-scope tests pass
- deploy wrapper or documentation explains production and experiment paths
- rollback steps are documented
- state migration is idempotent and backward compatible
- PR or merge note lists affected profiles, state keys, and deployment commands

## Test Plan

Unit tests:

- fill with `book = normal_bq` updates only normal ledger
- fill with `book = frozen_bq` updates only frozen ledger
- missing order ref becomes unknown and does not affect normal volume
- frozen manual reduce does not increment normal gross notional
- frozen pair release consumes only frozen lots
- normal daily cap ignores frozen fills
- normal planner ignores frozen quantity when computing inventory ratio
- exchange reconciliation reports normal + frozen + unknown vs exchange
- config signature reset preserves ledger keys

Integration-style tests:

- simulate normal entries, freeze transfer, frozen cleanup, then normal brushing remains two-sided
- simulate large frozen short and small normal inventory, normal planner still uses normal inventory only
- simulate production wrapper on non-production branch and verify rejection
- simulate experiment deploy with low disk and verify rejection

Production verification:

- status shows branch and commit
- status shows normal gross notional and frozen gross notional separately
- latest plan shows normal managed quantity independent of frozen quantity
- daily cap value matches normal-book fills only

## Rollout

1. Implement ledger ownership and fill routing behind compatibility helpers.
2. Add tests for normal/frozen/unknown fill classification.
3. Add runtime guard normal-book volume scope.
4. Add state migration preserving old keys.
5. Add submit validation awareness of book ownership.
6. Add deploy wrapper branch and disk checks.
7. Run on an experiment worktree with isolated state/output.
8. Promote to production only after normal two-sided brushing continues with frozen inventory present.

## Rollback

Rollback must be possible without losing ledger data:

- stop experiment runner
- leave production runner untouched
- restore previous production code via `git pull --ff-only` on production branch
- preserve state backups before migration
- if needed, disable new ledger ownership config and fall back to existing state keys

Production rollback should never require deleting frozen state. It should only switch which code path reads it.

