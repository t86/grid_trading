# Alpha Competition Auto-Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Alpha dashboard discover newly announced Binance Alpha trading competitions automatically, show them immediately, and remove them only after their final UTC competition period ends.

**Architecture:** Preserve the currently running production diff as an auditable Git branch, then formalize it on local `main`. Add a focused discovery module that owns the five-minute CMS scan, article-code keyed last-known-good cache, single-flight refresh, and status filtering. Keep rule parsing and metric calculation in `alpha_competition_metrics.py`; make both dashboard APIs consume the same discovery snapshot so no environment-maintained symbol list is needed.

**Tech Stack:** Python 3.12, `requests`, dataclasses, `ThreadingHTTPServer`, JSON atomic files, pytest, Playwright/browser QA, systemd, Git pull-based deployment.

---

## File map

- Create `src/grid_optimizer/alpha_competition_discovery.py`: announcement model, durable discovery cache, five-minute single-flight refresh, last-known-good behavior, upcoming/active/between-rounds filtering.
- Modify `src/grid_optimizer/alpha_competition_metrics.py`: DOS-compatible identity parsing, announcement enumeration/detail loading, public rule serialization helpers, metrics from already-discovered rules, upcoming timing fields.
- Modify `src/grid_optimizer/alpha_competition_dashboard.py`: construct the discovery service, use its symbols for both APIs, expose discovery metadata, and render upcoming/stale states.
- Modify `deploy/oracle/install_alpha_competition_dashboard.sh`: provision and pass the dedicated discovery cache path.
- Modify `tests/fixtures/alpha_competition_articles.json`: add a minimal sanitized DOS fixture based on the official article shape.
- Modify `tests/test_alpha_competition_metrics.py`: parser/provider/metrics regression tests.
- Create `tests/test_alpha_competition_discovery.py`: cache, state transition, failure, restart, and concurrency contracts.
- Modify `tests/test_alpha_competition_dashboard.py`: shared dynamic-symbol API and page rendering contracts.
- Modify `tests/test_install_alpha_competition_dashboard.py`: installer environment/cache contract.

### Task 1: Preserve and formalize the current production baseline

**Files:**
- Preserve remotely: `/home/ubuntu/wangge-alpha-dashboard/src/grid_optimizer/alpha_competition_metrics.py`
- Preserve remotely: `/home/ubuntu/wangge-alpha-dashboard/src/grid_optimizer/alpha_competition_dashboard.py`
- Modify locally after audit: `src/grid_optimizer/alpha_competition_metrics.py`
- Modify locally after audit: `src/grid_optimizer/alpha_competition_dashboard.py`

- [ ] **Step 1: Record exact production state and diff without changing it**

Run:

```bash
ssh srv-43-156-35-110 'git -C /home/ubuntu/wangge-alpha-dashboard rev-parse HEAD; git -C /home/ubuntu/wangge-alpha-dashboard status --short; git -C /home/ubuntu/wangge-alpha-dashboard diff --check; git -C /home/ubuntu/wangge-alpha-dashboard diff --stat'
```

Expected: commit `e42d8c40265d4aaaa0d484549cff227927196cb3`, exactly the two known modified Python files, and clean `diff --check`.

- [ ] **Step 2: Preserve the uncommitted production diff on a recoverable branch**

Run only after Step 1 matches:

```bash
ssh srv-43-156-35-110 'cd /home/ubuntu/wangge-alpha-dashboard && git switch -c codex/prod-alpha-dashboard-drift-20260813 && git add src/grid_optimizer/alpha_competition_metrics.py src/grid_optimizer/alpha_competition_dashboard.py && git commit -m "chore: preserve production alpha dashboard drift" && git push -u origin codex/prod-alpha-dashboard-drift-20260813'
```

Expected: a pushed preservation commit; the running process is unchanged because this step does not restart it.

- [ ] **Step 3: Bring the preserved baseline to local `main` for audit**

Run:

```bash
git fetch origin codex/prod-alpha-dashboard-drift-20260813
git cherry-pick origin/codex/prod-alpha-dashboard-drift-20260813
git diff e42d8c40265d4aaaa0d484549cff227927196cb3..HEAD -- src/grid_optimizer/alpha_competition_metrics.py src/grid_optimizer/alpha_competition_dashboard.py
```

Expected: the existing leaderboard column/private-cookie fallback and incomplete auto-symbol attempt are fully visible in tracked history. Do not copy files from production.

- [ ] **Step 4: Run the existing dashboard contracts against the preserved baseline**

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_metrics.py tests/test_alpha_competition_dashboard.py
```

Expected: record any failures caused by the pre-existing production drift before changing it; unrelated strategy tests are out of this task.

### Task 2: Parse DOS and enumerate announcement identities correctly

**Files:**
- Modify: `tests/fixtures/alpha_competition_articles.json`
- Modify: `tests/test_alpha_competition_metrics.py`
- Modify: `src/grid_optimizer/alpha_competition_metrics.py`

- [ ] **Step 1: Add the real DOS identity regression fixture and failing parser test**

Add a `DOS` fixture whose title is `Trade DAPPOS (DOS)` and whose two period lines use `DAPPOS Trading Competition Promotion Period`. Then add:

```python
def test_dos_accepts_consistent_project_name_period_labels() -> None:
    rule = parse_competition_rule(FIXTURES["DOS"], "DOS")

    assert (rule.symbol, rule.name, rule.article_code) == (
        "DOS", "DAPPOS", "2e19d56645a2472fa3dbf1b8bf2c7efe",
    )
    assert [round_.number for round_ in rule.rounds] == [1, 2]


def test_period_labels_must_still_be_consistent_within_one_article() -> None:
    article = copy.deepcopy(FIXTURES["DOS"])
    _replace_period_label(article, round_number=2, label="OTHER")

    with pytest.raises(RuleParseError, match="labels conflict"):
        parse_competition_rule(article, "DOS")
```

- [ ] **Step 2: Run the DOS tests and verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_metrics.py -k 'dos_accepts or period_labels'
```

Expected: DOS fails with `promotion round symbol does not match article`; the conflict contract also fails because the new message/behavior does not exist.

- [ ] **Step 3: Make period labels consistent within the article instead of equal to the ticker**

Change `_parse_rounds` to track the first explicit label and reject only a different later label:

```python
def _parse_rounds(blocks: tuple[_ArticleBlock, ...]) -> tuple[CompetitionRound, ...]:
    period_label: str | None = None
    multiplier_blocks = _multiplier_blocks(blocks)
    first_day = next(
        (index for index, block in enumerate(blocks) if block in multiplier_blocks),
        len(blocks),
    )
    rounds_by_number: dict[int, CompetitionRound] = {}
    for block in blocks[:first_day]:
        for match in _PERIOD_RE.finditer(block.text):
            label = match.group("symbol").upper()
            if period_label is None:
                period_label = label
            elif label != period_label:
                raise RuleParseError("promotion round labels conflict")
            number = int(match.group("number"))
            if number <= 0 or match.group("suffix") != _ordinal_suffix(number):
                raise RuleParseError("promotion round number is invalid")
            round_ = CompetitionRound(
                number,
                _parse_datetime(match.group("start")),
                _parse_datetime(match.group("end")),
            )
            if round_.start_utc >= round_.end_utc or round_.end_utc - round_.start_utc != timedelta(days=7):
                raise RuleParseError("promotion round must last exactly seven days")
            existing = rounds_by_number.get(number)
            if existing is not None and existing != round_:
                raise RuleParseError("promotion round descriptions conflict")
            rounds_by_number[number] = round_
    if not rounds_by_number:
        raise RuleParseError("at least one full promotion round is required")
    return tuple(rounds_by_number[number] for number in sorted(rounds_by_number))
```

Call it from `parse_competition_rule` without `expected_symbol`. The title remains the authoritative symbol check through `_parse_title(title, symbol)`.

- [ ] **Step 4: Add announcement metadata enumeration contracts**

Add a public immutable record and provider tests:

```python
@dataclass(frozen=True)
class CompetitionAnnouncement:
    symbol: str
    article_code: str
    title: str
    released_at_utc: datetime


def test_provider_enumerates_recent_competition_announcements_by_article_code() -> None:
    provider = metrics.BinanceCompetitionRuleProvider(session=session_with_dos_power_and_noise())

    result = provider.fetch_recent_announcements(now=NOW)

    assert [(item.symbol, item.article_code) for item in result[:2]] == [
        ("DOS", "2e19d56645a2472fa3dbf1b8bf2c7efe"),
        ("POWER", "8bd4e92286d8474fa440091eea5672ff"),
    ]
```

Also assert duplicate article codes are rejected, non-competition titles are ignored, pages stop after the 60-day cutoff, and `fetch_announcement_rule()` verifies both returned code and title symbol.

- [ ] **Step 5: Run provider tests and verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_metrics.py -k 'recent_competition_announcements or announcement_rule'
```

Expected: FAIL because `CompetitionAnnouncement`, `fetch_recent_announcements`, and `fetch_announcement_rule` are absent or incomplete.

- [ ] **Step 6: Implement the minimal announcement API**

Use the strict title regex and preserve CMS order:

```python
_COMPETITION_TITLE_SYMBOL_RE = re.compile(
    r"^Binance Alpha Trading Competition:\s*Trade\s+.+?\(([A-Z0-9_]{1,32})\)\s+and\b",
    re.IGNORECASE,
)

def _competition_symbol_from_title(item: Mapping[str, Any]) -> str | None:
    title = item.get("title")
    if not isinstance(title, str):
        raise RuleParseError("Binance CMS article title is invalid")
    match = _COMPETITION_TITLE_SYMBOL_RE.match(title.strip())
    return match.group(1).upper() if match else None
```

`fetch_recent_announcements()` must validate code/release time for every item, deduplicate by code, ignore non-matching titles, and return only entries on or after the UTC cutoff. `fetch_announcement_rule()` must request the detail by `article_code`, parse with `announcement.symbol`, and reject a mismatched returned code.

- [ ] **Step 7: Verify parser/provider GREEN and commit**

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_metrics.py
git add tests/fixtures/alpha_competition_articles.json tests/test_alpha_competition_metrics.py src/grid_optimizer/alpha_competition_metrics.py
git commit -m "fix: parse and enumerate current alpha competitions"
```

Expected: all metrics tests pass.

### Task 3: Add durable last-known-good discovery

**Files:**
- Create: `src/grid_optimizer/alpha_competition_discovery.py`
- Create: `tests/test_alpha_competition_discovery.py`
- Modify: `src/grid_optimizer/alpha_competition_metrics.py`

- [ ] **Step 1: Write state-transition and article-code cache tests**

Create tests around this public API:

```python
@dataclass(frozen=True)
class DiscoveredCompetition:
    announcement: CompetitionAnnouncement
    rule: CompetitionRule
    validated_at_utc: datetime


@dataclass(frozen=True)
class DiscoverySnapshot:
    discovered_at_utc: datetime | None
    competitions: tuple[DiscoveredCompetition, ...]
    stale: bool
    errors: tuple[str, ...]

    @property
    def rules(self) -> tuple[CompetitionRule, ...]:
        return tuple(item.rule for item in self.competitions)


def test_discovery_shows_upcoming_and_active_but_removes_ended(tmp_path: Path) -> None:
    service = discovery_service(
        tmp_path,
        announcements=[dos_announcement, power_announcement, prl_announcement],
        rules={"DOS": upcoming_dos, "POWER": active_power, "PRL": ended_prl},
    )

    snapshot = service.discover(now=NOW)

    assert [rule.symbol for rule in snapshot.rules] == ["DOS", "POWER"]
    assert snapshot.stale is False
```

Add separate tests for exact start/end boundaries, a between-rounds rule remaining present, newest published rule winning when one symbol has two article codes, and JSON state keyed by article code.

- [ ] **Step 2: Run transition/cache tests and verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_discovery.py -k 'shows_upcoming or boundary or article_code or between_rounds'
```

Expected: collection error because the discovery module does not exist.

- [ ] **Step 3: Implement the cache with atomic replacement**

Expose existing rule serialization as `encode_competition_rule()` and `decode_competition_rule()` from metrics, then implement:

```python
class CompetitionDiscoveryCache:
    VERSION = 1

    def load(self) -> DiscoverySnapshot:
        return self._decode_payload(json.loads(self.path.read_text(encoding="utf-8")))

    def store(self, snapshot: DiscoverySnapshot) -> None:
        payload = self._encode_snapshot(snapshot)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temp_path: Path | None = None
        try:
            with tempfile.NamedTemporaryFile(
                "w", encoding="utf-8", dir=self.path.parent,
                prefix=f".{self.path.name}.", suffix=".tmp", delete=False,
            ) as handle:
                temp_path = Path(handle.name)
                json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temp_path, self.path)
            temp_path = None
        finally:
            if temp_path is not None:
                temp_path.unlink(missing_ok=True)
```

The stored object must contain `version`, `discovered_at_utc`, and `rules_by_article_code`; each article entry contains its announcement, encoded rule, and `validated_at_utc`. A corrupt entry is skipped without discarding healthy entries; an invalid root yields an empty snapshot.

- [ ] **Step 4: Add failure and restart tests**

```python
def test_list_failure_uses_last_known_good_and_marks_stale(tmp_path: Path) -> None:
    cache = seeded_cache(tmp_path, rules=[upcoming_dos, active_power], discovered_at=NOW)
    service = CompetitionDiscoveryService(provider=FailingProvider(), cache=cache)

    result = service.discover(now=NOW + timedelta(minutes=6))

    assert [rule.symbol for rule in result.rules] == ["DOS", "POWER"]
    assert result.stale is True
    assert result.errors == ("competition announcement discovery unavailable",)


def test_new_bad_article_is_visible_without_hiding_healthy_rules(tmp_path: Path) -> None:
    result = service_with_one_bad_new_article(tmp_path).discover(now=NOW)

    assert [rule.symbol for rule in result.rules] == ["POWER"]
    assert result.stale is True
    assert result.errors == ("DOS: competition announcement rule unavailable",)
```

Also test an old cached rule survives its detail refresh failure until its known final end, then is removed at the final half-open boundary even while the list request is failing.

- [ ] **Step 5: Implement refresh, selection, and last-known-good behavior**

Implement `CompetitionDiscoveryService(provider, cache, ttl=timedelta(minutes=5))` with:

```python
def discover(self, *, now: datetime) -> DiscoverySnapshot:
    current = require_utc(now)
    snapshot = self._memory or self.cache.load()
    if self._is_fresh(snapshot, current):
        return self._filter_ended(snapshot, current)
    if snapshot.rules and not self._refresh_lock.acquire(blocking=False):
        return replace(self._filter_ended(snapshot, current), stale=True)
    if not snapshot.rules:
        with self._refresh_lock:
            return self._refresh_or_reload(current)
    try:
        return self._refresh(snapshot, current)
    finally:
        self._refresh_lock.release()
```

Refresh by announcement code. Reuse a cached rule whose validation time is within the rule TTL; otherwise refetch its detail. Preserve cached non-ended rules absent from a truncated page. Deduplicate displayed rules by symbol using the greatest `published_at_utc`, preserve CMS order, and never return rules whose final `end_utc <= now`.

- [ ] **Step 6: Add and satisfy the single-flight concurrency test**

Start eight threads behind a barrier against a stale seeded snapshot. Block the first provider call and assert exactly one provider list call while the other seven receive the previous rule set marked stale. Release the provider and assert the next call receives the refreshed set.

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_discovery.py
```

Expected: all discovery tests pass with no leaked temp files or live threads.

- [ ] **Step 7: Commit the discovery boundary**

```bash
git add src/grid_optimizer/alpha_competition_discovery.py src/grid_optimizer/alpha_competition_metrics.py tests/test_alpha_competition_discovery.py
git commit -m "feat: cache automatic alpha competition discovery"
```

### Task 4: Feed both APIs from one dynamic snapshot

**Files:**
- Modify: `src/grid_optimizer/alpha_competition_metrics.py`
- Modify: `src/grid_optimizer/alpha_competition_dashboard.py`
- Modify: `tests/test_alpha_competition_metrics.py`
- Modify: `tests/test_alpha_competition_dashboard.py`

- [ ] **Step 1: Write failing metrics contracts for discovered rules**

Add `collect_rules()` contracts:

```python
def test_collect_rules_exposes_upcoming_start_without_volume_fetch() -> None:
    payload = service.collect_rules([upcoming_dos], now=BEFORE_DOS_START)

    row = payload["rows"][0]
    assert row["status"] == "upcoming"
    assert row["round"] == 1
    assert row["roundStartUtc"] == upcoming_dos.rounds[0].start_utc.isoformat()
    assert row["weightedVolume"] is None
    assert volume.calls == []
```

Add a between-rounds equivalent that points to the next round, and assert ended rules passed defensively to this method are omitted.

- [ ] **Step 2: Verify metrics RED, implement, and verify GREEN**

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_metrics.py -k 'collect_rules'
```

Expected RED: `collect_rules` is absent. Implement it by validating each supplied rule and reusing `_build_row`; for `upcoming`/`between_rounds`, populate the next round number/start/end without fetching volume. Re-run until GREEN.

- [ ] **Step 3: Replace configured-list API tests with shared discovery tests**

Use a fake discovery service whose snapshot is DOS then POWER, and assert:

```python
competition = http_server.get("/api/competition").json()
market = http_server.get("/api/snapshot").json()

assert [row["symbol"] for row in competition["rows"]] == ["DOS", "POWER"]
assert market["symbols"] == ["DOS", "POWER"]
assert competition["discoveredAtUtc"] == "2026-08-13T02:00:00+00:00"
assert competition["discoveryStale"] is False
```

Keep the explicit `/api/snapshot?symbols=...` validation tests as a diagnostic override; only the queryless endpoint must use discovery. Assert changing `ALPHA_SYMBOLS` does not affect either queryless API.

- [ ] **Step 4: Run the API tests and verify RED**

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_dashboard.py -k 'discovery or dynamic or configured_symbol_order'
```

Expected: old configured-symbol contract fails and the new metadata is absent.

- [ ] **Step 5: Wire one discovery snapshot into both handlers**

Add a lazily constructed `discovery_service()` using the same CMS provider and the cache path from `ALPHA_COMPETITION_DISCOVERY_CACHE`. Handler behavior:

```python
snapshot = discovery_service_.discover(now=datetime.now(timezone.utc))
symbols = [rule.symbol for rule in snapshot.rules]

# /api/competition
payload = competition_metrics.collect_rules(list(snapshot.rules), now=now)
payload.update({
    "discoveredAtUtc": iso_or_none(snapshot.discovered_at_utc),
    "discoveryStale": snapshot.stale,
    "errors": [*snapshot.errors, *payload["errors"]],
})

# queryless /api/snapshot
self._send_json(collect_snapshot(symbols, market=market))
```

Do not call `_symbols_from_env()` for either queryless endpoint. Leave `check_alert_once()` unchanged for compatibility with the alert subsystem.

- [ ] **Step 6: Verify API GREEN and commit**

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_metrics.py tests/test_alpha_competition_discovery.py tests/test_alpha_competition_dashboard.py
git add src/grid_optimizer/alpha_competition_metrics.py src/grid_optimizer/alpha_competition_dashboard.py tests/test_alpha_competition_metrics.py tests/test_alpha_competition_dashboard.py
git commit -m "feat: serve discovered alpha competition symbols"
```

### Task 5: Render immediate upcoming and stale discovery states

**Files:**
- Modify: `src/grid_optimizer/alpha_competition_dashboard.py`
- Modify: `tests/test_alpha_competition_dashboard.py`

- [ ] **Step 1: Add failing HTML contracts**

Assert the page contains a dedicated discovery status live region and logic equivalent to:

```javascript
if (row.status === 'upcoming') {
  return `未开始 · ${formatStartCountdown(row.roundStartUtc)}`;
}
if (row.status === 'between_rounds') {
  return `轮间等待 · ${formatStartCountdown(row.roundStartUtc)}`;
}
if (payload.discoveryStale) {
  discoveryStatusEl.textContent = '公告发现数据已过期，正在保留最近一次成功名单。';
}
```

Also assert all discovery error strings go through `escapeHtml`, no raw `innerHTML` receives an unescaped error, and the stale notice has `role="alert"`.

- [ ] **Step 2: Verify UI RED**

Run:

```bash
.venv/bin/pytest -q tests/test_alpha_competition_dashboard.py -k 'upcoming or discovery_stale or accessible_live'
```

Expected: missing upcoming countdown and discovery stale region.

- [ ] **Step 3: Implement the minimal responsive UI changes**

Add one compact banner above the competition table. Reuse the existing status chips and mobile card layout; do not add another table or page. `upcoming` and `between_rounds` rows show the official announcement and winner count but use `—` for volume/multiplier/threshold cells.

- [ ] **Step 4: Verify contracts and real browser layouts**

Run the dashboard locally with deterministic fixtures, then verify at 1440×900 and 390×844:

```text
document.documentElement.scrollWidth === document.documentElement.clientWidth
competition row/card order is DOS then POWER
DOS text contains 未开始 and a start countdown
stale fixture shows the last-known-good rows plus the stale banner
```

Use the in-app browser/Playwright, inspect console and `/api/competition` network response, then close created tabs and stop the local server.

- [ ] **Step 5: Commit the UI**

```bash
git add src/grid_optimizer/alpha_competition_dashboard.py tests/test_alpha_competition_dashboard.py
git commit -m "feat: show alpha competition discovery states"
```

### Task 6: Install, verify, push, and deploy

**Files:**
- Modify: `deploy/oracle/install_alpha_competition_dashboard.sh`
- Modify: `tests/test_install_alpha_competition_dashboard.py`

- [ ] **Step 1: Write the failing installer cache contract**

Assert the generated unit contains:

```ini
Environment=ALPHA_COMPETITION_DISCOVERY_CACHE=/home/ubuntu/.cache/binance-alpha-volume-alert/competition_discovery.json
```

and that the installer validates this file is directly under the existing dedicated cache directory, rejects symlink escapes, preserves rollback, and creates no new service.

- [ ] **Step 2: Verify installer RED, implement, and verify GREEN**

Run:

```bash
.venv/bin/pytest -q tests/test_install_alpha_competition_dashboard.py
bash -n deploy/oracle/install_alpha_competition_dashboard.sh
```

Implement one validated `DISCOVERY_CACHE` variable beside `RULE_CACHE`; reuse the existing secure cache directory and unit environment generation. Expected: installer tests and shell syntax pass.

- [ ] **Step 3: Run full scoped verification**

```bash
.venv/bin/pytest -q tests/test_alpha_market.py tests/test_alpha_volume_alert.py tests/test_alpha_competition_metrics.py tests/test_alpha_competition_discovery.py tests/test_alpha_competition_dashboard.py tests/test_install_alpha_competition_dashboard.py
.venv/bin/python -m compileall -q src/grid_optimizer/alpha_competition_discovery.py src/grid_optimizer/alpha_competition_metrics.py src/grid_optimizer/alpha_competition_dashboard.py
bash -n deploy/oracle/install_alpha_competition_dashboard.sh
git diff --check
git status --short --branch
```

Expected: zero scoped failures, compile/syntax/diff checks exit 0, and only intentional files are modified.

- [ ] **Step 4: Run broader regression and classify only unrelated failures**

```bash
.venv/bin/pytest -q
```

Expected: no new Alpha/dashboard/installer failure. If historical strategy failures remain, compare exact node IDs with the previously recorded unrelated set; do not change trading strategy code in this task.

- [ ] **Step 5: Commit, synchronize, and push `main`**

```bash
git add deploy/oracle/install_alpha_competition_dashboard.sh tests/test_install_alpha_competition_dashboard.py docs/superpowers/specs/2026-08-13-alpha-competition-auto-discovery-design.md docs/superpowers/plans/2026-08-13-alpha-competition-auto-discovery.md
git commit -m "ops: install alpha competition discovery cache"
git pull --ff-only origin main
git push origin main
git rev-list --left-right --count origin/main...main
```

Expected: `0 0` after push.

- [ ] **Step 6: Move production to the tracked commit using Git only**

The preservation branch from Task 1 makes the prior production state recoverable. On the server, switch back to `main`, fast-forward, install, and restart through the tracked installer:

```bash
ssh srv-43-156-35-110 'cd /home/ubuntu/wangge-alpha-dashboard && git switch main && git pull --ff-only origin main && ./deploy/oracle/install_alpha_competition_dashboard.sh'
```

Do not use `scp`, `rsync`, ad hoc `cp`, or paste code.

- [ ] **Step 7: Verify service, Git, cache, and exact live symptom**

Verify:

```bash
ssh srv-43-156-35-110 'git -C /home/ubuntu/wangge-alpha-dashboard status --short --branch; git -C /home/ubuntu/wangge-alpha-dashboard rev-parse HEAD; systemctl is-active binance-alpha-dashboard.service; systemctl show -p ExecMainStatus binance-alpha-dashboard.service; journalctl -u binance-alpha-dashboard.service --since "10 minutes ago" --no-pager'
```

Authenticated live API assertions must show:

```text
DOS is present immediately (upcoming or active according to current UTC time)
PRL and every other final-ended rule are absent
competition and queryless snapshot symbol lists are identical
discoveryStale is false after a successful Binance scan
errors does not contain DOS parser failure
```

Finally load `http://43.156.35.110/alpha/` at desktop and mobile widths, confirm no horizontal overflow and no browser console/network errors. Do not click `Check Alert`, because it can send real email.
