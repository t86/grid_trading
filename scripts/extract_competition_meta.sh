#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <url>" >&2
  exit 1
fi

URL="$1"
CODEX_HOME="${CODEX_HOME:-$HOME/.codex}"
PWCLI="$CODEX_HOME/skills/playwright/scripts/playwright_cli.sh"
SESSION="comp-meta-$$"

read -r -d '' JS <<'EOF' || true
async () => {
  function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async function waitForStableLocation() {
    let previous = location.href;
    for (let i = 0; i < 40; i += 1) {
      await sleep(250);
      const current = location.href;
      if (current === previous) {
        await sleep(1500);
        return;
      }
      previous = current;
    }
  }

  function reactFiber(el) {
    return Object.values(el || {}).find(
      (value) => value && typeof value === "object" && "memoizedProps" in value && "return" in value
    ) || null;
  }

  await waitForStableLocation();

  function findLeaderboardState() {
    const section = document.getElementById("leaderboard-section") || document.body || document.documentElement;
    if (!section) {
      return null;
    }
    let best = null;
    for (const el of section.querySelectorAll("*")) {
      let fiber = reactFiber(el);
      while (fiber) {
        const rawType = fiber.elementType || fiber.type;
        const name = typeof rawType === "string"
          ? rawType
          : (rawType && (rawType.displayName || rawType.name)) || String(rawType || "");
        const props = fiber.memoizedProps || {};
        const columns = Array.isArray(props.columns) ? props.columns : [];
        const list = Array.isArray(props.list) ? props.list : [];
        const resourceIds = [...new Set(list.map((item) => item && item.resourceId).filter(Boolean))];
        const score = resourceIds.length * 1000 + list.length * 10 + columns.length;
        if (score > 0) {
          const candidate = {
            componentName: name,
            rankingType: props.rankingType || "",
            competitionType: props.competitionType || "",
            rewardUnit: props.rewardUnit || "",
            leaderboardUnit: props.leaderboardUnit || "",
            leaderboardUnitTitle: props.leaderboardUnitTitle || "",
            columns,
            list,
          };
          if (!best || score > best.score) {
            best = { score, state: candidate };
          }
        }
        fiber = fiber.return;
      }
    }
    return best ? best.state : null;
  }

  function currentTabLabel() {
    return Array.from(document.querySelectorAll(".bn-tab.active"))
      .map((el) => (el.textContent || "").trim())
      .find((text) => text) || "";
  }

  async function clickTab(label) {
    const button = Array.from(document.querySelectorAll(".bn-tab"))
      .find((el) => (el.textContent || "").trim() === String(label || "").trim());
    if (!button) {
      return false;
    }
    button.click();
    for (let i = 0; i < 80; i += 1) {
      if (currentTabLabel() === label) {
        await sleep(3500);
        return true;
      }
      await sleep(150);
    }
    return false;
  }

  async function collectBoard(label) {
    for (let i = 0; i < 120; i += 1) {
      const state = findLeaderboardState();
      if (state && state.list && state.list.length) {
        const metricColumn = state.columns.length ? state.columns[state.columns.length - 1] : null;
        return {
          tabLabel: label || currentTabLabel() || "默认",
          resourceIds: [...new Set(state.list.map((item) => item && item.resourceId).filter(Boolean))],
          metricField: metricColumn && metricColumn.dataIndex ? metricColumn.dataIndex : "",
          metricLabel: metricColumn && metricColumn.title ? metricColumn.title : "",
          rewardUnit: state.rewardUnit,
          leaderboardUnit: state.leaderboardUnit,
          leaderboardUnitTitle: state.leaderboardUnitTitle,
          rankingType: state.rankingType,
          competitionType: state.competitionType,
          bodyExcerpt: (document.body && document.body.innerText || "").slice(0, 5000),
        };
      }
      await sleep(250);
    }
    return null;
  }

  const labels = Array.from(document.querySelectorAll(".bn-tab"))
    .map((el) => (el.textContent || "").trim())
    .filter((text, index, arr) => text && arr.indexOf(text) === index);
  const targets = labels.filter((text) => !/活动主页/.test(text));
  const boards = [];

  if (!targets.length) {
    const board = await collectBoard(currentTabLabel() || "默认");
    if (board) {
      boards.push(board);
    }
  } else {
    for (const label of targets) {
      await clickTab(label);
      const board = await collectBoard(label);
      if (board) {
        boards.push(board);
      }
    }
  }

  return {
    url: location.href,
    title: document.title || "",
    labels,
    boards,
  };
}
EOF

"$PWCLI" --session "$SESSION" open "$URL" >/dev/null
sleep 10
cleanup() {
  "$PWCLI" --session "$SESSION" close >/dev/null 2>&1 || true
}
trap cleanup EXIT
"$PWCLI" --session "$SESSION" eval "$JS"
