# KAT 守护自动化

这份文档说明怎么把 `KAT` 巡检结果落到 `ecs-114` 的固定目录，并且让换电脑后的查看动作和自动化执行共用同一套路径。

## 固定输出目录

- 远端目录：`ecs-114:/home/ubuntu/wangge/output/kat_guard`
- 最新 JSON：`/home/ubuntu/wangge/output/kat_guard/latest.json`
- 最新 Markdown：`/home/ubuntu/wangge/output/kat_guard/latest.md`
- 历史快照：`/home/ubuntu/wangge/output/kat_guard/history.jsonl`

这几个文件都写在 `ecs-114` 上，所以换电脑后只要还能 `ssh ecs-114`，就能继续查看巡检结果。

## 手动执行

在仓库根目录执行：

```bash
python3 scripts/kat_guard_report.py
```

常用变体：

```bash
python3 scripts/kat_guard_report.py --print-markdown
python3 scripts/kat_guard_report.py --print-json
python3 scripts/kat_guard_report.py --skip-remote-write --print-markdown
```

脚本会同时巡检 `ecs-114` 和 `ecs-150` 的 `KATUSDT`，然后把汇总结果写回 `ecs-114`。

## 跨电脑查看

只看最新摘要：

```bash
ssh ecs-114 'sed -n "1,120p" /home/ubuntu/wangge/output/kat_guard/latest.md'
```

看结构化结果：

```bash
ssh ecs-114 'python3 -m json.tool /home/ubuntu/wangge/output/kat_guard/latest.json | sed -n "1,200p"'
```

看最近历史：

```bash
ssh ecs-114 'tail -n 20 /home/ubuntu/wangge/output/kat_guard/history.jsonl'
```

## 自动化建议

自动化本身仍然保存在创建它的那台本机上，但每次执行都应该先跑：

```bash
python3 scripts/kat_guard_report.py
```

然后再根据快照判断是否需要微调。当前允许的自动化边界是：

- 不提高杠杆
- 不关闭 `110000` 累计成交自动停止
- `114` 的 `max_position_notional` 不高于 `320U`
- `150` 的 `max_position_notional` 不高于 `220U`
- 只允许朝“少接仓、多卖出、提成交但控损”的方向调

## 典型使用流程

1. 在常驻的 Codex 机器上创建 `KAT 守护` 自动化。
2. 自动化每小时执行一次，先运行 `python3 scripts/kat_guard_report.py`。
3. 如果触发调参，调参完成后再次运行脚本覆盖 `latest.*`，并把新快照追加到 `history.jsonl`。
4. 换电脑后不要依赖本机自动化列表，直接登录 `ecs-114` 查看远端结果。
