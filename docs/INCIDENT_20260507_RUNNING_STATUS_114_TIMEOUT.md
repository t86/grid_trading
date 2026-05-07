# 2026-05-07 114 Running Status Overview Timeout

## 背景

`http://43.131.232.150:8789/running_status_overview` 需要稳定展示 150 和 114 的合约/现货运行状态。2026-05-07 启动 NOTUSDT 现货刷量后，150 总览中多次出现 114 超时或 114 现货状态缺失。

## 现象

- 150 访问 114 单币种现货状态很快：
  - `GET http://43.155.163.114:8788/api/spot_runner/status?symbol=NOTUSDT`
  - 实测约 `0.4s` 返回。
- 114 本机 `GET /api/running_status_overview?scope=local` 较慢：
  - 实测约 `6-8s` 返回。
- 114 `grid-web` 进程 CPU 常在 `40%+`，本机还同时跑多个高频 runner。
- 114 日志中出现 `BrokenPipeError`，通常是远端请求超时断开后，114 仍在写响应。
- 因 150 的 `GRID_RUNNING_STATUS_REMOTE_TIMEOUT_SECONDS` 曾为 `4s/10s` 量级，114 整节点 overview 慢时会导致跨服总览缺 114。

## 判断

这不是 114 NOTUSDT 现货 runner 本身不可用。现货轻量接口可快速返回，问题集中在 114 整节点 `running_status_overview` 聚合成本过高：它需要汇总多个合约 runner、审计文件、状态文件和现货状态，在 114 web 进程 CPU/内存压力较高时容易超过跨节点超时。

## 已做修复

- 跨服 overview 继续请求远端 `/api/running_status_overview?scope=local`。
- 同时对本机已知现货控制文件中的 symbol，额外请求远端轻量接口：
  - `/api/spot_runner/status?symbol=<SYMBOL>`
- 如果远端整节点 overview 超时，仍用轻量现货状态补回远端现货行，确保 150 页面能看到 114/150 NOTUSDT 现货状态。
- 如果整节点 overview 成功但其中现货行较旧，轻量现货行会覆盖同一服务器/币种的现货行。

## 运维建议

- 不要单纯依赖加大跨服 timeout；这会拖慢 150 页面，并且不能根治 114 整节点聚合慢的问题。
- 114 长期应降低 `grid-web` 聚合负载：
  - 保持 competition board 禁用。
  - 保持 overview/cache/load-shed 环境变量启用。
  - 优先用轻量接口监控关键现货 runner。
- 若 114 再次频繁超时，优先检查：
  - `systemctl status grid-web.service`
  - `journalctl -u grid-web.service --since "30 min ago"`
  - `curl -m 12 /api/spot_runner/status?symbol=NOTUSDT`
  - `curl -m 12 /api/running_status_overview?scope=local`
  - `ps -eo pid,pcpu,pmem,rss,args --sort=-rss | head`

