# Alpha 空投多源监控

部署在 111 的 `/alpha-airdrops`，统一入口 `/portal` 有链接。页面和 `/api/alpha-airdrops` 沿用 web 认证。

## 当前工作方式

- 钱包认证 Telegram 频道 `binance_wallet_announcements`：公开页面，不需要 Telegram 登录、API Key 或验证码。每轮完成后 30 秒再次检查，全天运行；延迟不是 SLA。
- 官网公开 CMS：活动栏目 93、最新消息栏目 49，每 5 分钟补充。只解析标题中有 Alpha + airdrop/空投的公告；不能代表广场或 App 全部消息。
- X 嵌入时间线：保留 `--enable-x` 可选备用，每 5 分钟检查；默认关闭，不再是关键依赖。
- 广场官方账号暂时仅提供原文入口，不启动常驻浏览器。官网 WebSocket 和 Telegram 登录后推送尚未接入。
- Bark 使用现有 `output/alpha_airdrop_monitor_bark.json`；邮件使用安装时明确指定的配置。绝不在页面/API 返回 Bark device key。

新预告、积分/数量/时间/降分条件更新、领取开始时间分别提醒。相同事件和相同结构化条件跨来源去重；未公布币种的消息只有在开始时间唯一匹配时合并。通知失败会重试，同渠道至少间隔 30 秒，超过 10 分钟不补发过期通知。首次扫描中的过时公告仅展示。消息编辑保留原发布时间，故“发布时间”和“首次发现”均展示；无法证明官方渠道间谁永远最早。

领取开始通知只表示公告时间已到，不表示账户符合资格、未领过或奖池仍有余额；需到官方 Alpha Events 确认。监控不自动交易、不签名、不领取。

## 安装与验证

遵守 pull-based 部署：提交并推送 main，服务器 `/usr/local/bin/grid-web-update` 拉取后再运行仓库内安装脚本。

```bash
cd /home/ubuntu/wangge
APP_DIR=/home/ubuntu/wangge SERVICE_USER=ubuntu \
ALERT_CONFIG_PATH=/home/ubuntu/wangge/output/alpha_airdrop_alert_notifier_config.json \
bash deploy/oracle/install_alpha_airdrop_monitor.sh
systemctl status grid-alpha-airdrop-monitor.timer
journalctl -u grid-alpha-airdrop-monitor.service -n 10 --no-pager
```

复用原 systemd 单元名称；不要额外启用另一份监控造成重复通知。服务限制 180M 内存、120 秒运行超时。运行数据为 `output/alpha_airdrop_feed.json`，原 X 状态不覆盖；命令行状态更新有跨进程锁，API 仅只读，JSON 原子替换。

无通知采集验证（使用临时路径以免覆盖正在运行的监控）：

```bash
.venv/bin/python -m grid_optimizer.alpha_airdrop_feed --no-notify --state-path /tmp/alpha-airdrop-probe.json
```

页面提供来源健康状态、失败退避时间和数据过期标记。来源故障保留已有事件，不显示成“没有空投”。发现 HTTP 429 时遵守 Retry-After/限流重置时间；其他失败指数退避。

## 如果升级为 Telegram 实时推送，你需要做什么

当前公开采集版不需要你处理任何 Telegram API 操作。如果公开页面不稳定，或需要减少轮询等待，再接入 MTProto：

1. 使用自己的 Telegram 账号订阅认证频道 `https://t.me/binance_wallet_announcements`。
2. 登录 `https://my.telegram.org` → **API development tools**，创建应用取得 `api_id`、`api_hash`（已有应用可复用）。这是 Telegram 客户端 API，不是 BotFather 的机器人 token。
3. 在后续提供的本地授权流程中，亲自输入手机号、验证码和二步验证密码，生成仅供监控使用的会话。当前版本没有实现这一步的授权脚本，不要提前把验证码或密码发到聊天里。
4. `api_hash` 和会话都是秘密，应写入安全配置、权限设为 600，不提交 Git、不写进网页；会话允许访问账号，应选专用监控账号并保护它。

机器人只能收到它已加入频道的帖子，不能用自己的 Bot API token 直接监听币安官方频道。不能把验证码、密码、会话字符串放进公开链接或 Bark 通知。

官方依据：

- https://core.telegram.org/api/obtaining_api_id
- https://core.telegram.org/bots/faq
- https://developers.binance.com/en/docs/products/announcements/general-info
- https://developers.binance.com/en/docs/products/announcements/announcement
