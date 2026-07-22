# ECS 150 SSH 访问故障分析与加固记录

## 1. 记录范围

- 日期：2026-07-22（Asia/Shanghai）
- 主机：ECS 150，SSH 别名 `srv-43-131-232-150`
- 关联主机：ECS 111，SSH 别名 `srv-43-155-136-111`
- 目标：在无法依赖云服务管理后台的情况下，恢复并加固 SSH 访问链路
- 状态：SSH 加固和 111 采集去重已经在生产主机生效并完成验证

本文只记录本次生产操作。仓库中的文档不能替代主机实时检查，后续仍应以 `sshd -T`、systemd、进程和日志为准。

## 2. 故障现象

历史故障并非始终表现为 22 端口不可达，而是存在以下分层状态：

1. `http://43.131.232.150:8789/api/health` 可以返回 200；
2. SSH TCP 连接和公钥认证可能成功；
3. 认证后无法创建命令或 shell 会话，客户端表现为超时或长时间无输出。

因此，Web 可用、22 端口可达和 SSH 会话可用必须分别验证，不能只依赖其中一项。

`POST /api/maintenance/recover_web` 只会触发 `grid-web-api2` Web 进程退出并由 systemd 拉起。它不会重启或修复 `sshd`、`systemd-logind`、PAM/MOTD 或主机资源压力，因此不能作为 SSH 恢复接口。

## 3. 现场证据

### 3.1 2026-07-22 当前状态

- 连续 8 次 22 端口 TCP 探测成功；
- 8789 health 连续返回 200，单次总耗时约 0.15 秒；
- 公钥认证、session channel 和远程命令均成功；
- 当前负载约 `0.00/0.05/0.08`；
- 内存 1.9 GiB，可用约 1.3 GiB；
- 根分区使用率约 55%；
- `ssh`、`systemd-logind`、`grid-web-api2` 均为 active。

### 3.2 历史压力窗口

2026-07-20 20:40 至 21:10 的 sysstat 和内核日志显示：

- 主机只有 2 个 CPU，`load1` 一度约为 34；
- 进程数上升到约 505；
- 大量进程停留在 `sshd -> run-parts -> update-motd/check-new-release/dumpe2fs`；
- SSH 已认证会话在 PAM/MOTD 初始化阶段堆积；
- `ubuntu` 用户 slice 达到 450 MiB 内存和 128 MiB swap 限制；
- 内核对该用户 slice 触发 cgroup OOM，杀死一个常驻内存约 382 MiB 的 SSH 会话内 `python3` 进程；
- 同期 `grid-web-api2` 单次内存峰值约 34 至 37 MiB，不是该次 OOM 的直接主体。

这解释了“密钥认证成功，但命令和 shell 无法建立”的现象：故障点主要位于认证后的会话初始化，而不是单纯的公网 22 端口或密钥校验。

### 3.3 111 定时采集放大连接堆积

111 的 `/home/ubuntu/binance_comp/run.sh` 每 30 分钟执行一次。修改前，它为了分别写 `cron.log` 和 `latest.txt`，重复调用了两遍：

- `monitor.py`
- `volume_tracker.py`

`volume_tracker.py` 会通过 SSH 分别读取 114 和 150 的 runner 事件，因此修改前每轮会连接 150 两次，即每小时 4 次、每周理论 672 次。150 的 SSH 日志在 7 天窗口内记录到来自 111 的 661 次成功认证，与该频率吻合。

正常情况下这些短连接可以及时关闭；当 PAM/MOTD 变慢时，新的定时连接会继续进入并积累，形成正反馈。

## 4. 已执行的生产变更

### 4.1 150 改为仅允许 ubuntu 公钥登录

新增：

```text
/etc/ssh/sshd_config.d/00-wangge-key-only.conf
```

内容：

```text
PermitRootLogin no
PasswordAuthentication no
KbdInteractiveAuthentication no
PubkeyAuthentication yes
AuthenticationMethods publickey
AllowUsers ubuntu
```

使用 `00-` 前缀是为了确保该文件在现有 `50-cloud-init.conf` 之前加载，避免后者的 `PasswordAuthentication yes` 先占用有效值。

变更过程保留了一条已认证管理会话；执行 `sshd -t` 成功后，只运行 `systemctl reload ssh`，没有重启主机或策略服务。

### 4.2 停止 SSH 登录时动态生成 MOTD

在 `/etc/pam.d/sshd` 中禁用了以下动态更新调用：

```text
session optional pam_motd.so motd=/run/motd.dynamic
```

保留了 `pam_motd.so noupdate`，因此可以显示已有静态内容，但 SSH 登录不再同步执行整套 `/etc/update-motd.d/` 脚本。这用于避免高负载时大量 `run-parts`、版本检查和文件系统检查进程阻塞 session 创建。

### 4.3 111 采集脚本去重

`/home/ubuntu/binance_comp` 不是 Git 工作树，因此对服务器现有脚本做了带备份的最小机械修改。

修改后的 `/home/ubuntu/binance_comp/run.sh` 只运行一次 `monitor.py` 和一次 `volume_tracker.py`，然后通过 `tee` 同时写入追加日志和最新快照：

```bash
{
  echo "########## $(date "+%Y-%m-%d %H:%M:%S") ##########"
  ./venv/bin/python monitor.py
  ./venv/bin/python volume_tracker.py
} 2>&1 | tee -a /home/ubuntu/binance_comp/cron.log \
  > /home/ubuntu/binance_comp/latest.txt
```

## 5. 验证结果

### 5.1 SSH 正向与反向验证

- 连续 5 次全新 `ubuntu + key` 会话成功，单次约 0.99 至 1.13 秒；
- root 登录失败，服务端只提供 publickey；
- ubuntu 密码/键盘交互登录失败，服务端只提供 publickey；
- 关闭保留管理会话后，再次建立全新 key 会话成功；
- `sshd -T` 有效值确认：

```text
permitrootlogin no
passwordauthentication no
kbdinteractiveauthentication no
allowusers ubuntu
authenticationmethods publickey
```

- 新会话完成后，`update-motd`、`run-parts`、`check-new-release`、`dumpe2fs` 残留进程数为 0；
- `ssh.service` 保持 active，`NRestarts=0`。

### 5.2 111 去重验证

- `bash -n /home/ubuntu/binance_comp/run.sh` 通过；
- 手工执行一次完整采集，退出码为 0；
- `latest.txt` 同时包含排行榜监控和 runner 内部累计数据；
- 150 sshd 日志确认该次执行只产生 1 次来自 111 的成功认证。

### 5.3 业务服务边界

- 本次只 reload 了 `ssh.service`；
- 没有重启主机；
- 没有调用策略启停、撤单、平仓或参数调整接口；
- `grid-web-api2` 保持 active；
- 本机 health 返回 `{"ok": true}` 和 HTTP 200。

## 6. 回滚位置

150 回滚目录：

```text
/var/backups/wangge-ssh-20260722T092405/
```

其中包含：

- `sshd_config.before`
- `pam_sshd.before`
- `pam_sshd.current`
- `00-wangge-key-only.conf.current`
- `ROLLBACK.txt`

文件权限均为 600。回滚会重新开放旧的 SSH 行为，只能在已保留的 root-capable 会话中作为应急操作执行，执行后必须再次运行 `sshd -t` 并新建独立 SSH 会话验证。

111 原脚本备份：

```text
/home/ubuntu/binance_comp/run.sh.bak.20260722T092603
```

## 7. 后续故障检查顺序

发生访问异常时按以下顺序检查，避免反复调用重型页面或无效恢复接口：

1. TCP：检查 22 端口是否能建立连接；
2. Web：只请求轻量的 `/api/health`，不要用 `/monitor` 代替健康检查；
3. SSH 分阶段：使用 `ssh -vv` 区分 TCP、密钥认证、channel open 和远程命令阶段；
4. 主机资源：检查 `uptime`、`free`、`ps`、`systemd-cgtop` 和 `sar`；
5. SSH/PAM：检查 `journalctl -u ssh`、`systemd-logind`、残留 session scope 和 MOTD 子进程；
6. 定时来源：检查 111 或其他主机是否在故障窗口持续创建新连接；
7. 不要把 `recover_web` 当成 SSH 修复接口。

## 8. 剩余风险与未执行事项

### 8.1 BCHUSDT runner 重启风暴

最终复核发现 `grid-loop@BCHUSDT.service` 存在独立的历史故障：

- systemd 重启计数已经达到 2304；
- 进程约每 50 秒达到 20 次连续错误后退出，再被 systemd 拉起；
- 当前错误为：

```text
_handle_terminal_drain_round() missing 1 required keyword-only argument: 'runtime_guard_config'
```

该循环在本次 SSH reload 前已经存在，本次没有停止或修改 BCHUSDT。持续重启会增加进程、日志和资源压力，可能再次影响 SSH 可用性，应作为独立代码修复和生产部署处理。

### 8.2 备用控制链路

本次没有部署反向 SSH 隧道。若需要完全摆脱公网 22 和云后台依赖，可后续设计由 150 主动连接 111 的专用反向隧道，并使用独立受限密钥和 localhost 绑定端口。

### 8.3 其他安全项

- UFW 和 fail2ban 在检查时均未启用；
- key-only 已阻断 root 和密码认证，但公网仍会产生 TCP/密钥扫描流量；
- 若本次排查期间使用的 Web Basic Auth 内容不是占位符，应单独轮换，不能写入仓库或日志。
