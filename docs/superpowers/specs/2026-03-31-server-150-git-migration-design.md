# 150 Server Git Migration Design

## Goal

把 `43.131.232.150` 上当前非 git 的部署目录 `/home/ubuntu/wangge` 迁移成基于仓库 `https://github.com/t86/grid_trading.git` `main` 分支的标准部署目录，并保留现有运行配置、端口与业务状态文件。

迁移完成后，150 需要满足两件事：

1. 代码目录本身是 git 仓库，可在机上执行标准更新。
2. 有一个固定的更新入口，可以自动拉取 `main`、更新依赖、重启服务并做健康检查。

## Current State

迁移前已确认：

- 服务器已安装 git：`/usr/bin/git`，版本 `2.43.0`
- 当前 systemd 服务：`grid-web.service`
- 当前工作目录：`/home/ubuntu/wangge`
- 当前 Python 启动命令：
  - `/home/ubuntu/wangge/.venv/bin/python -m grid_optimizer.web --host 0.0.0.0 --port 8788`
- 当前 service 依赖：
  - `EnvironmentFile=/etc/grid-web.env`
  - `GRID_SYMBOL_LISTS_PATH=/home/ubuntu/wangge/output/symbol_lists.json`
- 当前 `/home/ubuntu/wangge` 不是 git 仓库，没有 `.git/`

## Scope

这次只处理 `150` 机器，不改其他服务器。

本次迁移包含：

- 150 上建立仓库来源为 `t86/grid_trading` 的 git 工作目录
- 保留现有 `output/` 数据
- 保留 `/etc/grid-web.env`
- 保留现有 service 名称 `grid-web`
- 保留现有端口 `8788`
- 增加一个后续可重复执行的更新脚本
- 调整仓库中的 Oracle 安装/更新脚本，使其兼容 150 的现网约束

本次迁移不包含：

- 多服务器统一编排
- CI/CD 全量接管
- 自动添加 GitHub deploy key 到仓库设置

## Recommended Approach

采用“旁路 clone + 切换目录”的迁移方式，而不是在现有 `/home/ubuntu/wangge` 目录上原地初始化 git。

原因：

- 原目录来源不明，直接原地接管最容易把线上状态和仓库历史搅在一起。
- 旁路 clone 可以先验证仓库结构、依赖安装和 service 模板，再做最终切换。
- 一旦失败，回滚也只需要把旧目录切回去并重启 service。

## Design

### 1. Git Repository Migration

第一次迁移按下面的目录策略进行：

- 当前目录备份为：
  - `/home/ubuntu/wangge.backup.<timestamp>`
- 临时 clone 目录：
  - `/home/ubuntu/grid_trading.migrate.<timestamp>`
- 最终运行目录仍然保持：
  - `/home/ubuntu/wangge`

流程：

1. 停止 `grid-web.service`
2. 备份当前 `/home/ubuntu/wangge`
3. 用 deploy key 在临时目录 clone `origin/main`
4. 把临时 clone 的内容移动到新的 `/home/ubuntu/wangge`
5. 从备份中迁回必须保留的数据目录和运行文件

### 2. Preserved Runtime Assets

迁移时必须保留：

- `/etc/grid-web.env`
- `/home/ubuntu/wangge/output/`

其中 `output/` 需要整目录保留，而不是挑文件恢复。原因是其中同时包含：

- runner control JSON
- state JSON
- audit / summary jsonl
- symbol list
- 当前运行行为依赖的持久化状态

如果只恢复部分文件，runner 可能在重启后走到 `reset-state` 类似的重建路径。

### 3. Service Compatibility

仓库里的 `deploy/oracle/install_or_update.sh` 需要兼容 150 的现网 service 约束：

- 默认 `APP_DIR` 必须允许设置为 `/home/ubuntu/wangge`
- 默认 `GRID_WEB_PORT` 不能硬编码为 `8787`
- service 文件要带：
  - `EnvironmentFile=/etc/grid-web.env`
  - `Environment=GRID_SYMBOL_LISTS_PATH=/home/ubuntu/wangge/output/symbol_lists.json`

迁移后 `grid-web.service` 仍保持：

- service name：`grid-web`
- working directory：`/home/ubuntu/wangge`
- port：`8788`

### 4. Post-Migration Update Flow

150 上新增统一更新脚本，例如：

- `/usr/local/bin/grid-web-update`

逻辑固定为：

1. `cd /home/ubuntu/wangge`
2. `git fetch origin main`
3. `git reset --hard origin/main`
4. 更新 `.venv` 与 Python 依赖
5. `sudo systemctl restart grid-web`
6. 输出当前 commit、service 状态和 health check 结果

该脚本是后续所有手工更新的唯一入口，避免继续使用手工拷代码。

### 5. Health Verification

迁移完成必须验证：

- `git rev-parse --is-inside-work-tree` 返回 true
- `git rev-parse --abbrev-ref HEAD` 为 `main`
- `git rev-parse HEAD` 能输出当前 commit
- `systemctl status grid-web --no-pager`
- `curl http://127.0.0.1:8788/api/health`

如任一步失败，立即走回滚。

### 6. Rollback

回滚原则是“恢复旧目录 + 恢复旧 service + 启动旧服务”。

回滚步骤：

1. 停止 `grid-web.service`
2. 移除失败的新 `/home/ubuntu/wangge`
3. 把 `/home/ubuntu/wangge.backup.<timestamp>` 改回 `/home/ubuntu/wangge`
4. 重新加载 systemd 并重启 `grid-web.service`
5. 重新执行本地 health check

只要备份目录和 `/etc/grid-web.env` 没被破坏，回滚是直接的。

## Risks

### 1. Deploy Key 未配置完成

如果 150 没有可用的 GitHub 只读 deploy key，首次 clone 会失败。

应对方式：

- 先在 150 生成 ssh key
- 由人工把公钥加到 `t86/grid_trading` 的 Deploy keys
- 再开始正式迁移

### 2. output 恢复不完整

如果 `output/` 没有整目录带回，运行状态可能和迁移前不一致。

应对方式：

- 按目录整体迁回
- 切换前后对 `output/` 做目录级核对

### 3. 服务模板覆盖错端口或环境文件

如果仍然沿用当前仓库中默认 `8787` 且不写 `EnvironmentFile` 的模板，迁移后服务会和现网不一致。

应对方式：

- 先修仓库安装脚本
- 再执行服务器迁移

## Success Criteria

迁移完成后，满足以下条件才算成功：

- `/home/ubuntu/wangge` 是 git 仓库
- `origin` 指向 `https://github.com/t86/grid_trading.git` 或等价的 SSH 远端
- 当前分支为 `main`
- `grid-web.service` 正常运行
- Web 服务继续监听 `8788`
- `/etc/grid-web.env` 仍被 service 使用
- 原有 `output/` 数据仍然存在
- 后续可以在机上用统一更新脚本执行 git 更新
