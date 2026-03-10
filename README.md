# Slurm Watchdog

A lightweight Slurm job monitor with multi-platform notifications for HPC clusters.

**[中文文档](#中文文档)**

---

## Features

- **Real-time Job Monitoring**: Track job state changes (PENDING → RUNNING → COMPLETED/FAILED)
- **Multi-platform Notifications**: Support for 80+ platforms via Apprise (WeChat, DingTalk, Feishu, Telegram, Slack, Email, Webhooks, etc.)
- **Output File Analysis**: Analyze job output for convergence indicators and errors
- **Non-sudo Deployment**: Install and run as a regular user using systemd user services
- **Low Resource Usage**: SQLite database with WAL mode, adaptive polling intervals
- **Configurable Filtering**: Filter jobs by name, partition, or other attributes

## Installation

### From Source

```bash
# Clone the repository
git clone https://github.com/Odysseyer/slurm-watchdog.git
cd slurm-watchdog

# Install with pip
pip install -e .

# Or install with development dependencies
pip install -e ".[dev]"
```

### From PyPI (when published)

```bash
pip install slurm-watchdog
```

## Quick Start

```bash
# 1. Initialize configuration
slurm-watchdog config init

# 2. Edit configuration to add notification URLs
# Edit ~/.config/slurm-watchdog/config.toml

# 3. Test notification
slurm-watchdog test-notify "Hello from Slurm Watchdog"

# 4. Install as systemd service
slurm-watchdog install

# 5. Check status
slurm-watchdog status
```

## Configuration

Configuration is stored in `~/.config/slurm-watchdog/config.toml`.

### Basic Configuration

```toml
[watchdog]
poll_interval_running = 60    # Poll every 60s when jobs are active
poll_interval_idle = 300      # Poll every 5min when idle
disappeared_grace_seconds = 30  # Wait 30s before finalizing disappeared jobs
user = ""                     # Empty = current user
# job_name_filter = "^md-.*"  # Optional: filter by job name (regex)
# partition_filter = "gpu"    # Optional: filter by partition (regex)

[database]
path = "~/.local/share/slurm-watchdog/watchdog.db"

[notify]
# Apprise notification URLs
urls = [
    # Generic webhook
    "json://https://your-server.com/webhook",

    # Enterprise chat (examples)
    # "wecombot://corpid/secret/agentid",  # WeCom
    # "dingtalk://token/secret",            # DingTalk
    # "feishu://token/secret",              # Feishu

    # Messaging apps
    # "telegram://bottoken/chatid",
    # "slack://tokena/tokenb/tokenc",

    # Email (SMTP)
    # "mailtos://user:pass@host:port?to=email@example.com",
]

on_job_started = false
on_job_completed = true
on_job_failed = true
on_job_cancelled = true
on_job_timeout = true
on_job_boot_fail = true
on_job_out_of_memory = true
on_job_lost = true

[notify.retry]
max_retries = 3
backoff_factor = 2.0
```

### Output Analysis Configuration

```toml
[output_analysis]
enabled = true
tail_lines = 50

# Patterns indicating successful convergence
convergence_patterns = [
    "Convergence criteria met",
    "Normal termination",
    "SCF converged",
    "completed successfully",
]

# Patterns indicating errors
error_patterns = [
    "ERROR:",
    "FATAL:",
    "Segmentation fault",
    "MPI_ERR",
    "Killed",
]
```

## CLI Commands

```bash
# Service management
slurm-watchdog install      # Install systemd service
slurm-watchdog start        # Start service
slurm-watchdog stop         # Stop service
slurm-watchdog restart      # Restart service
slurm-watchdog status       # Show service status
slurm-watchdog logs [-f]    # View logs (follow mode)

# Configuration
slurm-watchdog config init     # Create default config
slurm-watchdog config show     # Show current config
slurm-watchdog config validate # Validate config
slurm-watchdog config template # Print config template

# Manual operations
slurm-watchdog scan            # Run a single scan
slurm-watchdog run             # Run daemon (foreground)
slurm-watchdog test-notify     # Send test notification
```

## Systemd Integration

Slurm Watchdog runs as a **systemd user service**, which means:

- **No sudo required**: Regular users can install and manage the service
- **Auto-restart**: Service automatically restarts on failure
- **Lingering**: Services continue running after logout (if linger is enabled)

### Enable Lingering (Recommended)

To allow the service to continue running after you log out:

```bash
loginctl enable-linger
```

This command typically doesn't require sudo on most HPC systems.

### Service Management

```bash
# Standard systemctl commands work with --user flag
systemctl --user status slurm-watchdog
systemctl --user start slurm-watchdog
systemctl --user stop slurm-watchdog
systemctl --user enable slurm-watchdog
journalctl --user -u slurm-watchdog -f
```

## Notification Examples

### Webhook

```toml
[notify]
urls = [
    "json://https://your-server.com/webhook",
]
```

The webhook receives a POST request with JSON body:

```json
{
  "title": "✅ Slurm Job Completed: my-job",
  "body": "Job: my-job (12345)\nState: COMPLETED\n...",
  "type": "info"
}
```

### WeChat Work (企业微信)

```toml
[notify]
urls = [
    "wecombot://corpid/secret/agentid",
]
```

### DingTalk (钉钉)

```toml
[notify]
urls = [
    "dingtalk://token/secret",
]
```

### Telegram

```toml
[notify]
urls = [
    "telegram://bottoken/chatid",
]
```

## Job States

| State | Description | Terminal |
|-------|-------------|----------|
| PENDING | Job is waiting for resources | No |
| RUNNING | Job is currently executing | No |
| SUSPENDED | Job is suspended | No |
| COMPLETED | Job finished successfully | Yes |
| FAILED | Job failed | Yes |
| CANCELLED | Job was cancelled | Yes |
| TIMEOUT | Job exceeded time limit | Yes |
| NODE_FAIL | Job failed due to node failure | Yes |
| PREEMPTED | Job was preempted | Yes |

## Architecture

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│   Slurm Queue   │────▶│   Watcher    │────▶│  Database   │
│  (squeue/sacct) │     │  (poll loop) │     │  (SQLite)   │
└─────────────────┘     └──────────────┘     └─────────────┘
                              │                     │
                              ▼                     │
                        ┌──────────────┐           │
                        │   Analyzer   │◀──────────┘
                        │ (output scan)│
                        └──────────────┘
                              │
                              ▼
                        ┌──────────────┐
                        │   Notifier   │
                        │  (Apprise)   │
                        └──────────────┘
                              │
                              ▼
                        ┌──────────────┐
                        │  Webhooks/   │
                        │  Chat Apps   │
                        └──────────────┘
```

## Development

### Running Tests

```bash
pip install -e ".[dev]"
pytest
```

### Code Quality

```bash
ruff check src/
ruff format src/
```

## Requirements

- Python 3.10+
- Access to Slurm commands (`squeue`, `sacct`)
- Linux with systemd (for service mode)

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- [Apprise](https://github.com/caronc/apprise) - Multi-platform notification library
- [Click](https://click.palletsprojects.com/) - CLI framework
- [Pydantic](https://pydantic-docs.helpmanual.io/) - Data validation

---

# 中文文档

一个轻量级的 Slurm 作业监控工具，支持多平台消息推送，专为 HPC 集群设计。

## 功能特性

- **实时作业监控**：追踪作业状态变化（PENDING → RUNNING → COMPLETED/FAILED）
- **多平台通知**：通过 Apprise 支持 80+ 平台（企业微信、钉钉、飞书、Telegram、Slack、邮件、Webhook 等）
- **输出文件分析**：分析作业输出文件，检测收敛标志和错误信息
- **无需 sudo**：普通用户使用 systemd user service 即可安装运行
- **低资源占用**：SQLite 数据库（WAL 模式），自适应轮询间隔
- **灵活过滤**：按作业名称、分区等属性过滤监控目标

## 安装

### 从源码安装

```bash
# 克隆仓库
git clone https://github.com/Odysseyer/slurm-watchdog.git
cd slurm-watchdog

# 使用 pip 安装
pip install -e .

# 或安装开发依赖
pip install -e ".[dev]"
```

### 从 PyPI 安装（发布后）

```bash
pip install slurm-watchdog
```

## 快速开始

```bash
# 1. 初始化配置
slurm-watchdog config init

# 2. 编辑配置文件，添加通知地址
# 编辑 ~/.config/slurm-watchdog/config.toml

# 3. 测试通知
slurm-watchdog test-notify "来自 Slurm Watchdog 的测试消息"

# 4. 安装为 systemd 服务
slurm-watchdog install

# 5. 查看状态
slurm-watchdog status
```

## 配置

配置文件位于 `~/.config/slurm-watchdog/config.toml`。

### 基础配置

```toml
[watchdog]
poll_interval_running = 60    # 有作业运行时每 60 秒轮询一次
poll_interval_idle = 300      # 空闲时每 5 分钟轮询一次
disappeared_grace_seconds = 30  # 作业从队列消失后等待 30 秒再判定
user = ""                     # 留空表示当前用户
# job_name_filter = "^md-.*"  # 可选：按作业名过滤（正则）
# partition_filter = "gpu"    # 可选：按分区过滤（正则）

[database]
path = "~/.local/share/slurm-watchdog/watchdog.db"

[notify]
# Apprise 通知 URL
urls = [
    # 通用 Webhook
    "json://https://your-server.com/webhook",

    # 企业通讯工具示例
    # "wecombot://corpid/secret/agentid",  # 企业微信
    # "dingtalk://token/secret",            # 钉钉
    # "feishu://token/secret",              # 飞书

    # 即时通讯
    # "telegram://bottoken/chatid",
    # "slack://tokena/tokenb/tokenc",

    # 邮件 (SMTP)
    # "mailtos://user:pass@host:port?to=email@example.com",
]

on_job_started = false
on_job_completed = true
on_job_failed = true
on_job_cancelled = true
on_job_timeout = true
on_job_boot_fail = true
on_job_out_of_memory = true
on_job_lost = true

[notify.retry]
max_retries = 3
backoff_factor = 2.0
```

### 输出分析配置

```toml
[output_analysis]
enabled = true
tail_lines = 50

# 收敛成功的关键词
convergence_patterns = [
    "Convergence criteria met",
    "Normal termination",
    "SCF converged",
    "completed successfully",
]

# 错误关键词
error_patterns = [
    "ERROR:",
    "FATAL:",
    "Segmentation fault",
    "MPI_ERR",
    "Killed",
]
```

## CLI 命令

```bash
# 服务管理
slurm-watchdog install      # 安装 systemd 服务
slurm-watchdog start        # 启动服务
slurm-watchdog stop         # 停止服务
slurm-watchdog restart      # 重启服务
slurm-watchdog status       # 查看服务状态
slurm-watchdog logs [-f]    # 查看日志（跟随模式）

# 配置管理
slurm-watchdog config init     # 创建默认配置
slurm-watchdog config show     # 显示当前配置
slurm-watchdog config validate # 验证配置
slurm-watchdog config template # 打印配置模板

# 手动操作
slurm-watchdog scan            # 执行一次扫描
slurm-watchdog run             # 前台运行守护进程
slurm-watchdog test-notify     # 发送测试通知
```

## Systemd 集成

Slurm Watchdog 以 **systemd 用户服务** 运行，这意味着：

- **无需 sudo**：普通用户可以自行安装和管理服务
- **自动重启**：服务失败时自动重启
- **Lingering**：启用后服务在用户登出后继续运行

### 启用 Lingering（推荐）

让服务在你登出后继续运行：

```bash
loginctl enable-linger
```

这个命令在大多数 HPC 系统上不需要 sudo。

### 服务管理

```bash
# 使用 --user 标志的 systemctl 命令
systemctl --user status slurm-watchdog
systemctl --user start slurm-watchdog
systemctl --user stop slurm-watchdog
systemctl --user enable slurm-watchdog
journalctl --user -u slurm-watchdog -f
```

## 通知示例

### Webhook

```toml
[notify]
urls = [
    "json://https://your-server.com/webhook",
]
```

Webhook 收到的 POST 请求体：

```json
{
  "title": "✅ Slurm Job Completed: my-job",
  "body": "Job: my-job (12345)\nState: COMPLETED\n...",
  "type": "info"
}
```

### 企业微信

```toml
[notify]
urls = [
    "wecombot://corpid/secret/agentid",
]
```

### 钉钉

```toml
[notify]
urls = [
    "dingtalk://token/secret",
]
```

### 飞书

```toml
[notify]
urls = [
    "feishu://token/secret",
]
```

### Telegram

```toml
[notify]
urls = [
    "telegram://bottoken/chatid",
]
```

## 作业状态

| 状态 | 描述 | 终态 |
|------|------|------|
| PENDING | 等待资源 | 否 |
| RUNNING | 正在运行 | 否 |
| SUSPENDED | 已挂起 | 否 |
| COMPLETED | 成功完成 | 是 |
| FAILED | 运行失败 | 是 |
| CANCELLED | 已取消 | 是 |
| TIMEOUT | 超时 | 是 |
| NODE_FAIL | 节点故障 | 是 |
| PREEMPTED | 被抢占 | 是 |

## 系统架构

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐
│   Slurm 队列    │────▶│   监控器     │────▶│   数据库    │
│  (squeue/sacct) │     │  (轮询循环)  │     │  (SQLite)   │
└─────────────────┘     └──────────────┘     └─────────────┘
                              │                     │
                              ▼                     │
                        ┌──────────────┐           │
                        │   分析器     │◀──────────┘
                        │ (输出扫描)   │
                        └──────────────┘
                              │
                              ▼
                        ┌──────────────┐
                        │   通知器     │
                        │  (Apprise)   │
                        └──────────────┘
                              │
                              ▼
                        ┌──────────────┐
                        │  Webhook /   │
                        │  聊天软件    │
                        └──────────────┘
```

## 开发

### 运行测试

```bash
pip install -e ".[dev]"
pytest
```

### 代码质量

```bash
ruff check src/
ruff format src/
```

## 系统要求

- Python 3.10+
- Slurm 命令（`squeue`、`sacct`）
- Linux 系统（需要 systemd 支持服务模式）

## 许可证

MIT 许可证 - 详见 [LICENSE](LICENSE)。

## 贡献

欢迎贡献代码！请随时提交 Pull Request。

## 致谢

- [Apprise](https://github.com/caronc/apprise) - 多平台通知库
- [Click](https://click.palletsprojects.com/) - CLI 框架
- [Pydantic](https://pydantic-docs.helpmanual.io/) - 数据验证
