# mcpbash

使用 [mcp-go](https://github.com/mark3labs/mcp-go) 构建的命令执行 MCP 服务器，支持 stdio 和 HTTP 两种运行模式。

## 功能

| 工具 | 说明 |
|------|------|
| `submit_command` | 提交命令，返回命令ID与初始状态 |
| `start_command` | 启动已提交的命令 |
| `get_status` | 查看命令状态（可选ID，不填返回全部） |
| `get_output` | 获取命令输出（支持窗口大小与 stdout/stderr） |
| `intervene_command` | 干预命令（stdin输入或信号） |
| `kill_command` | 强制结束运行中的命令 |

## 环境要求

- Go 1.22+
- Windows 默认使用 PowerShell 作为 shell；非 Windows 默认 bash。
- 如需在 Windows 上使用 bash，请安装 WSL 或 Git Bash，并在提交时设置 `shell: "bash"`。

## 快速开始

```bash
# 进入项目目录
cd mcpbash

# 整理依赖
go mod tidy

# 构建可执行文件
go build -o mcpbash.exe ./cmd/mcpbash
```

### 两种运行模式

#### 1. Stdio 模式（默认）

适用于 MCP 客户端通过标准输入输出连接本进程：

```bash
./mcpbash.exe
```

#### 2. HTTP 模式（远程调用）

适用于通过网络远程调用 MCP 服务：

```bash
# 默认端口 8080
./mcpbash.exe -mode http

# 指定端口
./mcpbash.exe -mode http -port 9000

# 绑定特定地址
./mcpbash.exe -mode http -host 127.0.0.1 -port 8080
```

HTTP 模式下，端点为 `POST /mcp`，符合 MCP over HTTP 规范。

## 工具参数说明

### submit_command

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `cmd` | string | 是 | 要执行的完整命令字符串 |
| `args` | string[] | 否 | 额外参数列表（shell模式通常不需要） |
| `env` | object | 否 | 环境变量映射 |
| `dir` | string | 否 | 工作目录 |
| `shell` | string | 否 | shell类型，如 `bash` 或 `powershell`；Windows默认 `powershell`，非Windows默认 `bash` |

**返回**：`{ id, status, pid }`

### start_command

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `id` | string | 是 | 命令ID |

**返回**：命令状态信息

### get_status

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `id` | string | 否 | 命令ID；不填返回全部命令状态 |

**返回**：状态信息数组或单个状态对象，包含字段：
- `id`: 命令ID
- `status`: 状态（pending/running/done/failed/killed）
- `pid`: 进程PID（运行时有效，结束后为0）
- `exitCode`: 退出码
- `error`: 错误信息
- `command`: 原命令
- `shell`: 使用的shell
- `createdAt`: 创建时间
- `startedAt`: 启动时间
- `endedAt`: 结束时间

### get_output

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `id` | string | 是 | 命令ID |
| `window` | number | 否 | 窗口大小（字节数）；默认返回全部 |
| `stream` | string | 否 | 输出流，`stdout` 或 `stderr`；默认 `stdout` |

### intervene_command

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| `id` | string | 是 | 命令ID |
| `input` | string | 否 | 写入到stdin的字符串 |
| `signal` | string | 否 | 信号类型，如 `SIGINT`/`SIGTERM`/`SIGKILL` |

### 参数 | 类型 | 必填 | kill_command

| 说明 |
|------|------|------|------|
| `id` | string | 是 | 命令ID |

## 注意事项

- Windows 的信号支持受限，当前实现对 `signal` 在 Windows 上统一为强制结束（Kill）。
- PID 字段在命令启动后有效，命令结束后会自动重置为 0。
- HTTP 模式采用无状态设计，每个请求独立处理。
