package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"runtime"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"mcp-exec/internal/executor"
)

var (
	flagMode = flag.String("mode", "stdio", "运行模式: stdio 或 http")
	flagPort = flag.String("port", "8080", "HTTP模式下的端口")
	flagHost = flag.String("host", "0.0.0.0", "HTTP模式下的主机地址")
)

func main() {
	mgr := executor.NewManager()

	s := server.NewMCPServer(
		"mcp-exec",
		"0.1.0",
		server.WithToolCapabilities(false),
		server.WithRecovery(),
	)

	// 工具：提交命令
	submitTool := mcp.NewTool("submit_command",
		mcp.WithDescription("提交一条命令，返回命令ID与初始状态"),
		mcp.WithString("cmd",
			mcp.Required(),
			mcp.Description("要执行的完整命令字符串"),
		),
		mcp.WithObject("env",
			mcp.Description("可选：环境变量映射"),
		),
		mcp.WithString("dir",
			mcp.Description("可选：工作目录"),
		),
		mcp.WithString("shell",
			mcp.Description("可选：shell类型，如 bash 或 powershell；Windows默认powershell，非Windows默认bash"),
		),
	)
	s.AddTool(submitTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		cmd, err := req.RequireString("cmd")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		// 读取 env
		envMap := map[string]string{}
		if all := req.GetArguments(); all != nil {
			if raw, ok := all["env"].(map[string]any); ok {
				buf, _ := json.Marshal(raw)
				_ = json.Unmarshal(buf, &envMap)
			}
		}
		dir := req.GetString("dir", "")
		shell := req.GetString("shell", "")
		if shell == "" {
			if runtime.GOOS == "windows" {
				shell = "powershell"
			} else {
				shell = "bash"
			}
		}

		id := mgr.Submit(executor.SubmitOptions{
			Command: cmd,
			Env:     envMap,
			Dir:     dir,
			Shell:   shell,
		})
		st := mgr.Status(id)
		result := map[string]any{"id": id, "status": st.Status}
		res, _ := mcp.NewToolResultJSON(result)
		return res, nil
	})

	// 工具：执行命令
	startTool := mcp.NewTool("start_command",
		mcp.WithDescription("启动已提交的命令"),
		mcp.WithString("id", mcp.Required(), mcp.Description("命令ID")),
	)
	s.AddTool(startTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := req.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		if err := mgr.Start(id); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		st := mgr.Status(id)
		res, _ := mcp.NewToolResultJSON(st)
		return res, nil
	})

	// 工具：查看命令状态（可选ID）
	statusTool := mcp.NewTool("get_status",
		mcp.WithDescription("查看命令状态；如不传ID返回全部命令状态"),
		mcp.WithString("id", mcp.Description("可选：命令ID")),
	)
	s.AddTool(statusTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id := req.GetString("id", "")
		if id != "" {
			st := mgr.Status(id)
			res, _ := mcp.NewToolResultJSON(st)
			return res, nil
		}
		list := mgr.StatusAll()
		res, _ := mcp.NewToolResultJSON(list)
		return res, nil
	})

	// 工具：获取输出
	outputTool := mcp.NewTool("get_output",
		mcp.WithDescription("获取命令输出；支持窗口大小与选择stdout/stderr"),
		mcp.WithString("id", mcp.Required(), mcp.Description("命令ID")),
		mcp.WithNumber("window", mcp.Description("可选：窗口大小(字节)；默认全部")),
		mcp.WithString("stream", mcp.Description("可选：stdout或stderr；默认stdout")),
	)
	s.AddTool(outputTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := req.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		window := int(req.GetFloat("window", 0))
		stream := req.GetString("stream", "stdout")
		data, err := mgr.Output(id, executor.OutputOptions{Window: window, Stream: stream})
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		return mcp.NewToolResultText(string(data)), nil
	})

	// 工具：干预命令（stdin 或 signal）
	interveneTool := mcp.NewTool("intervene_command",
		mcp.WithDescription("向运行中的命令写入stdin或发送信号(Windows仅支持stdin与强制结束)"),
		mcp.WithString("id", mcp.Required(), mcp.Description("命令ID")),
		mcp.WithString("input", mcp.Description("可选：写入到stdin的字符串")),
		mcp.WithString("signal", mcp.Description("可选：信号类型，如SIGINT/SIGTERM/SIGKILL(跨平台差异)")),
	)
	s.AddTool(interveneTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := req.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		input := req.GetString("input", "")
		signal := req.GetString("signal", "")
		if input != "" {
			if err := mgr.WriteStdin(id, []byte(input)); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
		}
		if signal != "" {
			if err := mgr.Signal(id, signal); err != nil {
				return mcp.NewToolResultError(err.Error()), nil
			}
		}
		return mcp.NewToolResultText("ok"), nil
	})

	// 工具：强制结束
	killTool := mcp.NewTool("kill_command",
		mcp.WithDescription("强制结束运行中的命令"),
		mcp.WithString("id", mcp.Required(), mcp.Description("命令ID")),
	)
	s.AddTool(killTool, func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id, err := req.RequireString("id")
		if err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		if err := mgr.Kill(id); err != nil {
			return mcp.NewToolResultError(err.Error()), nil
		}
		st := mgr.Status(id)
		res, _ := mcp.NewToolResultJSON(st)
		return res, nil
	})

	// 启动服务器
	flag.Parse()

	if *flagMode == "http" {
		runHTTPServer(s, *flagHost, *flagPort)
	} else {
		// 启动stdio服务器
		if err := server.ServeStdio(s); err != nil {
			fmt.Fprintf(os.Stderr, "Server error: %v\n", err)
		}
	}
}

func runHTTPServer(s *server.MCPServer, host, port string) {
	// 使用 Streamable HTTP 模式
	httpServer := server.NewStreamableHTTPServer(s,
		server.WithStateful(false), // 无状态模式，每个请求独立处理
	)

	addr := net.JoinHostPort(host, port)
	srv := &http.Server{
		Addr:    addr,
		Handler: httpServer,
	}

	fmt.Printf("MCP Bash HTTP Server 启动于 http://%s\n", addr)
	fmt.Printf("端点: /mcp (POST)\n")
	fmt.Printf("按 Ctrl+C 停止服务器\n")

	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Fprintf(os.Stderr, "HTTP Server error: %v\n", err)
		os.Exit(1)
	}
}
