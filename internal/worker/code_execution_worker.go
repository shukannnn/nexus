package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"nexus/internal/jobs"
	"os"
	"os/exec"
	"time"
)

type CodeExecutionWorker struct {
	boxPool chan int
}

type CodeExecutionWorkerPayload struct {
	Language      string `json:"language"`
	SourceCode    string `json:"source_code"`
	Stdin         string `json:"stdin"`
	TimeLimitMs   int    `json:"time_limit_ms"`
	MemoryLimitKb int    `json:"memory_limit_kb"`
}

func NewCodeExecutionWorker(boxPool chan int) *CodeExecutionWorker {
	return &CodeExecutionWorker{
		boxPool: boxPool,
	}
}

func (_ CodeExecutionWorker) Timeout() time.Duration {
	return 60 * time.Second
}

func (worker CodeExecutionWorker) Process(ctx context.Context, job *jobs.Job) error {
	var payload CodeExecutionWorkerPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("error while reading payload of codeexecution worker: %w", err)
	}

	if payload.Language != "python3" && payload.Language != "cpp" {
		return fmt.Errorf("unsupported language provided to code execution worker: %s", payload.Language)
	}

	if payload.TimeLimitMs <= 0 || payload.TimeLimitMs > 10000 {
		return fmt.Errorf("invalid timelimit provided to codeexeuction worker: %d", payload.TimeLimitMs)
	}

	if payload.MemoryLimitKb <= 0 || payload.MemoryLimitKb > 262144 {
		return fmt.Errorf("invalid memory limit provided to code execution worker: %d", payload.MemoryLimitKb)
	}

	// getting a box id from the channel, it works as queue to get.
	boxID := <-worker.boxPool
	defer func() {
		worker.boxPool <- boxID
	} ()

	// running the isolate command to get a box along with context
	cmd := exec.CommandContext(ctx, "isolate", "--init", fmt.Sprintf("--box-id=%d", boxID))
	
	// defer function to clean up the box
	defer func() {
		cleanupCmd := exec.Command("isolate", "--cleanup", fmt.Sprintf("--box-id=%d", boxID))
		if err := cleanupCmd.Run(); err != nil {
			slog.Error("error while cleaning box for codeexecution worker", "error", err, "jobID", job.ID)
		}
	}()

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("error while running the isolate command in code execution worker: %w", err)
	}

	
	// copying the source code into isoltate
	solutionFileName := fmt.Sprintf("/var/local/lib/isolate/%d/box/", boxID)

	if payload.Language == "python3" {
		solutionFileName = solutionFileName + "solution.py"
	} 

	if err := os.WriteFile(solutionFileName, []byte(payload.SourceCode), 0644); err != nil {
		return fmt.Errorf("error while writing the file to isolate for code execution worker: %w", err)
	}

	// copying the stdin into isolate, if stdin is present
	if payload.Stdin != "" {
		stdinFileName := fmt.Sprintf("/var/local/lib/isolate/%d/box/stdin.txt", boxID)
		if err := os.WriteFile(stdinFileName, []byte(payload.Stdin), 0644); err != nil {
			return fmt.Errorf("error while writing stdin file to isolate for code execution worker: %w", err) 
		}
	}

	// running the isolate box to execute the actual code
	args := []string {
		"--run",
		fmt.Sprintf("--box-id=%d", boxID),
		fmt.Sprintf("--time=%.3f", float64(payload.TimeLimitMs)/1000.0),
		fmt.Sprintf("--mem=%d", payload.MemoryLimitKb),
		"--stdout=stdout.txt",
		"--stderr=stderr.txt",
		fmt.Sprintf("--meta=/tmp/meta-%d.txt", boxID),
	}

	// check if stdin is there, if yes then append it
	if payload.Stdin != "" {
		args = append(args, "--stdin=stdin.txt")
	}

	args = append(args, "--")

	if payload.Language == "python3" {
		args = append(args, "/usr/bin/python3", "solution.py")
	}

	executionCmd := exec.CommandContext(ctx, "isolate", args...)
	if err := executionCmd.Run(); err != nil {
		slog.Error("error while running the source code for code execution worker", "error", err, "jobID", job.ID)
	}

	// reading the meta, stderr and stdout output from the process
	stdoutFileName := fmt.Sprintf("/var/local/lib/isolate/%d/box/stdout.txt", boxID)
	stderrFileName := fmt.Sprintf("/var/local/lib/isolate/%d/box/stderr.txt", boxID)
	metaFileName := fmt.Sprintf("/tmp/meta-%d.txt", boxID)

	stdoutContent, err := os.ReadFile(stdoutFileName)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error while reading stdout file in code execution worker: %w", err)
	}

	stderrContent, err := os.ReadFile(stderrFileName)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error while reading stderr file in code execution worker: %w", err)
	}

	metaContent, err := os.ReadFile(metaFileName)
	if err != nil {
		return fmt.Errorf("erorr while reading meta file in code execution worker: %w", err)
	}

	slog.Info("code execution result", "jobID", job.ID, "stdout", string(stdoutContent), "stderr", string(stderrContent), "meta", string(metaContent))

	return nil
}
