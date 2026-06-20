package worker

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"nexus/internal/jobs"
)

type LogWorker struct {
}

type LogWorkerPayload struct {
	Message string `json:"message"`
}

func (_ LogWorker) Process(job *jobs.Job) error {

	var payload LogWorkerPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("error while reading paylaod of logworker : %w", err)
	}

	if payload.Message == "" {
		return fmt.Errorf("empty payload message.")
	}

	slog.Info("processing logworker", "job_id", job.ID, "attempt number", job.Attempts)
	fmt.Printf("output of logworker: %s\n", payload.Message)
	slog.Info("logworker job completed", "job_id", job.ID)

	return nil
}
