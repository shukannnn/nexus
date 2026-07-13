package jobs

import (
	"encoding/json"
	"time"

	"github.com/google/uuid"
)

type Job struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Status      string          `json:"status"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	LastError   string          `json:"last_error"`
}

type JobError struct {
	ID        string `json:"id"`
	JobID     string `json:"job_id"`
	Attempt   int    `json:"attempt"`
	Error     string `json:"error"`
	CreatedAt string `json:"created_at"`
}

type DeadLetterJob struct {
	ID          string          `json:"id"`
	JobID       string          `json:"job_id"`
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	LastError   string          `json:"last_error"`
	Attempts    int             `json:"attempts"`
	MaxAttempts int             `json:"max_attempts"`
	ReplayJobID string          `json:"replay_job_id"`
	CreatedAt   string          `json:"created_at"`
	UpdatedAt   string          `json:"updated_at"`
}

type CodeExecutionResult struct {
    ID        string `json:"id"`
    JobID     string `json:"job_id"`
    Status    string `json:"status"`
    Stdout    string `json:"stdout"`
    Stderr    string `json:"stderr"`
    TimeMs    int    `json:"time_ms"`
    MemoryKb  int    `json:"memory_kb"`
    ExitCode  int    `json:"exit_code"`
    Message   string `json:"message"`
    Verdict   string `json:"verdict"`
    CreatedAt time.Time `json:"created_at"`
}

type CodeExecutionResponse struct {
	JobStatus string                `json:"job_status"`
	Result    *CodeExecutionResult  `json:"result"`
}

const (
	StatusPending    = "pending"
	StatusProcessing = "processing"
	StatusCompleted  = "completed"
	StatusFailed     = "failed"
	StatusRetrying   = "retrying"
)

func New(jobType string, payload []byte) *Job {
	return &Job{
		ID:          uuid.NewString(),
		Type:        jobType,
		Payload:     payload,
		Status:      StatusPending,
		Attempts:    0,
		MaxAttempts: 3,
		LastError:   "",
	}
}
