package jobs

import (
	"encoding/json"

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
