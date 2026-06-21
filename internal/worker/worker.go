package worker

import (
	"context"
	"nexus/internal/jobs"
	"time"
)

type Worker interface {
	Process(ctx context.Context, job *jobs.Job) error
	Timeout() time.Duration
}
