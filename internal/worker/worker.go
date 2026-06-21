package worker

import (
	"context"
	"nexus/internal/jobs"
)

type Worker interface {
	Process(ctx context.Context, job *jobs.Job) error
}
