package worker

import (
	"nexus/internal/jobs"
)

type Worker interface {
	Process(job *jobs.Job) error
}
