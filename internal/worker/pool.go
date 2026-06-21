package worker

import (
	"context"
	"log/slog"
	"time"
)

type Pool struct {
	poolSize   int
	processJob func(ctx context.Context) (string, error)
}

func NewPool(poolSize int, processJob func(context.Context) (string, error)) *Pool {
	return &Pool{
		poolSize:   poolSize,
		processJob: processJob,
	}
}

func (pool *Pool) Start(ctx context.Context) {
	for i := 0; i < pool.poolSize; i++ {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					jobID, err := pool.processJob(ctx)
					if err != nil {
						slog.Error("job processed by worker with error", "jobId", jobID, "error", err)
					}
					if jobID == "" {
						time.Sleep(1 * time.Second)
					}
				}
			}
		}()
	}
}
