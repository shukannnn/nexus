package app

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"nexus/internal/config"
	"nexus/internal/jobs"
	"nexus/internal/queue"
	"nexus/internal/store"
	"nexus/internal/worker"
	"time"

	"github.com/redis/go-redis/v9"
)

type App struct {
	redisClient *redis.Client
	dbClient    *sql.DB
}

func NewApp(cfg *config.Config) (*App, error) {

	dbClient, err := store.Open(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}

	redisClient, err := queue.Open(cfg.RedisAddr)
	if err != nil {
		errDB := dbClient.Close()
		if errDB != nil {
			return nil, fmt.Errorf("error connecting to redis and closing db: %w", errDB)
		}
		return nil, fmt.Errorf("error connecting to redis: %w", err)
	}

	return &App{
		redisClient: redisClient,
		dbClient:    dbClient,
	}, nil

}

func (app *App) GetJobByID(id string) (*jobs.Job, error) {
	return store.GetJobByID(app.dbClient, id)
}

func (app *App) CreatePersistAndEnqueueJob(jobType string, payload json.RawMessage) (string, error) {

	job := jobs.New(jobType, payload)
	if err := store.InsertJob(app.dbClient, job); err != nil {
		return "", fmt.Errorf("error inserting job into database: %w", err)
	}
	if errRedis := queue.Enqueue(app.redisClient, job.ID); errRedis != nil {
		if err := store.MarkRetryingOrFailedWithError(app.dbClient, job.ID, job.Attempts, jobs.StatusFailed, errRedis.Error()); err != nil {
			var errs []error
			errs = append(append(errs, err), errRedis)
			return "", errors.Join(errs...)
		}
		return "", fmt.Errorf("error inserting jobid into redis: %w", errRedis)
	}

	return job.ID, nil

}

func (app *App) getWorkerForType(jobType string) (worker.Worker, error) {
	var appWorker worker.Worker

	switch jobType {
	case "log":
		appWorker = worker.LogWorker{}

	case "webhook":
		appWorker = worker.WebHookWorker{}

	default:
		return nil, fmt.Errorf("invalid job type: %s", jobType)

	}

	return appWorker, nil
}

func (app *App) ScheduleRetry(jobID string, attempts int) {
	base := 2 * time.Second
	delay := base * time.Duration(math.Pow(2, float64(attempts)))
	jitter := time.Duration(rand.Intn(attempts*5)) * time.Second
	totalDelay := delay + jitter

	slog.Info("retrying job", "job_id", jobID, "attempt", attempts, "delay", totalDelay)

	time.Sleep(totalDelay)
	if errRedis := queue.Enqueue(app.redisClient, jobID); errRedis != nil {
		slog.Error("failed to re-enqueue job for retry", "job_id", jobID, "error", errRedis)
		if err := store.MarkRetryingOrFailedWithError(app.dbClient, jobID, attempts, jobs.StatusFailed, errRedis.Error()); err != nil {
			slog.Error("failed to mark job as failed after retry enqueue error", "job_id", jobID, "error", err)
		}
	}
}

func (app *App) ProcessNextJob() (string, error) {

	jobID, err := queue.GetJobIDFromRedis(app.redisClient)
	if err != nil {
		return "", fmt.Errorf("error while getting jobid from redis: %w", err)
	}

	if jobID == "" {
		return "", nil
	}

	slog.Info("job dequeued", "job_id", jobID)

	job, err := app.GetJobByID(jobID)
	if err != nil {
		return "", fmt.Errorf("error while fetching job from database: %w", err)
	}

	if job.Attempts >= job.MaxAttempts {
		slog.Warn("job exceeded max attempts", "job_id", jobID, "attempts", job.Attempts)
		if err := store.MarkRetryingOrFailedWithError(app.dbClient, jobID, job.Attempts+1, jobs.StatusFailed, "max attempts exceeded"); err != nil {
			return "", fmt.Errorf("error while updating status to failure for max attempts: %w", err)
		}
		// remove it from processing set when failure
		queue.RemoveFromProcessing(app.redisClient, jobID)
		return jobID, nil
	}

	if err := store.MarkProcessingAndIncrementAttempts(app.dbClient, jobID); err != nil {
		return "", fmt.Errorf("error while updating status of the job to processing: %w", err)
	}

	//getting the jobworker
	appWorker, err := app.getWorkerForType(job.Type)
	if err != nil {
		if err := store.MarkRetryingOrFailedWithError(app.dbClient, jobID, job.Attempts+1, jobs.StatusFailed, err.Error()); err != nil {
			return "", fmt.Errorf("error while updating the job status to retrying: %w", err)
		}
		// remove it from processing set when failure
		queue.RemoveFromProcessing(app.redisClient, jobID)
		return jobID, nil
	}

	err = appWorker.Process(job)
	if err != nil {
		if err := store.MarkRetryingOrFailedWithError(app.dbClient, jobID, job.Attempts+1, jobs.StatusRetrying, err.Error()); err != nil {
			return "", fmt.Errorf("error while updating the job status to retrying: %w", err)
		}
		slog.Error("error while processin job", "job_id", jobID, "attempt", job.Attempts + 1, "error", err.Error())
		go app.ScheduleRetry(jobID, job.Attempts+1)
		slog.Info("job scheduled for retry", "job_id", jobID, "attempt", job.Attempts+1)
		return jobID, nil
	}

	if err := store.UpdateJobStatus(app.dbClient, jobID, jobs.StatusCompleted); err != nil {
		return "", fmt.Errorf("error while updating the status of the job to completed: %w", err)
	}

	//remove from processing from success
	queue.RemoveFromProcessing(app.redisClient, jobID)
	slog.Info("job completed", "job_id", jobID)


	return jobID, nil
}

func (app *App) Close() error {
	var errs []error

	if err := app.dbClient.Close(); err != nil {
		errs = append(errs,
			fmt.Errorf("error closing database: %w", err))
	}

	if err := app.redisClient.Close(); err != nil {
		errs = append(errs,
			fmt.Errorf("error closing redis: %w", err))
	}

	return errors.Join(errs...)
}
