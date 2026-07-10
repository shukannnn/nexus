package app

import (
	"context"
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
	redisClient       *redis.Client
	dbClient          *sql.DB
	gracePeriod       int
	reapInterval      int
	visibilityTimeout int
	sendGridAPIKey    string
	boxPool chan int
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

	// creating a box pool which is required by the code execution worker
	boxPool := make(chan int, cfg.PoolSize)
	for i := 0; i < cfg.PoolSize; i++ {
		boxPool <- i
	}

	return &App{
		redisClient:       redisClient,
		dbClient:          dbClient,
		gracePeriod:       cfg.GracePeriod,
		visibilityTimeout: cfg.VisibilityTimeout,
		reapInterval:      cfg.ReapInterval,
		sendGridAPIKey:    cfg.SendGridAPIKey,
		boxPool: boxPool,
	}, nil

}

func (app *App) GetJobByID(ctx context.Context, id string) (*jobs.Job, error) {
	return store.GetJobByID(ctx, app.dbClient, id)
}

func (app *App) CreatePersistAndEnqueueJob(ctx context.Context, jobType string, payload json.RawMessage) (string, error) {
	// KNOWN GAP: if the process crashes between the Postgres commit and this enqueue call,
	// the job stays "pending" in Postgres but never reaches Redis, and nothing currently
	// recovers it. Proper fix would be a
	// transactional outbox pattern if this ever becomes a real problem.
	job := jobs.New(jobType, payload)
	if err := store.InsertJob(ctx, app.dbClient, job); err != nil {
		return "", fmt.Errorf("error inserting job into database: %w", err)
	}
	if errRedis := queue.Enqueue(app.redisClient, job.ID); errRedis != nil {
		if err := store.MarkRetryingOrFailedWithError(ctx, app.dbClient, job, jobs.StatusFailed, errRedis.Error()); err != nil {
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
		appWorker = worker.NewWebHookWorker(app.dbClient)

	case "email":
		appWorker = worker.NewEmailWorker(app.sendGridAPIKey)

	case "db_cleanup":
		appWorker = worker.DBCleanerWorker{}

	case "code_execution":
		appWorker = worker.NewCodeExecutionWorker(app.boxPool)

	default:
		return nil, fmt.Errorf("invalid job type: %s", jobType)

	}

	return appWorker, nil
}

func (app *App) ScheduleRetry(ctx context.Context, job *jobs.Job, cause error) {

	//calculating delay
	base := 2 * time.Second
	delay := base * time.Duration(math.Pow(2, float64(job.Attempts)))
	jitter := time.Duration(rand.Intn(job.Attempts*5)) * time.Second
	totalDelay := delay + jitter

	//re-stamp the jobID value in processing set to expiry time so that reaper can know whether it is being retried or not
	//expiry time = current_time + totalDelay + gracePeriod
	expiryTime := time.Now().Add(totalDelay + time.Duration(app.gracePeriod)*time.Second).Unix()
	queue.UpdateValueInProcessingQueue(app.redisClient, job.ID, expiryTime)

	//now we will mark this as retrying
	//we did not mark it before because reaper can get retrying status but it might have the old timestamp in processing set
	if err := store.MarkRetryingOrFailedWithError(ctx, app.dbClient, job, jobs.StatusRetrying, cause.Error()); err != nil {
		slog.Error("error while updating the job status to retrying", "error", err, "jobID", job.ID)
	}

	slog.Info("retrying job", "jobID", job.ID, "delay", totalDelay, "attempts", job.Attempts)

	//sleeping it for the delay
	time.Sleep(totalDelay)

	//moving the job from processing queue to job queue, so that the jobworker can pick it up
	//no error handling because if it fails, reaper will pick it up anyways
	queue.RemoveFromProcessingAndInsertIntoJob(app.redisClient, job.ID, "ScheduleRetry")
}

func (app *App) ProcessNextJob(ctx context.Context) (string, error) {

	jobID, err := queue.GetJobIDFromRedis(app.redisClient)
	if err != nil {
		return "", fmt.Errorf("error while getting jobid from redis: %w", err)
	}

	if jobID == "" {
		return "", nil
	}

	slog.Info("job dequeued", "job_id", jobID)

	job, err := app.GetJobByID(ctx, jobID)
	if err != nil {
		return "", fmt.Errorf("error while fetching job from database: %w", err)
	}

	if job.Attempts >= job.MaxAttempts {
		slog.Warn("job exceeded max attempts", "job_id", jobID, "attempts", job.Attempts)
		if err := store.MarkRetryingOrFailedWithError(ctx, app.dbClient, job, jobs.StatusFailed, "max attempts exceeded"); err != nil {
			return "", fmt.Errorf("error while updating status to failure for max attempts: %w", err)
		}
		// remove it from processing set when failure
		queue.RemoveFromProcessing(app.redisClient, jobID)
		return jobID, nil
	}

	if err := store.MarkProcessingAndIncrementAttempts(ctx, app.dbClient, jobID); err != nil {
		return "", fmt.Errorf("error while updating status of the job to processing: %w", err)
	}

	// increaseing the attempt here as db got incremented.
	job.Attempts += 1

	//getting the jobworker
	appWorker, err := app.getWorkerForType(job.Type)
	if err != nil {
		if err := store.MarkRetryingOrFailedWithError(ctx, app.dbClient, job, jobs.StatusFailed, err.Error()); err != nil {
			return "", fmt.Errorf("error while updating the job status to retrying: %w", err)
		}
		// remove it from processing set when failure
		queue.RemoveFromProcessing(app.redisClient, jobID)
		return jobID, nil
	}

	//creating a timeout-context
	jobCtx, cancelJob := context.WithTimeout(ctx, appWorker.Timeout())
	defer cancelJob()

	err = appWorker.Process(jobCtx, job)
	if err != nil {
		slog.Error("error while processing job", "job_id", jobID, "attempt", job.Attempts, "error", err.Error())
		go app.ScheduleRetry(ctx, job, err)
		return jobID, nil
	}

	if err := store.UpdateJobStatus(ctx, app.dbClient, jobID, jobs.StatusCompleted); err != nil {
		return "", fmt.Errorf("error while updating the status of the job to completed: %w", err)
	}

	//remove from processing from success
	queue.RemoveFromProcessing(app.redisClient, jobID)
	slog.Info("job completed", "job_id", jobID)

	return jobID, nil
}

func (app *App) ReplayDeadLetterJob(ctx context.Context, deadLetterJobID string) (string, error) {

	// KNOWN GAP: if the process crashes between the Postgres commit and this enqueue call,
	// the job stays "pending" in Postgres but never reaches Redis, and nothing currently
	// recovers it. Proper fix would be a
	// transactional outbox pattern if this ever becomes a real problem.

	slog.Info("replaying job", "ID", deadLetterJobID)

	//getting the dead letter job from db
	deadLetterJob, err := store.GetDeadLetterJobByID(ctx, app.dbClient, deadLetterJobID)
	if err != nil {
		return "", err
	}

	//creating a new job
	job := jobs.New(deadLetterJob.Type, deadLetterJob.Payload)

	//replay the job by creating a job in jobs table and updating the replay job id column in dead letter queue
	if err := store.ReplayDeadLetterJob(ctx, app.dbClient, deadLetterJob, job); err != nil {
		return "", err
	}

	//enqueue the job in jobs queue
	if err := queue.Enqueue(app.redisClient, job.ID); err != nil {
		return "", err
	}

	slog.Info("replayed job successfully", "ID", deadLetterJobID)

	return job.ID, nil
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
