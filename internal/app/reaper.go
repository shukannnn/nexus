package app

import (
	"context"
	"log/slog"
	"nexus/internal/queue"
	"nexus/internal/store"
	"nexus/internal/jobs"
	"time"
)


func (app *App) Reap(){
	//getting stale jobs
	result, err := queue.GetStaleJobs(app.redisClient)
	if err != nil {
		slog.Error("error while getting stale jobs in reap", "error", err)
		return
	}

	//mapping for jobs to score
	jobsToScore := make(map[string]float64)
	var jobIDs []string

	for _, job := range result {
		ID := job.Member.(string)
		score := job.Score

		jobsToScore[ID] = score
		jobIDs = append(jobIDs, ID)

	}


	jobRows, err := store.GetJobByIDs(app.dbClient, jobIDs)
	if err != nil {
		slog.Error("error while getting job rows in reap", "error", err)
		return
	}

	for _, job := range jobRows {
		switch job.Status{
			case jobs.StatusCompleted, jobs.StatusFailed:
				//remove from processing queue as no longer needed
			case jobs.StatusPending:
				//remove from processing queue and add it to job queue
			case jobs.StatusRetrying:
				//check if the current time is less then the stamp time
				//if yes then job is stale so move to processing queue (keeping the status retrying won't do any harm)
				//if no then keep it as it is
			case jobs.StatusProcessing:
				//check the currenttime > stamp time + visibility timeout
				//if yes then stale job, remove from processing and add to jobs (keeping the status processing won't do any harm)
				//if mo then keep it as it is
		}
	}

}

func (app *App) StartReaper(ctx context.Context) {
	//creating a ticker with time interval of reap interval
	ticker := time.NewTicker((time.Duration(app.reapInterval)) * time.Second)
	for {
		select {
		case <-ticker.C:
			// call sweep function
			app.Reap()
		case <-ctx.Done():
			//this means the server is shutdown
			ticker.Stop()
			return
		}
	}

}
