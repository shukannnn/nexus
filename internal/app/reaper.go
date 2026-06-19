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
	jobsToScore := make(map[string]int64)
	var jobIDs []string

	for _, job := range result {
		ID := job.Member.(string)
		score := job.Score

		jobsToScore[ID] = int64(score)
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
				slog.Info("removing from processing as completed/failed", "jobID", job.ID)
				queue.RemoveFromProcessing(app.redisClient, job.ID)
			case jobs.StatusPending:
				//remove from processing queue and add it to job 
				queue.RemoveFromProcessingAndInsertIntoJob(app.redisClient, job.ID)
				slog.Info("removing from processing and inserting into job as pending", "jobID", job.ID)
			case jobs.StatusRetrying:
				//check if the current time > stamp time
				//if yes then job is stale so move to processing queue (keeping the status retrying won't do any harm)
				//if no then keep it as it is
				if time.Now().Unix() > jobsToScore[job.ID]{
					queue.RemoveFromProcessingAndInsertIntoJob(app.redisClient, job.ID)
					slog.Info("removing from processing and inserting into job as retrying and stale", "jobID", job.ID)
				}
			case jobs.StatusProcessing:
				//check the currenttime > stamp time + visibility timeout
				//if yes then stale job, remove from processing and add to jobs (keeping the status processing won't do any harm)
				//if mo then keep it as it is
				if time.Now().Unix() > (jobsToScore[job.ID] + int64(app.visibilityTimeout)){
					queue.RemoveFromProcessingAndInsertIntoJob(app.redisClient, job.ID)
					slog.Info("removing from processing and inserting into job as processing and stale", "jobID", job.ID)
				}
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
			slog.Info("starting reap")
			app.Reap()
			slog.Info("reap completed")
		case <-ctx.Done():
			//this means the server is shutdown
			ticker.Stop()
			return
		}
	}

}
