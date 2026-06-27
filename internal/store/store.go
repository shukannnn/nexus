package store

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"nexus/internal/jobs"

	"github.com/lib/pq"
)

func Open(databaseURL string) (*sql.DB, error) {
	db, err := sql.Open("postgres", databaseURL)

	if err != nil {
		return nil, fmt.Errorf("failed to open db: %w", err)
	}

	err = db.Ping()

	if err != nil {
		return nil, fmt.Errorf("failed to ping db: %w", err)
	}

	return db, nil
}

func InsertJob(ctx context.Context, db *sql.DB, job *jobs.Job) error {
	query := `INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, last_error) 
				VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err := db.ExecContext(ctx, query, job.ID, job.Type, job.Payload, job.Status, job.Attempts, job.MaxAttempts, job.LastError)

	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

func GetJobByID(ctx context.Context, db *sql.DB, id string) (*jobs.Job, error) {
	query := `
	SELECT id, type, payload, status, attempts, max_attempts, last_error
	FROM jobs
	WHERE id = $1`

	var record jobs.Job
	var last_error sql.NullString

	err := db.QueryRowContext(ctx, query, id).Scan(&record.ID, &record.Type, &record.Payload, &record.Status, &record.Attempts, &record.MaxAttempts, &last_error)

	if err != nil {
		return nil, fmt.Errorf("error while fetching job by id: %w", err)
	}

	if last_error.Valid {
		record.LastError = last_error.String
	} else {
		record.LastError = ""
	}

	return &record, nil
}

func GetJobByIDs(ctx context.Context, db *sql.DB, ids []string) ([]*jobs.Job, error) {
	query := `
	SELECT id, type, payload, status, attempts, max_attempts, last_error from 
	jobs where id = ANY($1)`

	rows, err := db.QueryContext(ctx, query, pq.Array(ids))
	if err != nil {
		return nil, fmt.Errorf("error while fetching job by ids: %w", err)
	}
	defer rows.Close()

	var jobRows []*jobs.Job

	for rows.Next() {
		var record jobs.Job
		var last_error sql.NullString
		err := rows.Scan(&record.ID, &record.Type, &record.Payload, &record.Status, &record.Attempts, &record.MaxAttempts, &last_error)
		if err != nil {
			return nil, fmt.Errorf("error while fetching job by id in fetchjobsbyids: %w", err)
		}

		if last_error.Valid {
			record.LastError = last_error.String
		} else {
			record.LastError = ""
		}
		jobRows = append(jobRows, &record)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error while iterating rows: %w", err)
	}

	return jobRows, nil

}

func UpdateJobStatus(ctx context.Context, db *sql.DB, id string, status string) error {
	query := `UPDATE jobs SET status = $1 where id = $2`

	_, err := db.ExecContext(ctx, query, status, id)

	if err != nil {
		return fmt.Errorf("update status error: %w", err)
	}

	return nil
}

func MarkProcessingAndIncrementAttempts(ctx context.Context, db *sql.DB, id string) error {
	query := `UPDATE jobs 
			  SET status = $1,
			  attempts = attempts + 1
			  where id = $2`

	if _, err := db.ExecContext(ctx, query, jobs.StatusProcessing, id); err != nil {
		return fmt.Errorf("increase attempts error: %w", err)
	}

	return nil
}

func MarkRetryingOrFailedWithError(ctx context.Context, db *sql.DB, job *jobs.Job, status string, last_error string) error {

	if status == jobs.StatusFailed {
		slog.Error("job failed", "job_id", job.ID, "error", last_error)
	} else {
		slog.Warn("job retrying", "job_id", job.ID, "error", last_error)
	}

	//using transaction to update the status and error table
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("transaction begin error in mark retrying or failed: %w", err)
	}
	defer tx.Rollback()

	query := `UPDATE jobs set status = $1, last_error = $2 where id = $3`
	if _, err := tx.ExecContext(ctx, query, status, last_error, job.ID); err != nil {
		return fmt.Errorf("error updating job status: %w", err)
	}

	query = `INSERT INTO job_errors (job_id, attempt, error) VALUES ($1, $2, $3)`
	if _, err := tx.ExecContext(ctx, query, job.ID, job.Attempts, last_error); err != nil {
		return fmt.Errorf("error inserting job error: %w", err)
	}

	if status == jobs.StatusFailed {
		query := `INSERT INTO dead_letter_jobs (job_id, type, payload, last_error, attempts, max_attempts) VALUES ($1, $2, $3, $4, $5, $6)`
		if _, err := tx.ExecContext(ctx, query, job.ID, job.Type, job.Payload, last_error, job.Attempts, job.MaxAttempts); err != nil {
			return fmt.Errorf("error inserting dead letter job: %w", err)
		}
	}

	return tx.Commit()
}

func GetDeadLetterJobByID(ctx context.Context, db *sql.DB, deadLetterJobID string) (*jobs.DeadLetterJob, error) {
	query := `SELECT id, job_id, type, payload, last_error, attempts, max_attempts, replay_job_id, created_at, updated_at FROM dead_letter_jobs WHERE id = $1`

	var record jobs.DeadLetterJob
	var last_error sql.NullString
	var replay_job_id sql.NullString

	err := db.QueryRowContext(ctx, query, deadLetterJobID).Scan(&record.ID, &record.JobID, &record.Type, &record.Payload, &last_error, &record.Attempts, &record.MaxAttempts, &replay_job_id, &record.CreatedAt, &record.UpdatedAt)

	if err != nil {
		return nil, fmt.Errorf("error while fetching deadletterjob by id: %w", err)
	}

	if last_error.Valid {
		record.LastError = last_error.String
	} else {
		record.LastError = ""
	}

	if replay_job_id.Valid {
		record.ReplayJobID = replay_job_id.String
	} else {
		record.ReplayJobID = ""
	}

	return &record, nil
}

func ReplayDeadLetterJob(ctx context.Context, db *sql.DB, deadLetterJob *jobs.DeadLetterJob, job *jobs.Job) error {

	//begin transaction
	tx, err := db.BeginTx(ctx, nil)

	if err != nil {
		return fmt.Errorf("transaction begin error in replay dead letter job: %w", err)
	}
	defer tx.Rollback()

	//insert the new job into the db
	query := `INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, last_error) 
			VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err = tx.ExecContext(ctx, query, job.ID, job.Type, job.Payload, job.Status, job.Attempts, job.MaxAttempts, job.LastError)
	if err != nil {
		return fmt.Errorf("error while inserting a new job during replay dead letter job : %w", err)
	}

	//update the deadlettertable to the replay jobid
	query = `UPDATE dead_letter_jobs SET replay_job_id = $1 WHERE id = $2`
	_, err = tx.ExecContext(ctx, query, job.ID, deadLetterJob.ID)
	if err != nil {
		return fmt.Errorf("error while updating replay job id in replay dead letter job : %w", err)
	}

	return tx.Commit()
}

func ClaimWebhookDelivery(ctx context.Context, db *sql.DB, jobID string) (bool, error) {
	query := `INSERT into webhook_deliveries(job_id) values ($1) on CONFLICT (job_id) do nothing`

	count, err := db.ExecContext(ctx, query, jobID)
	if err != nil {
		return false, fmt.Errorf("error while claiming webhook : %w", err)
	}
	rowsAffected, err := count.RowsAffected()
	if err != nil {
		return false, fmt.Errorf("error while claiming webhook : %w", err)
	}
	return (rowsAffected > 0), nil
}

func IsWebhookDelivered(ctx context.Context, db *sql.DB, jobID string) (bool, error) {
	query := `SELECT EXISTS(SELECT 1 FROM webhook_deliveries WHERE job_id = $1 AND status = 'delivered')`

	var exists bool
	err := db.QueryRowContext(ctx, query, jobID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("error while checking webhook is delivered: %w", err)
	}
	return exists, nil
}

func ReleaseWebhookDelivery(ctx context.Context, db *sql.DB, jobID string) error {
	query := `UPDATE webhook_deliveries SET status = 'delivered' where job_id = $1`
	if _, err := db.ExecContext(ctx, query, jobID); err != nil {
		return fmt.Errorf("error while releasing webhook : %w", err)
	}
	return nil
}

func DeleteWebhookDelivery(ctx context.Context, db *sql.DB, jobID string) error {
	query := `DELETE from webhook_deliveries where job_id = $1`
	if _, err := db.ExecContext(ctx, query, jobID); err != nil {
		return fmt.Errorf("error while deleting webhook : %w", err)
	}
	return nil
}