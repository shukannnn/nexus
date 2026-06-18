package store

import (
	"database/sql"
	"fmt"
	"log/slog"
	"nexus/internal/jobs"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
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

func InsertJob(db *sql.DB, job *jobs.Job) error {
	query := `INSERT INTO jobs (id, type, payload, status, attempts, max_attempts, last_error) 
				VALUES ($1, $2, $3, $4, $5, $6, $7)`

	_, err := db.Exec(query, job.ID, job.Type, job.Payload, job.Status, job.Attempts, job.MaxAttempts, job.LastError)

	if err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	return nil
}

func GetJobByID(db *sql.DB, id string) (*jobs.Job, error) {
	query := `
	SELECT id, type, payload, status, attempts, max_attempts, last_error
	FROM jobs
	WHERE id = $1`

	var record jobs.Job
	var last_error sql.NullString

	err := db.QueryRow(query, id).Scan(&record.ID, &record.Type, &record.Payload, &record.Status, &record.Attempts, &record.MaxAttempts, &last_error)

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

func GetJobByIDs(db *sql.DB, ids []string) ([]*jobs.Job, error) {
	query := `
	SELECT id, type, payload, status, attempts, max_attempts, last_error from 
	jobs where id = ANY($1)`

	rows, err := db.Query(query, pq.Array(ids))
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

func UpdateJobStatus(db *sql.DB, id string, status string) error {
	query := `UPDATE jobs SET status = $1 where id = $2`

	_, err := db.Exec(query, status, id)

	if err != nil {
		return fmt.Errorf("update status error: %w", err)
	}

	return nil
}

func MarkProcessingAndIncrementAttempts(db *sql.DB, id string) error {
	query := `UPDATE jobs 
			  SET status = $1,
			  attempts = attempts + 1
			  where id = $2`

	if _, err := db.Exec(query, jobs.StatusProcessing, id); err != nil {
		return fmt.Errorf("increase attempts error: %w", err)
	}

	return nil
}

func MarkRetryingOrFailedWithError(db *sql.DB, id string, attempt int, status string, last_error string) error {

	if status == jobs.StatusFailed {
		slog.Error("job failed", "job_id", id, "error", last_error)
	} else {
		slog.Warn("job retrying", "job_id", id, "error", last_error)
	}

	//using transaction to update the status and error table
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("transaction begin error in mark retrying or failed: %w", err)
	}
	defer tx.Rollback()

	query := `UPDATE jobs set status = $1, last_error = $2 where id = $3`
	if _, err := tx.Exec(query, status, last_error, id); err != nil {
		return fmt.Errorf("error updating job status: %w", err)
	}

	query = `INSERT INTO job_errors (job_id, attempt, error) VALUES ($1, $2, $3)`
	if _, err := tx.Exec(query, id, attempt, last_error); err != nil {
		return fmt.Errorf("error inserting job error: %w", err)
	}

	return tx.Commit()
}
