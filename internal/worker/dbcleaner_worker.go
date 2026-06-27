package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"nexus/internal/jobs"
	"time"
)

type DBCleanerWorker struct {
}

type DBCleanerWorkerPayload struct {
	Table            string `json:"tables"`
	OlderThanSeconds int      `json:"older_than_seconds"`
	ConnectionString string   `json:"connection_string"`
}

func (_ DBCleanerWorker) Timeout() time.Duration {
	return time.Second * 100
}

func OpenConnection(connectionString string) (*sql.DB, error) {
	//function to open a connection to the given connectionString
	db, err := sql.Open("postgres", connectionString)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (worker DBCleanerWorker) Process(ctx context.Context, job *jobs.Job) error {

	//not implementing idempotency as it is not needed
	var payload DBCleanerWorkerPayload

	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("error while reading payload of db cleaner worker: %w", err)
	}

	if payload.Table == "" {
		return fmt.Errorf("empty table list provided in db cleaner worker")
	}
	if payload.ConnectionString == "" {
		return fmt.Errorf("empty connection string provided in db cleaner worker")
	}
	if payload.OlderThanSeconds < 0 {
		return fmt.Errorf("negative older than seconds provided in db cleaner worker")
	}

	slog.Info("processing db cleaner worker", "job_id", job.ID, "attempt number", job.Attempts)
	//opening the connection to the db using the given connection_string
	db, err := OpenConnection(payload.ConnectionString)
	if err != nil {
		return fmt.Errorf("error while connecting to the db in db cleaner worker: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("DELETE FROM %s WHERE created_at < NOW() - INTERVAL '%d seconds'", payload.Table, payload.OlderThanSeconds)
	
	result, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("error while executing query in db cleaner worker, %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("error while getting rows affected: %w", err)
	}
	slog.Info("db cleanup completed", "table", payload.Table, "rows_deleted", rows)
	return nil
}