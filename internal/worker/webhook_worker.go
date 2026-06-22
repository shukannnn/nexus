package worker

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"nexus/internal/jobs"
	"nexus/internal/store"
	"time"
)

type WebHookWorker struct {
	db *sql.DB
}

type WebHookWorkerPayload struct {
	URL     string          `json:"url"`
	Payload json.RawMessage `json:"payload"`
	Secret  string          `json:"secret"`
}

var httpClient = &http.Client{}

func NewWebHookWorker(db *sql.DB) WebHookWorker {
    return WebHookWorker{db: db}
}

func (_ WebHookWorker) Timeout() time.Duration {
	return time.Second * 30
}

func (worker WebHookWorker) Process(ctx context.Context, job *jobs.Job) error {
	var payload WebHookWorkerPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("error while reading paylaod of webhookworker : %w", err)
	}

	if payload.URL == "" {
		return fmt.Errorf("empty url provided.")
	}

	if payload.Secret == "" {
		return fmt.Errorf("empty secret.")
	}

	//check if the request has already been made?
	exists, err := store.CheckWebhookDelivered(worker.db, job.ID)
	//if error is not nil we will assume the hook is not delivered based on the assumption the receiver is idempotent
	if err == nil && exists == true {
		//webhook already delivered
		slog.Info("webhook already delivered", "jobID", job.ID)
		return nil
	}

	slog.Info("processing webhookworker", "job_id", job.ID, "attempt number", job.Attempts)

	mac := hmac.New(sha256.New, []byte(payload.Secret))
	mac.Write([]byte(payload.Payload))
	signature := hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, payload.URL, bytes.NewReader(payload.Payload))
	if err != nil {
		return fmt.Errorf("error while making post request for webhookworker : %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Nexus-Signature", signature)
	req.Header.Set("X-Nexus-Delivery-ID", job.ID)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("webhook delivery failed with error: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Error("error closing webhook response body", "error", err)
		}
	}()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("webhook delivery failed with status: %d", resp.StatusCode)
	}
	slog.Info("webhook delivered", "url", payload.URL, "status", resp.StatusCode)
	
	//insert the webhook into db, if failed we will ignore it as the receiver is idempotent
	if err := store.InsertWebhookDelivery(worker.db, job.ID); err != nil {
		slog.Info("webhook inserting failed with error", "error", err, "jobID", job.ID)
	}

	return nil
}
