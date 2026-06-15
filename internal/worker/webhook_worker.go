package worker

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"nexus/internal/jobs"
)

type WebHookWorker struct {
}

type WebHookWorkerPayload struct {
	URL     string          `json:"url"`
	Payload json.RawMessage `json:"payload"`
	Secret  string          `json:"secret"`
}

func (_ WebHookWorker) Process(job *jobs.Job) error {
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

	slog.Info("processing webhookworker", "job_id", job.ID, "attempts number", job.Attempts+1)

	mac := hmac.New(sha256.New, []byte(payload.Secret))
	mac.Write([]byte(payload.Payload))
	signature := hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequest(http.MethodPost, payload.URL, bytes.NewReader(payload.Payload))
	if err != nil {
		return fmt.Errorf("error while making post request for webhookworker : %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Nexus-Signature", signature)

	resp, err := http.DefaultClient.Do(req)
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
	return nil
}
