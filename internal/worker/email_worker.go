package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/mail"
	"nexus/internal/jobs"
	"strings"
	"time"
)

type EmailWorker struct {
	apiKey string
	email string
}

type EmailWorkerPayload struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

const APIURL string = "https://api.sendgrid.com/v3/mail/send"

type sgEmail struct {
	Email string `json:"email"`
}

type sgPersonalization struct {
	To []sgEmail `json:"to"`
}

type sgContent struct {
	Type  string `json:"type"`
	Value string `json:"value"`
}

type sgRequestBody struct {
	Personalizations []sgPersonalization `json:"personalizations"`
	From             sgEmail             `json:"from"`
	Subject          string              `json:"subject"`
	Content          []sgContent         `json:"content"`
}

func NewEmailWorker(apiKey string, email string) *EmailWorker {
	return &EmailWorker{
		apiKey: apiKey,
		email: email,
	}
}

func (_ EmailWorker) Timeout() time.Duration {
	return time.Second * 10
}

func (worker EmailWorker) Process(ctx context.Context, job *jobs.Job) error {
	//we are not implementing idempotency here as it is not needed
	var payload EmailWorkerPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return fmt.Errorf("error while reading payload of email worker: %w", err)
	}

	payload.To = strings.TrimSpace(payload.To)
	payload.Body = strings.TrimSpace(payload.Body)
	payload.Subject = strings.TrimSpace(payload.Subject)

	if payload.To == "" {
		return fmt.Errorf("to is missing in email worker payload")
	}

	if payload.Body == "" && payload.Subject == "" {
		return fmt.Errorf("body and subject are missing in email worker payload")
	}

	validEmail, err := mail.ParseAddress(payload.To)

	if err != nil || validEmail.Address != payload.To {
		return fmt.Errorf("invalid input email for email worker")
	}

	slog.Info("processing emailworker", "job_id", job.ID, "attempt number", job.Attempts)

	//constructing sendgrid request body
	var sendGridRequest = sgRequestBody{
		Personalizations: []sgPersonalization{
			{
				To: []sgEmail{
					{
						Email: payload.To,
					},
				},
			},
		},
		From:    sgEmail{Email: worker.email},
		Subject: payload.Subject,
		Content: []sgContent{
			{
				Type:  "text/plain",
				Value: payload.Body,
			},
		},
	}

	sendGridRequestB, err := json.Marshal(sendGridRequest)
	if err != nil {
		return fmt.Errorf("error while creating request for email: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, APIURL, bytes.NewReader(sendGridRequestB))
	if err != nil {
		return fmt.Errorf("error while creating request with context for email worker :%w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+worker.apiKey)

	//using the same httpclient which was declared in webhook worker
	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("email delivery failed with error: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			slog.Error("error while closing email response body", "error", err)
		}
	}()

	if resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("email delivery failed with status %d: %s", resp.StatusCode, string(body))
	}

	slog.Info("email delivered", "To", payload.To, "status", resp.StatusCode)
	return nil
}
