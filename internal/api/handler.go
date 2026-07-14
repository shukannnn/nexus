package api

import (
	"encoding/json"
	"net/http"
	"nexus/internal/app"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Handler struct {
	app *app.App
}

func NewHandler(application *app.App) *Handler {
	return &Handler{
		app: application,
	}
}

func (h *Handler) Routes() http.Handler {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/jobs/{id}", h.getJob)
	r.Post("/jobs", h.createJob)
	r.Post("/dead-letter/{id}/replay", h.replay)
	r.Post("/judge", h.judge)
	r.Get("/judge/{id}", h.getJudge)
	r.Handle("/metrics", promhttp.Handler())

	return r
}

func (h *Handler) createJob(w http.ResponseWriter, r *http.Request) {

	var req struct {
		Type    string          `json:"type"`
		Payload json.RawMessage `json:"payload"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	jobID, err := h.app.CreatePersistAndEnqueueJob(r.Context(), req.Type, req.Payload)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := struct {
		ID string `json:"id"`
	}{
		ID: jobID,
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if resErr := json.NewEncoder(w).Encode(&res); resErr != nil {
		http.Error(w, resErr.Error(), http.StatusInternalServerError)
		return
	}

}

func (h *Handler) getJob(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	job, err := h.app.GetJobByID(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if resErr := json.NewEncoder(w).Encode(&job); resErr != nil {
		http.Error(w, resErr.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) replay(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	jobID, err := h.app.ReplayDeadLetterJob(r.Context(), id)

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := struct {
		ID string `json:"id"`
	}{
		ID: jobID,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if resErr := json.NewEncoder(w).Encode(&res); resErr != nil {
		http.Error(w, resErr.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) judge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Language       string `json:"language"`
		SourceCode     string `json:"source_code"`
		Stdin          string `json:"stdin"`
		ExpectedOutput string `json:"expected_output"`
		TimeLimitMs    int    `json:"time_limit_ms"`
		MemoryLimitKb  int    `json:"memory_limit_kb"`
		Compare        bool   `json:"compare"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}

	if req.Language == "" {
        http.Error(w, "language is required", http.StatusBadRequest)
        return
    }
    if req.Language != "python3" && req.Language != "cpp" {
        http.Error(w, "unsupported language: must be python3 or cpp", http.StatusBadRequest)
        return
    }
    if req.SourceCode == "" {
        http.Error(w, "source_code is required", http.StatusBadRequest)
        return
    }
    if req.ExpectedOutput == "" {
        http.Error(w, "expected_output is required", http.StatusBadRequest)
        return
    }
    if req.TimeLimitMs <= 0 || req.TimeLimitMs > 10000 {
        http.Error(w, "time_limit_ms must be between 1 and 10000", http.StatusBadRequest)
        return
    }
    if req.MemoryLimitKb <= 0 || req.MemoryLimitKb > 262144 {
        http.Error(w, "memory_limit_kb must be between 1 and 262144", http.StatusBadRequest)
        return
    }
	
	req.Compare = true

	b, err := json.Marshal(req)
	if err != nil {
		http.Error(w, "failed to marshal payload", http.StatusInternalServerError)
		return
	}

	jobID, err := h.app.CreatePersistAndEnqueueJob(r.Context(), "code_execution", json.RawMessage(b))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	res := struct {
		ID string `json:"id"`
	}{
		ID: jobID,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)

	if err := json.NewEncoder(w).Encode(&res); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (h *Handler) getJudge(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")

	result, err := h.app.GetCodeExecutionResultByJobID(r.Context(), id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if resErr := json.NewEncoder(w).Encode(&result); resErr != nil {
		http.Error(w, resErr.Error(), http.StatusInternalServerError)
		return
	}
}