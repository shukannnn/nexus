package api

import (
	"encoding/json"
	"net/http"
	"nexus/internal/app"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
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
	r.Get("/dead-letter/:id/replay", h.replay)

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

	jobID, err := h.app.CreatePersistAndEnqueueJob(req.Type, req.Payload)
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

	job, err := h.app.GetJobByID(id)
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

	jobID, err := h.app.ReplayDeadLetterJob(id)

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
