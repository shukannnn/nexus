package main

import (
	"context"
	"log"
	"log/slog"
	"net/http"
	"nexus/internal/api"
	"nexus/internal/app"
	"nexus/internal/config"
	"nexus/internal/logger"
	"nexus/internal/worker"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

func main() {

	//reading from env
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatal("error loading .env file:", err)
	}

	//init logger
	slog.SetDefault(logger.Init())

	//loading config
	cfg, err := config.Load()
	if err != nil {
		slog.Error("error loading config", "error", err)
		os.Exit(1)
	}

	//creating an App
	application, err := app.NewApp(cfg)
	if err != nil {
		slog.Error("error starting application", "error", err)
		os.Exit(1)
	}
	//defer function to close the app
	defer func() {
		if err := application.Close(); err != nil {
			slog.Error("error closing app", "error", err)
			os.Exit(1)
		}
	}()

	slog.Info("loaded config and connected to redis and database")

	//rest handler and http server
	handler := api.NewHandler(application)
	srv := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: handler.Routes(),
	}

	//starting the pool
	workerCtx, stopWorkers := context.WithCancel(context.Background())
	pool := worker.NewPool(cfg.PoolSize, application.ProcessNextJob)
	pool.Start(workerCtx)

	//goroutine to make the server start in background
	go func() {
		slog.Info("server starting", "port", cfg.Port)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("error starting server", "error", err)
			os.Exit(1)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	slog.Info("shutting down server")

	//cancelling worker context so that workers stop
	stopWorkers()

	ctxTimeout, cancelTimeout := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelTimeout()

	if err := srv.Shutdown(ctxTimeout); err != nil {
		slog.Error("error stopping server", "error", err)
		os.Exit(1)
	}

	slog.Info("server stopped")
}
