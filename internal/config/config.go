package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port              string
	RedisAddr         string
	DatabaseURL       string
	PoolSize          int
	GracePeriod       int
	VisibilityTimeout int
	ReapInterval      int
	SendGridAPIKey    string
	Email             string
}

func Load() (*Config, error) {
	port := os.Getenv("PORT")
	redisAddr := os.Getenv("REDIS_ADDR")
	databaseURL := os.Getenv("DATABASE_URL")

	if port == "" {
		port = "8080"
	}

	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	if databaseURL == "" {
		return nil, fmt.Errorf("database url is required")
	}

	size, err := strconv.Atoi(os.Getenv("POOL_SIZE"))
	if err != nil {
		return nil, fmt.Errorf("invalid pool size: %w", err)
	}

	gracePeriod, err := strconv.Atoi(os.Getenv("GRACE_PERIOD"))
	if err != nil {
		return nil, fmt.Errorf("invalid grace period: %w", err)
	}

	reapInterval, err := strconv.Atoi(os.Getenv("REAP_INTERVAL"))
	if err != nil {
		return nil, fmt.Errorf("invalid reap interval: %w", err)
	}

	visibilityTimeout, err := strconv.Atoi(os.Getenv("VISIBILITY_TIMEOUT"))
	if err != nil {
		return nil, fmt.Errorf("invalid visibility timeout: %w", err)
	}

	sendGridAPIKey := os.Getenv("SENDGRID_API_KEY")
	if sendGridAPIKey == "" {
		return nil, fmt.Errorf("sendgrid api key is required")
	}

	email := os.Getenv("email")

	c := Config{
		Port:              port,
		RedisAddr:         redisAddr,
		DatabaseURL:       databaseURL,
		PoolSize:          size,
		GracePeriod:       gracePeriod,
		VisibilityTimeout: visibilityTimeout,
		ReapInterval:      reapInterval,
		SendGridAPIKey:    sendGridAPIKey,
		Email:             email,
	}

	return &c, nil
}
