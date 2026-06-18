package config

import (
	"fmt"
	"os"
	"strconv"
)

type Config struct {
	Port        string
	RedisAddr   string
	DatabaseURL string
	PoolSize    int
	GracePeriod int
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

	gracePerion, err := strconv.Atoi(os.Getenv("GRACE_PERIOND"))
	if err != nil {
		return nil, fmt.Errorf("invalid grace period: %w", err)
	}

	c := Config{
		Port:        port,
		RedisAddr:   redisAddr,
		DatabaseURL: databaseURL,
		PoolSize:    size,
		GracePeriod: gracePerion,
	}

	return &c, nil
}
