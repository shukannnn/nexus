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
}

func Load() (*Config, error) {
	port := os.Getenv("PORT")
	redisAddr := os.Getenv("REDIS_ADDR")
	databaseURL := os.Getenv("DATABASE_URL")
	poolSize := os.Getenv("POOL_SIZE")

	if port == "" {
		port = "8080"
	}

	if redisAddr == "" {
		redisAddr = "localhost:6379"
	}

	if databaseURL == "" {
		return nil, fmt.Errorf("database url is required")
	}

	size, err := strconv.Atoi(poolSize)
	if err != nil {
		return nil, fmt.Errorf("invalid pool size: %w", err)
	}

	c := Config{
		Port:        port,
		RedisAddr:   redisAddr,
		DatabaseURL: databaseURL,
		PoolSize:    size,
	}

	return &c, nil
}
