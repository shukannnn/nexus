package queue

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

const QUEUENAME = "jobs"

func Open(redisAddr string) (*redis.Client, error) {
	client := redis.NewClient(
		&redis.Options{
			Addr: redisAddr,
		},
	)

	err := client.Ping(context.Background()).Err()
	if err != nil {
		return nil, fmt.Errorf("ping redis: %v", err)
	}

	return client, nil
}

func Enqueue(client *redis.Client, jobId string) error {
	err := client.RPush(context.Background(), QUEUENAME, jobId).Err()

	if err != nil {
		return fmt.Errorf("enqueue failed with error : %w", err)
	}

	return nil

}

func Dequeue(client *redis.Client) (string, error) {
	jobId, err := client.LPop(context.Background(), QUEUENAME).Result()

	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", fmt.Errorf("error while dequeue : %w", err)
	}

	return jobId, nil

}
