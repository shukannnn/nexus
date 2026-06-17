package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

const QUEUE_NAME = "jobs"
const PROCESSING_QUEUE = "processing"

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
	err := client.RPush(context.Background(), QUEUE_NAME, jobId).Err()

	if err != nil {
		return fmt.Errorf("enqueue failed with error : %w", err)
	}

	return nil

}

func GetJobIDFromRedis(client *redis.Client) (string, error) {

	luaScript := ` local jobID = redis.call('LPOP', KEYS[1])
					if jobID then
						redis.call('ZADD', KEYS[2], ARGV[1], jobID)
					end
					return jobID`
	
	result, err := client.Eval(context.Background(), luaScript, []string{QUEUE_NAME, PROCESSING_QUEUE}, time.Now().Unix()).Result()
	
	if err != nil {
		if err == redis.Nil {
			return "", nil
		}
		return "", fmt.Errorf("error while dequeue : %w", err)
	}

	jobID, ok := result.(string)
	if !ok {
		return "", fmt.Errorf("unexpected type for job id: %T", result)
	}

	return jobID, nil

}


func RemoveFromProcessing(client *redis.Client, jobID string) {
	if err := client.ZRem(context.Background(), PROCESSING_QUEUE, jobID).Err(); err != nil {
		slog.Error("error while removing from processing queue", "error", err, "jobID", jobID)
	}
}