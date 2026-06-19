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
		return "", fmt.Errorf("error while getJobidfromredis : %w", err)
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

func UpdateValueInProcessingQueue(client *redis.Client, jobID string, expiryTime int64) {
	if err := client.ZAdd(context.Background(), PROCESSING_QUEUE, redis.Z{
		Score:  float64(expiryTime),
		Member: jobID,
	}).Err(); err != nil {
		slog.Error("error while upating value in processing queue", "error", err, "jobID", jobID)
	}
}

func RemoveFromProcessingAndInsertIntoJob(client *redis.Client, jobID string, source string) {
	luaScript := `
	local removed = redis.call('ZREM', KEYS[2], ARGV[1])
	if removed == 1 then
		redis.call('RPUSH', KEYS[1], ARGV[1])
	end`

	if err := client.Eval(context.Background(), luaScript, []string{QUEUE_NAME, PROCESSING_QUEUE}, jobID).Err(); err != nil {
		 if err == redis.Nil {
            return
        }
		slog.Error("error while removing from processing queue and inserting into job queue", "error", err, "jobID", jobID, "source", source)
	}
}

func GetStaleJobs(client *redis.Client) ([]redis.Z, error){
	time := time.Now().Unix()
	result, err := client.ZRangeArgsWithScores(context.Background(), redis.ZRangeArgs{
		Key: PROCESSING_QUEUE,
		ByScore: true,
		Start: "-inf",
		Stop: time,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("error while quering stale jobs: %w", err)
	}

	return result, nil
}