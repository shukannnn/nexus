#!/bin/bash

for i in $(seq 1 50); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{
      \"type\": \"log\",
      \"payload\": {
        \"message\": \"hello from nexus log job $i\"
      }
    }" &
done

for i in $(seq 1 50); do
  curl -s -X POST http://localhost:8080/jobs \
    -H "Content-Type: application/json" \
    -d "{
      \"type\": \"webhook\",
      \"payload\": {
        \"url\": \"https://webhook.site/1f16c0e3-2fff-4337-b372-a8049ac3121e\",
        \"payload\": {\"event\": \"user.created\", \"id\": $i},
        \"secret\": \"mysecret\"
      }
    }" &
done

wait
echo "all 100 jobs enqueued"