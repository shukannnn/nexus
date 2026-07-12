# Nexus

A distributed job queue and scheduler built in Go. Nexus accepts background jobs over HTTP, queues them in Redis, and processes them reliably using a concurrent worker pool backed by PostgreSQL.

Built as the backend engine for a competitive programming judge, but general enough to run any background workload.


## Running locally

```bash
cp .env.example .env   # fill in your values
docker compose up --build
```

Run migrations manually against the PostgreSQL container before first use.

## API

```
POST /jobs              Enqueue a job
GET  /jobs/:id          Get job status

POST /judge             Submit code for judging
GET  /judge/:id         Get verdict (AC / WA / TLE / RE / CE)

POST /dead-letter/:id/replay    Replay a failed job
```

## Environment variables

| Variable | Description |
|---|---|
| `PORT` | HTTP server port (default: 8080) |
| `REDIS_ADDR` | Redis address (default: localhost:6379) |
| `DATABASE_URL` | PostgreSQL connection string |
| `POOL_SIZE` | Number of concurrent workers |
| `GRACE_PERIOD` | Shutdown grace period in seconds |
| `REAP_INTERVAL` | How often the reaper runs in seconds |
| `VISIBILITY_TIMEOUT` | Job visibility timeout in seconds |
| `SENDGRID_API_KEY` | SendGrid API key for email jobs |