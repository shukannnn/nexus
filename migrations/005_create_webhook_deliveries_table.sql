CREATE TABLE webhook_deliveries (
    job_id UUID PRIMARY KEY REFERENCES jobs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);