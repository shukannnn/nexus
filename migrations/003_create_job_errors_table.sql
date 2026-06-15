CREATE TABLE job_errors (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID REFERENCES jobs(id),
    attempt INT,
    error TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);