CREATE TABLE dead_letter_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID NOT NULL REFERENCES jobs(id),
    type TEXT NOT NULL,
    payload JSONB NOT NULL,
    last_error TEXT,
    attempts INT NOT NULL,
    max_attempts INT NOT NULL,
    replay_job_id UUID REFERENCES jobs(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TRIGGER dead_letter_jobs_updated_at_trigger
BEFORE UPDATE ON dead_letter_jobs
FOR EACH ROW
EXECUTE FUNCTION update_updated_at_column();