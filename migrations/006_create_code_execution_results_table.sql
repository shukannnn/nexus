CREATE TABLE code_execution_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID UNIQUE NOT NULL,
    status VARCHAR NOT NULL,
    stdout TEXT,
    stderr TEXT,
    time_ms INTEGER,
    memory_kb INTEGER,
    exit_code INTEGER,
    message TEXT,
    verdict VARCHAR,
    created_at TIMESTAMPTZ DEFAULT NOW()
);