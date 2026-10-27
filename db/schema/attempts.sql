-- Additive migration; safe to apply to an existing database without losing runs.
ALTER TABLE runs ADD COLUMN IF NOT EXISTS failed_step_id TEXT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS current_attempt_id TEXT;
ALTER TABLE steps ADD COLUMN IF NOT EXISTS retry_after TIMESTAMPTZ;

-- One immutable identity per command offer (including explicit refusals).
CREATE TABLE IF NOT EXISTS step_attempts (
    id TEXT PRIMARY KEY,
    step_id TEXT NOT NULL REFERENCES steps(id),
    number INT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (step_id, number)
);

-- History is append-only in the application; outcomes never overwrite offers.
CREATE TABLE IF NOT EXISTS attempt_events (
    id BIGSERIAL PRIMARY KEY,
    attempt_id TEXT NOT NULL REFERENCES step_attempts(id),
    kind TEXT NOT NULL CHECK (kind IN
        ('offered', 'accepted', 'refused', 'completed', 'failed', 'retry_scheduled', 'unknown')),
    error TEXT,
    retryable BOOLEAN,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (attempt_id, kind)
);
