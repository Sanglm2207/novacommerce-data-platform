-- NovaCommerce Day 07: quality audit trail (run once after V003)
CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS etl.pipeline_runs (
    run_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    pipeline_name text NOT NULL,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    status text NOT NULL DEFAULT 'RUNNING'
       CHECK (status IN ('RUNNING','SUCCESS','FAILED')),
    checks_total integer NOT NULL DEFAULT 0,
    checks_failed integer NOT NULL DEFAULT 0,
    error_message text
);

CREATE TABLE IF NOT EXISTS etl.data_quality_results (
    result_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id bigint NOT NULL REFERENCES etl.pipeline_runs(run_id),
    check_name text NOT NULL,
    observed_value numeric NOT NULL,
    threshold numeric NOT NULL,
    passed boolean NOT NULL,
    severity text NOT NULL CHECK (severity IN ('ERROR','WARNING')),
    checked_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (run_id, check_name)
);

CREATE INDEX IF NOT EXISTS idx_pipeline_runs_recent
ON etl.pipeline_runs (pipeline_name, started_at DESC);
