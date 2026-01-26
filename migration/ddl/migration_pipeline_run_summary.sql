CREATE TABLE pdm_test.migration_pipeline_run_summary (
    run_id           UUID PRIMARY KEY,
    started_at       TIMESTAMP NOT NULL,
    finished_at      TIMESTAMP,
    country          VARCHAR(100),
    total_records    INTEGER NOT NULL,
    created_count    INTEGER NOT NULL DEFAULT 0,
    updated_count    INTEGER NOT NULL DEFAULT 0,
    terminated_count INTEGER NOT NULL DEFAULT 0,
    failed_count     INTEGER NOT NULL DEFAULT 0,
    warning_count    INTEGER NOT NULL DEFAULT 0,
    status           VARCHAR(20) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED')),
    error_message    TEXT,
    created_at       TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_migration_pipeline_run_started_at
    ON pdm_test.migration_pipeline_run_summary (started_at DESC);

COMMENT ON TABLE pdm_test.migration_pipeline_run_summary IS 'Tracks pipeline execution history with counts and status';
COMMENT ON COLUMN pdm_test.migration_pipeline_run_summary.run_id IS 'Unique identifier for each pipeline run';
COMMENT ON COLUMN pdm_test.migration_pipeline_run_summary.terminated_count IS 'Number of successfully terminated user accounts';
COMMENT ON COLUMN pdm_test.migration_pipeline_run_summary.error_message IS 'Pipeline-level error message if status is FAILED';
COMMENT ON COLUMN pdm_test.migration_pipeline_run_summary.status IS 'RUNNING: in progress, SUCCESS: all succeeded, PARTIAL: some failures, FAILED: critical failure';