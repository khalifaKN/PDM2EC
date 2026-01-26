CREATE TABLE pdm_test.migration_user_sync_results (
    id                     BIGSERIAL PRIMARY KEY,
    run_id                 UUID NOT NULL,
    user_id                VARCHAR(100) NOT NULL,
    operation              VARCHAR(20) NOT NULL CHECK (operation IN ('CREATE', 'UPDATE', 'TERMINATE')),
    status                 VARCHAR(20) NOT NULL CHECK (status IN ('SUCCESS', 'FAILED', 'WARNING')),
    error_message          TEXT,
    warning_message        TEXT,
    success_message        TEXT,
    payload_snapshot       JSONB,
    created_at             TIMESTAMP NOT NULL DEFAULT NOW(),
    CONSTRAINT fk_migration_user_sync_results_run
        FOREIGN KEY (run_id)
        REFERENCES pdm_test.migration_pipeline_run_summary (run_id)
);

CREATE INDEX idx_migration_user_sync_results_run
    ON pdm_test.migration_user_sync_results (run_id);

CREATE INDEX idx_migration_user_sync_results_user
    ON pdm_test.migration_user_sync_results (user_id);

CREATE INDEX idx_migration_user_sync_results_status
    ON pdm_test.migration_user_sync_results (status);

CREATE INDEX idx_migration_user_sync_results_operation
    ON pdm_test.migration_user_sync_results (operation);

--USING GIN index for efficient querying of JSONB payloads, GIN (Generalized Inverted Index) is designed to handle complex data types like JSONB.
CREATE INDEX idx_migration_user_sync_results_payload_gin
    ON pdm_test.migration_user_sync_results
    USING GIN (payload_snapshot);

COMMENT ON TABLE pdm_test.migration_user_sync_results IS 'Tracks all user sync operation results: successes, failures, and warnings';
COMMENT ON COLUMN pdm_test.migration_user_sync_results.operation IS 'CREATE: new employee, UPDATE: field changes, TERMINATE: employment termination and user deactivation';
COMMENT ON COLUMN pdm_test.migration_user_sync_results.status IS 'SUCCESS: operation succeeded cleanly, FAILED: operation failed, WARNING: operation succeeded with warnings';
COMMENT ON COLUMN pdm_test.migration_user_sync_results.success_message IS 'Confirmation message for successful operations';
COMMENT ON COLUMN pdm_test.migration_user_sync_results.payload_snapshot IS 'JSON snapshot of the payload (for debugging and audit trail)';