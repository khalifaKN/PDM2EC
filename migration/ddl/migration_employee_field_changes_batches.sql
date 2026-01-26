CREATE TABLE pdm_test.migration_employee_field_changes_batches (
    batch_id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    started_at TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'RUNNING' CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED')),
    total_users INT NOT NULL,
    users_with_changes INT DEFAULT 0,
    CONSTRAINT fk_migration_employee_field_changes_batches_run
        FOREIGN KEY (run_id)
        REFERENCES pdm_test.migration_pipeline_run_summary (run_id)
);

CREATE INDEX idx_migration_employee_field_changes_batches_run_id
    ON pdm_test.migration_employee_field_changes_batches(run_id);

CREATE INDEX idx_migration_employee_field_changes_batches_started_at 
    ON pdm_test.migration_employee_field_changes_batches(started_at DESC);

CREATE INDEX idx_migration_employee_field_changes_batches_status 
    ON pdm_test.migration_employee_field_changes_batches(status);

COMMENT ON TABLE pdm_test.migration_employee_field_changes_batches IS 'Batch metadata for field change detection runs';
COMMENT ON COLUMN pdm_test.migration_employee_field_changes_batches.status IS 'RUNNING: in progress, COMPLETED: finished successfully, FAILED: batch processing failed';