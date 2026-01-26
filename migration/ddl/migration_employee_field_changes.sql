CREATE TABLE pdm_test.migration_employee_field_changes (
    id BIGSERIAL PRIMARY KEY,
    batch_id UUID NOT NULL REFERENCES pdm_test.migration_employee_field_changes_batches(batch_id),
    userid TEXT NOT NULL,
    field_name TEXT NOT NULL,
    ec_value TEXT,
    pdm_value TEXT,
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE(batch_id, userid, field_name)
);

CREATE INDEX idx_migration_employee_field_changes_batch_id ON pdm_test.migration_employee_field_changes(batch_id);
CREATE INDEX idx_migration_employee_field_changes_userid ON pdm_test.migration_employee_field_changes(userid);
CREATE INDEX idx_migration_employee_field_changes_detected_at ON pdm_test.migration_employee_field_changes(detected_at DESC);

COMMENT ON TABLE pdm_test.migration_employee_field_changes IS 'Tracks field-level changes detected for existing employees';
COMMENT ON COLUMN pdm_test.migration_employee_field_changes.ec_value IS 'Current value in SAP SuccessFactors (EC)';
COMMENT ON COLUMN pdm_test.migration_employee_field_changes.pdm_value IS 'New value from source system (PDM)';