-- pdm_test.migration_employee_field_changes_batches definition

-- Drop table

-- DROP TABLE pdm_test.migration_employee_field_changes_batches;

CREATE TABLE pdm_test.migration_employee_field_changes_batches (
	batch_id uuid NOT NULL,
	run_id uuid NOT NULL,
	started_at timestamp DEFAULT now() NOT NULL,
	finished_at timestamp NULL,
	status text DEFAULT 'RUNNING'::text NOT NULL,
	total_users int4 NOT NULL,
	users_with_changes int4 DEFAULT 0 NULL,
	CONSTRAINT migration_employee_field_changes_batches_pkey PRIMARY KEY (batch_id),
	CONSTRAINT migration_employee_field_changes_batches_status_check CHECK ((status = ANY (ARRAY['RUNNING'::text, 'COMPLETED'::text, 'FAILED'::text]))),
	CONSTRAINT fk_migration_employee_field_changes_batches_run FOREIGN KEY (run_id) REFERENCES pdm_test.migration_pipeline_run_summary(run_id)
);
CREATE INDEX idx_migration_employee_field_changes_batches_run_id ON pdm_test.migration_employee_field_changes_batches USING btree (run_id);
CREATE INDEX idx_migration_employee_field_changes_batches_started_at ON pdm_test.migration_employee_field_changes_batches USING btree (started_at DESC);
CREATE INDEX idx_migration_employee_field_changes_batches_status ON pdm_test.migration_employee_field_changes_batches USING btree (status);