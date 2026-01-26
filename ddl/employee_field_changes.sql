-- pdm_test.migration_employee_field_changes definition

-- Drop table

-- DROP TABLE pdm_test.migration_employee_field_changes;

CREATE TABLE pdm_test.migration_employee_field_changes (
	id bigserial NOT NULL,
	batch_id uuid NOT NULL,
	userid text NOT NULL,
	field_name text NOT NULL,
	ec_value text NULL,
	pdm_value text NULL,
	detected_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT migration_employee_field_changes_batch_id_userid_field_name_key UNIQUE (batch_id, userid, field_name),
	CONSTRAINT migration_employee_field_changes_pkey PRIMARY KEY (id),
	CONSTRAINT migration_employee_field_changes_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES pdm_test.migration_employee_field_changes_batches(batch_id)
);
CREATE INDEX idx_migration_employee_field_changes_batch_id ON pdm_test.migration_employee_field_changes USING btree (batch_id);
CREATE INDEX idx_migration_employee_field_changes_detected_at ON pdm_test.migration_employee_field_changes USING btree (detected_at DESC);
CREATE INDEX idx_migration_employee_field_changes_userid ON pdm_test.migration_employee_field_changes USING btree (userid);