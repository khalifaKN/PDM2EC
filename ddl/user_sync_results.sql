-- pdm_test.user_sync_results definition

-- Drop table

-- DROP TABLE pdm_test.user_sync_results;

CREATE TABLE pdm_test.user_sync_results (
	id int8 DEFAULT nextval('pdm_test.user_sync_failures_id_seq'::regclass) NOT NULL,
	run_id uuid NOT NULL,
	user_id varchar(100) NOT NULL,
	operation varchar(20) NOT NULL,
	status varchar(20) NOT NULL,
	error_message text NULL,
	warning_message text NULL,
	payload_snapshot jsonb NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	success_message text NULL,
	failed_entities jsonb NULL,
	success_entities jsonb NULL,
	skipped_entities jsonb NULL,
	CONSTRAINT user_sync_results_operation_check CHECK (((operation)::text = ANY ((ARRAY['CREATE'::character varying, 'UPDATE'::character varying, 'TERMINATE'::character varying])::text[]))),
	CONSTRAINT user_sync_results_pkey PRIMARY KEY (id),
	CONSTRAINT user_sync_results_status_check CHECK (((status)::text = ANY ((ARRAY['SUCCESS'::character varying, 'FAILED'::character varying, 'WARNING'::character varying])::text[]))),
	CONSTRAINT user_sync_results_unique_record UNIQUE (run_id, user_id, operation, created_at),
	CONSTRAINT fk_user_sync_results_run FOREIGN KEY (run_id) REFERENCES pdm_test.pipeline_run_summary(run_id)
);
CREATE INDEX idx_user_sync_results_operation ON pdm_test.user_sync_results USING btree (operation);
CREATE INDEX idx_user_sync_results_payload_gin ON pdm_test.user_sync_results USING gin (payload_snapshot);
CREATE INDEX idx_user_sync_results_run ON pdm_test.user_sync_results USING btree (run_id);
CREATE INDEX idx_user_sync_results_status ON pdm_test.user_sync_results USING btree (status);
CREATE INDEX idx_user_sync_results_user ON pdm_test.user_sync_results USING btree (user_id);