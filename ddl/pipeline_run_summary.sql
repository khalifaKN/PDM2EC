-- pdm_test.pipeline_run_summary definition

-- Drop table

-- DROP TABLE pdm_test.pipeline_run_summary;

CREATE TABLE pdm_test.pipeline_run_summary (
	run_id uuid NOT NULL,
	started_at timestamp NOT NULL,
	finished_at timestamp NULL,
	total_records int4 NOT NULL,
	created_count int4 DEFAULT 0 NOT NULL,
	updated_count int4 DEFAULT 0 NOT NULL,
	terminated_count int4 DEFAULT 0 NOT NULL,
	failed_count int4 DEFAULT 0 NOT NULL,
	warning_count int4 DEFAULT 0 NOT NULL,
	status varchar(20) NOT NULL,
	error_message text NULL,
	created_at timestamp DEFAULT now() NOT NULL,
	CONSTRAINT pipeline_run_summary_pkey PRIMARY KEY (run_id),
	CONSTRAINT pipeline_run_summary_status_check CHECK (((status)::text = ANY ((ARRAY['RUNNING'::character varying, 'SUCCESS'::character varying, 'PARTIAL'::character varying, 'FAILED'::character varying])::text[])))
);
CREATE INDEX idx_pipeline_run_started_at ON pdm_test.pipeline_run_summary USING btree (started_at DESC);