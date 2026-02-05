import sys
import os

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = DAG_DIR

if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd

# Import pipeline functions
from test.test_migration_pipeline import (
    extract_and_cache_database_data,
    extract_and_cache_sap_data,
    load_cached_data,
    extract_employee_classifications,
    validate_new_employees,
    prepare_new_employees_data,
    resolve_creation_order,
    process_new_employees,
    detect_field_changes,
    process_field_updates,
    process_inactive_users,
    save_final_outputs,
    print_final_summary,
    send_notification_email
)

# File-based caches
from cache.postgres_cache import PostgresDataCache
from cache.oracle_cache import OracleDataCache
from cache.sap_cache import SAPDataCache

logger = logging.getLogger("airflow.task")

default_args = {
    'owner': 'khalifa',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='pdm_to_ec_sync',
    default_args=default_args,
    description='PDM to EC Data Synchronization',
    schedule=None,
    start_date=datetime(2026, 1, 30),
    catchup=False,
) as dag:

    # STEP 1
    extract_db_task = PythonOperator(
        task_id='extract_database_data',
        python_callable=extract_and_cache_database_data
    )

    # STEP 2
    extract_sap_task = PythonOperator(
        task_id='extract_sap_data',
        python_callable=extract_and_cache_sap_data
    )

    # STEP 3
    def load_cache_wrapper():
        load_cached_data()

    load_cache_task = PythonOperator(
        task_id='load_cached_data',
        python_callable=load_cache_wrapper
    )

    # STEP 4
    def classify_employees_wrapper():

        cached_ec = PostgresDataCache.get("ec_data_df")
        cached_pdm = OracleDataCache.get("pdm_data_df")

        existing, new, inactive = extract_employee_classifications(
            cached_pdm, cached_ec
        )

        # Save outputs for later stages
        existing.to_parquet("/tmp/pdm_cache/existing.parquet")
        new.to_parquet("/tmp/pdm_cache/new.parquet")
        inactive.to_parquet("/tmp/pdm_cache/inactive.parquet")

    classify_employees_task = PythonOperator(
        task_id='classify_employees',
        python_callable=classify_employees_wrapper
    )

    # STEP 5
    def validate_new_wrapper():
        sap_cache = SAPDataCache.get("employees_df")
        new = PostgresDataCache.get("new_employees_df") or \
              pd.read_parquet("/tmp/pdm_cache/new.parquet")

        validate_new_employees(new, SAPDataCache())

    validate_new_task = PythonOperator(
        task_id='validate_new_employees',
        python_callable=validate_new_wrapper
    )

    # STEP 6
    def prepare_new_wrapper():
        new = pd.read_parquet("/tmp/pdm_cache/new.parquet")
        prepared = prepare_new_employees_data(new)
        prepared.to_parquet("/tmp/pdm_cache/prepared_new.parquet")

    prepare_new_task = PythonOperator(
        task_id='prepare_new_employees',
        python_callable=prepare_new_wrapper
    )

    # STEP 7
    def resolve_order_wrapper():
        prepared = pd.read_parquet("/tmp/pdm_cache/prepared_new.parquet")
        existing = pd.read_parquet("/tmp/pdm_cache/existing.parquet")
        batches, summary = resolve_creation_order(prepared, existing)
        
        if batches is not None and summary is not None:
            PostgresDataCache.set("batches", batches)
            PostgresDataCache.set("batches_summary", summary)
        else:
            logger.error("There's no new employees to process or an error occurred during order resolution.")

    resolve_order_task = PythonOperator(
        task_id='resolve_creation_order',
        python_callable=resolve_order_wrapper
    )

    # STEP 8
    def process_new_wrapper():
        prepared = pd.read_parquet("/tmp/pdm_cache/prepared_new.parquet")
        batches = PostgresDataCache.get("batches")
        summary = PostgresDataCache.get("batches_summary")
        process_new_employees(prepared, batches, summary)

    process_new_task = PythonOperator(
        task_id='process_new_employees',
        python_callable=process_new_wrapper
    )

    # STEP 9
    def detect_changes_wrapper():
        cached_ec = PostgresDataCache.get("ec_data_df")
        cached_pdm = OracleDataCache.get("pdm_data_df")
        existing = pd.read_parquet("/tmp/pdm_cache/existing.parquet")

        changes = detect_field_changes(cached_pdm, cached_ec, existing)
        PostgresDataCache.set("field_changes", changes)

    detect_changes_task = PythonOperator(
        task_id='detect_field_changes',
        python_callable=detect_changes_wrapper
    )

    # STEP 10
    def process_updates_wrapper():
        changes = PostgresDataCache.get("field_changes")
        process_field_updates(changes)

    process_updates_task = PythonOperator(
        task_id='process_field_updates',
        python_callable=process_updates_wrapper
    )

    # STEP 11
    def process_inactive_wrapper():
        inactive = pd.read_parquet("/tmp/pdm_cache/inactive.parquet")
        pdm = OracleDataCache.get("pdm_data_df")
        ec = PostgresDataCache.get("ec_data_df")
        process_inactive_users(inactive, pdm, ec)

    process_inactive_task = PythonOperator(
        task_id='process_inactive_users',
        python_callable=process_inactive_wrapper
    )

    # STEP 12
    def save_outputs_wrapper():
        existing = pd.read_parquet("/tmp/pdm_cache/existing.parquet")
        new = pd.read_parquet("/tmp/pdm_cache/new.parquet")
        inactive = pd.read_parquet("/tmp/pdm_cache/inactive.parquet")
        changes = PostgresDataCache.get("field_changes")

        save_final_outputs(existing, new, inactive, changes)
        print_final_summary(existing, new, inactive, changes)

    save_outputs_task = PythonOperator(
        task_id='save_final_outputs',
        python_callable=save_outputs_wrapper
    )

    # STEP 13
    send_notification_task = PythonOperator(
        task_id='send_notification_email',
        python_callable=lambda: send_notification_email(
            datetime.now().strftime("%Y%m%d%H%M%S")
        )
    )

    # DAG graph
    extract_db_task >> extract_sap_task >> load_cache_task
    load_cache_task >> classify_employees_task
    classify_employees_task >> validate_new_task >> prepare_new_task >> resolve_order_task >> process_new_task
    classify_employees_task >> detect_changes_task >> process_updates_task
    classify_employees_task >> process_inactive_task
    [process_new_task, process_updates_task, process_inactive_task] >> save_outputs_task >> send_notification_task