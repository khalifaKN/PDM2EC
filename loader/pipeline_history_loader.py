from utils.logger import get_logger
from psycopg2.extras import execute_values
from datetime import datetime
from typing import Dict, List, Optional
import uuid
import json

logger = get_logger('pipeline_history_loader')

class PipelineHistoryLoader:
    """
    Loader class for pipeline execution history tracking.
    Handles pipeline_run_summary and user_sync_results tables.
    """
    def __init__(self, postgres_connector,table_names: Dict):
        """
        Initializes the PipelineHistoryLoader with a Postgres connector and table names.
        Args:
            postgres_connector: Instance of PostgresDBConnector for DB operations
            table_names (Dict):
                {
                    "pipeline_run_summary": "pdm_test.pipeline_run_summary",
                    "user_sync_results": "pdm_test.user_sync_results"
                }
        """
        self.postgres_connector = postgres_connector
        self.run_id = None
        self.table_names = table_names
    
    def start_pipeline_run(self, total_records: int, start_time=None, country=None) -> str:
        """
        Initiates a new pipeline run and returns run_id.
        
        Args:
            total_records (int): Total number of records to process
            start_time (datetime, optional): Start time of the pipeline run
            country (str, optional): Country associated with the pipeline run
            
        Returns:
            str: UUID of the pipeline run
        """
        self.run_id = str(uuid.uuid4())
        
        if country:
            insert_query = f"""
                INSERT INTO {self.table_names['pipeline_run_summary']} (
                    run_id, started_at, total_records, status, country
                ) VALUES (%s, %s, %s, %s, %s)
            """
        else:
            insert_query = f"""
                INSERT INTO {self.table_names['pipeline_run_summary']} (
                    run_id, started_at, total_records, status
                ) VALUES (%s, %s, %s, %s)
            """
        
        connection = None
        cursor = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            if country:
                cursor.execute(insert_query, (
                    self.run_id,
                    start_time if start_time else datetime.now(),
                    total_records,
                    'RUNNING',
                    country
                ))
            else:
                cursor.execute(insert_query, (
                    self.run_id,
                    start_time if start_time else datetime.now(),
                    total_records,
                    'RUNNING'
                ))
            connection.commit()
            logger.info(f"Started pipeline run {self.run_id} with {total_records} records")
        except Exception as e:
            logger.error(f"Failed to start pipeline run: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        
        return self.run_id
    
    def complete_pipeline_run(
        self,
        created_count: int = 0,
        updated_count: int = 0,
        terminated_count: int = 0,
        failed_count: int = 0,
        warning_count: int = 0,
        error_message: Optional[str] = None
    ):
        """
        Completes the pipeline run with final counts and status.
        
        Args:
            created_count: Number of successfully created records
            updated_count: Number of successfully updated records
            terminated_count: Number of successfully terminated records
            failed_count: Number of failed operations
            warning_count: Number of warnings
            error_message: Error message if pipeline failed
        """
        if not self.run_id:
            logger.warning("No active run_id to complete")
            return
        
        # Determine status
        if failed_count > 0 and (created_count + updated_count + terminated_count) == 0:
            status = 'FAILED'
        elif failed_count > 0:
            status = 'PARTIAL'
        else:
            status = 'SUCCESS'
        
        update_query = f"""
            UPDATE {self.table_names['pipeline_run_summary']}
            SET finished_at = %s,
                created_count = %s,
                updated_count = %s,
                terminated_count = %s,
                failed_count = %s,
                warning_count = %s,
                status = %s,
                error_message = %s
            WHERE run_id = %s
        """
        
        connection = None
        cursor = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            cursor.execute(update_query, (
                datetime.now(),
                created_count,
                updated_count,
                terminated_count,
                failed_count,
                warning_count,
                status,
                error_message,
                self.run_id
            ))
            connection.commit()
            logger.info(f"Completed pipeline run {self.run_id}: {status} - "
                       f"Created: {created_count}, Updated: {updated_count}, "
                       f"Terminated: {terminated_count}, Failed: {failed_count}, "
                       f"Warnings: {warning_count}")
        except Exception as e:
            logger.error(f"Failed to complete pipeline run: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
    
    def bulk_insert_results(self, results: List[Dict]):
        """
        Bulk inserts user sync results (success, failures, and warnings).
        
        Args:
            results: List of sync result records with keys:
                - user_id: str
                - operation: str (CREATE, UPDATE, TERMINATE)
                - status: str (SUCCESS, FAILED, WARNING)
                - error_message: str (optional)
                - warning_message: str (optional)
                - success_message: str (optional)
                - payload_snapshot: dict (optional)
        """
        if not results:
            logger.info("No results to insert")
            return
        
        if not self.run_id:
            logger.warning("No active run_id for results")
            return
        
        insert_query = f"""
            INSERT INTO {self.table_names['user_sync_results']} (
                run_id,
                user_id,
                operation,
                status,
                error_message,
                warning_message,
                success_message,
                payload_snapshot,
                failed_entities,
                success_entities,
                skipped_entities
            ) VALUES %s
            ON CONFLICT (run_id, user_id, operation, created_at) DO NOTHING
        """

        # Deduplicate results using a detailed key so distinct messages/payloads are preserved.
        # Key components: user_id, operation, status, message text, payload snapshot JSON
        seen = set()
        unique_results = []
        for record in results:
            message_text = record.get('error_message') or record.get('warning_message') or record.get('success_message')
            try:
                payload_text = json.dumps(record.get('payload_snapshot')) if record.get('payload_snapshot') else None
            except Exception:
                payload_text = str(record.get('payload_snapshot'))

            key = (
                record.get('user_id'),
                record.get('operation'),
                record.get('status'),
                message_text,
                payload_text
            )
            if key in seen:
                logger.info(f"Skipping duplicate result record for user {record.get('user_id')} op {record.get('operation')} message='{message_text}'")
                continue
            seen.add(key)
            unique_results.append(record)

        values = [
            (
                self.run_id,
                record['user_id'],
                record['operation'],
                record['status'],
                record.get('error_message'),
                record.get('warning_message'),
                record.get('success_message'),
                json.dumps(record.get('payload_snapshot')) if record.get('payload_snapshot') else None,
                json.dumps(record.get('failed_entities')) if record.get('failed_entities') else None,
                json.dumps(record.get('success_entities')) if record.get('success_entities') else None,
                json.dumps(record.get('skipped_entities')) if record.get('skipped_entities') else None
            )
            for record in unique_results
        ]
        
        connection = None
        cursor = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            execute_values(cursor, insert_query, values)
            connection.commit()
            logger.info(f"Inserted {len(unique_results)} result records")
        except Exception as e:
            logger.error(f"Failed to insert result records: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
