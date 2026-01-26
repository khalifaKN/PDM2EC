from utils.logger import get_logger
from psycopg2.extras import execute_values

logger = get_logger('bulk_insert_employee_field_changes')

class BulkInsertEmployeeFieldChanges:
    """
    Loader class for bulk inserting employee field changes into PostgreSQL.
    """
    def __init__(self, postgres_connector, table_names: dict):
        self.postgres_connector = postgres_connector  # Psycopg2DatabaseConnection instance
        self.table_names = table_names

    def bulk_insert_employee_field_changes(self, buffer: list):
        """
        Bulk inserts employee field changes into the employee_field_changes table.
        Args:
            buffer (list): List of dictionaries representing employee field changes.
        Returns:
            int: Number of inserted records.
        """
        if not buffer:
            logger.info("No employee field changes to insert.")
            return 0

        insert_query = f"""
            INSERT INTO {self.table_names['employee_field_changes']} (
                batch_id,
                userid,
                field_name,
                ec_value,
                pdm_value,
                detected_at
            ) VALUES %s
            ON CONFLICT(batch_id, userid, field_name) DO UPDATE
            SET ec_value = EXCLUDED.ec_value,
                pdm_value = EXCLUDED.pdm_value,
                detected_at = EXCLUDED.detected_at
        """

        from datetime import datetime
        
        values = [
            (
                record["batch_id"],
                record["userid"],
                record["field_name"],
                record["ec_value"],
                record["pdm_value"],
                datetime.now()
            )
            for record in buffer
        ]

        inserted_count = 0
        connection = None
        cursor = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            execute_values(cursor, insert_query, values)
            inserted_count = len(values)
            connection.commit()
            logger.info(f"Inserted {inserted_count} employee field changes.")
        except Exception as e:
            logger.error(f"Error inserting employee field changes: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

        return inserted_count

    def initiate_batch(self, batch_id: str, run_id: str, total_users: int, batch_context: str = None):
        """
        Inserts batch metadata into employee_field_changes_batches
        
        Args:
            batch_id: Unique identifier for this batch
            run_id: Pipeline run ID this batch belongs to
            total_users: Total number of users in this batch
            batch_context: Description of batch (e.g., 'SCM/IM Users', 'Standard Users')
        """
        insert_query = f"""
        INSERT INTO {self.table_names['employee_field_changes_batches']} (
            batch_id, run_id, started_at, status, total_users, users_with_changes, batch_context
        ) VALUES (%s, %s, NOW(), 'RUNNING', %s, 0, %s)
        """
        connection = None
        cursor = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            cursor.execute(insert_query, (batch_id, run_id, total_users, batch_context))
            connection.commit()
            logger.info(f"Initiated batch {batch_id} ({batch_context}) for run {run_id} with {total_users} users.")
        except Exception as e:
            logger.error(f"Failed to initiate batch: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()