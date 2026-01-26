from loader.bulk_insert_employee_field_changes import BulkInsertEmployeeFieldChanges
from cache.employees_cache import EmployeesDataCache
from planning.field_change_data import FieldChange
from typing import Callable, Iterator
from validator.person.email_validator import EmailValidator
from utils.logger import get_logger
import uuid
import pandas as pd
Logger = get_logger("base_users_updates_retriever")

class BaseUsersUpdatesRetriever:
    """
    Base class for retrieving and persisting user field changes by comparing PDM and EC data.
    Args:
        pdm_data (pd.DataFrame): DataFrame containing PDM user data.
        ec_data (pd.DataFrame): DataFrame containing EC user data.
        user_ids (list): List of user IDs to process.
        chunk_size (int, optional): Number of changes to process in each chunk. Defaults to 1000.
        postgres_connector (optional): Connector for PostgreSQL database.
    Functions:
        persist_changes_chunked: Persists changes in chunks using a provided generator.
    """
    def __init__(self, pdm_data, ec_data, user_ids, postgres_connector, table_names, chunk_size=1000, sap_email_data=None, run_id=None, batch_context=None):
        self.pdm_data = pdm_data.copy()
        self.ec_data = ec_data.copy()
        self.user_ids = set(user_ids)
        self.chunk_size = chunk_size
        self.postgres_connector = postgres_connector
        self.bulk_inserter = BulkInsertEmployeeFieldChanges(postgres_connector, table_names)
        self.batch_id = None
        self.run_id = run_id  # Store pipeline run_id for linking batches
        self.batch_context = batch_context  # Description of batch (e.g., 'SCM/IM Users')
        self.employees_cache = EmployeesDataCache()
        self.table_names = table_names
        
        # Normalize SAP email data column names to lowercase for EmailValidator
        if sap_email_data is not None and not sap_email_data.empty:
            email_data_copy = sap_email_data.copy()
            # Map SAP columns to expected names by EmailValidator
            # SAP columns come as: personidexternal, emailaddress, emailtype, isprimary (all lowercase)
            column_mapping = {
                'personidexternal': 'userid',
                'emailaddress': 'emailaddress',
                'emailtype': 'emailtype',  
                'isprimary': 'isprimary'  
            }
            # Only rename if columns exist
            existing_cols = {k: v for k, v in column_mapping.items() if k in email_data_copy.columns}
            email_data_copy = email_data_copy.rename(columns=existing_cols)
            self.sap_email_data = email_data_copy
        else:
            self.sap_email_data = pd.DataFrame()
    
    def _initialize_batch(self):
        """
        Initializes a new batch for tracking changes using a unique batch ID.
        uuid is generated and stored in self.batch_id.
        Links batch to pipeline run if run_id is provided.
        """
        self.batch_id = str(uuid.uuid4())
        # Use run_id if provided, otherwise generate a new one for standalone batches
        run_id_to_use = self.run_id if self.run_id else str(uuid.uuid4())
        self.bulk_inserter.initiate_batch(self.batch_id, run_id_to_use, total_users=len(self.user_ids), batch_context=self.batch_context)
    
    def persist_changes_chunked(self, change_generator: Callable[[], Iterator[FieldChange]], cache_key: str):
        """
        Persist changes in chunks using the provided change generator.
        Args:
            change_generator (Callable[[], Iterator[FieldChange]]): Generator function yielding FieldChange objects.
            cache_key (str): Key to store/retrieve changes in/from EmployeesDataCache.
        """

        if not self.postgres_connector:
            raise RuntimeError("Postgres connector is required to persist changes")
        
        self._initialize_batch()
        buffer = []
        changed_users = set()
        all_changes_by_user = {}

        for _change in change_generator():
            change = _change.to_dict()
            change["batch_id"] = self.batch_id
            buffer.append(change)
            changed_users.add(change["userid"])
            all_changes_by_user.setdefault(change["userid"], []).append(change)
            if len(buffer) >= self.chunk_size:
                self.bulk_inserter.bulk_insert_employee_field_changes(buffer)
                buffer.clear()

        if buffer:
            self.bulk_inserter.bulk_insert_employee_field_changes(buffer)

        df = pd.DataFrame(
            [change for changes in all_changes_by_user.values() for change in changes]
        )
        self.employees_cache.set(cache_key, df)
        Logger.info(f"Persisted {len(changed_users)} users in batch {self.batch_id}")
        self._update_batch_stats(len(changed_users))
        Logger.info(f"Completed processing batch {self.batch_id}")

    def _update_batch_stats(self, users_with_changes: int):
        """
        Updates the batch stats in employee_field_changes_batches table.
        Args:
            users_with_changes (int): Number of users with detected changes.
        """
        update_query = f"""
        UPDATE {self.table_names['employee_field_changes_batches']}
        SET users_with_changes = %s,
            finished_at = NOW(),
            status = 'COMPLETED'
        WHERE batch_id = %s
        """

        connection = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            cursor.execute(update_query, (users_with_changes, self.batch_id))
            connection.commit()
            Logger.info(f"Updated batch {self.batch_id} stats: {users_with_changes} users with changes.")
            cursor.close()
        except Exception as e:
            Logger.error(f"Failed to update batch stats: {e}")
            if connection:
                connection.rollback()
            raise
        finally:
            if connection:
                connection.close()
    def _control_email_updates(
        self,
        userid: str,
        pdm_row: pd.Series,
        is_scm_user: bool,
        is_im_user: bool
    ):
        """
        Controls email updates by validating and deciding necessary actions using EmailValidator.
        Yields FieldChange objects for each action.
        """
        validator = EmailValidator(
            record=pdm_row,
            email_data=self.sap_email_data,
            userid=userid
        )

        decisions = validator.decide()  # structured dict with insert, delete, update_type, primary

        # -------------------------
        # Insert emails
        # -------------------------
        for item in decisions.get("insert", []):
            yield FieldChange(
                userid=userid,
                field_name=f"email::insert::{item['type']}",
                ec_value=None,
                pdm_value=item['email'],
                is_scm_user=is_scm_user,
                is_im_user=is_im_user
            )

        # -------------------------
        # Delete emails
        # -------------------------
        for item in decisions.get("delete", []):
            yield FieldChange(
                userid=userid,
                field_name=f"email::delete::{item['type']}",
                ec_value=item['email'],
                pdm_value=None,
                is_scm_user=is_scm_user,
                is_im_user=is_im_user
            )

        # -------------------------
        # Update type
        # -------------------------
        for item in decisions.get("update_type", []):
            yield FieldChange(
                userid=userid,
                field_name=f"email::update_type::{item['email']}",
                ec_value=item['old_type'],
                pdm_value=item['new_type'],
                is_scm_user=is_scm_user,
                is_im_user=is_im_user
            )

        # -------------------------
        # Primary promotion/demotion
        # -------------------------
        primary = decisions.get("primary", {})
        if primary.get("promote"):
            email = primary["promote"]
            # Determine email type from record (handle None values)
            private_email = (pdm_row.get("private_email") or "").lower()
            email_type = 18240 if email == private_email else 18242
            yield FieldChange(
                userid=userid,
                field_name=f"email::promote::{email_type}",
                ec_value=None,
                pdm_value=email,
                is_scm_user=is_scm_user,
                is_im_user=is_im_user
            )
        if primary.get("demote"):
            email = primary["demote"]
            # Determine email type from record (handle None values)
            private_email = (pdm_row.get("private_email") or "").lower()
            email_type = 18240 if email == private_email else 18242
            yield FieldChange(
                userid=userid,
                field_name=f"email::demote::{email_type}",
                ec_value=email,
                pdm_value=None,
                is_scm_user=is_scm_user,
                is_im_user=is_im_user
            )


