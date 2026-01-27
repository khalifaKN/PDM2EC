""" "
This module contains migration-specific processing logic for handling inactive employees.
The logic extends migration processing which itself extends core processing. It customizes the batch upsert execution to ensure
that inactive employees are processed correctly during migration.
The specific logic for migration inactive employee processing is the next:

- Create dummy positions for inactive employees if doesn't exist.

- Create minimal payloads for inactive employees which are:

   1- Historical Position (Dummy Position, won't be created if already exists)
   2- PerPerson
   3- EmpEnployment (Actual hire date, Date of entry kn group)
   4- EmpJob (Linked to dummy position): Only Initload will be sent without DATACHG
   5- Termination: Actual termination date

- The data source for inactive employees is not the same as active employees. The data extraction will be processed in earilier steps.
- The inactive employees will be processed are those left the company in the last 5 years.

The next will be overridden from CoreProcessor:
 - Constructor (Initializer): To set up any migration-specific attributes.
 - Class Attributes: EXECUTION_PLAN, ENTITY_DEPENDENCIES
 - Instance Attributes: collected_payloads
 - Class Methods:
    - _process_single_user : To handle the overall migration logic per user.
    - _build_update_payloads: To build the specific payloads for inactive employees if employee already exists in EC.
"""

from mapper.retrieve_person_id_external import get_userid_from_personid
from orchestrator.user_context import UserExecutionContext
from utils.logger import get_logger
from api.auth_client import AuthAPI
from api.api_client import APIClient
from api.upsert_client import UpsertClient
from cache.postgres_cache import PostgresDataCache
from cache.oracle_cache import OracleDataCache
from cache.sap_cache import SAPDataCache
from cache.employees_cache import EmployeesDataCache
from payload_builders.employment._terminate_emp import (
    EmploymentTerminationPayloadBuilder,
)
from migration.migration_processing import MigrationProcessor

import pandas as pd

Logger = get_logger("migration_ inactive_emp_processing")


class MigrationInactiveEmpProcessor(MigrationProcessor):
    """
    Processor for handling migration of inactive employees.
    Extends MigrationProcessor to implement logic specific to inactive employees during migration.
    """

    EXECUTION_PLAN = [
        ("Position", "position"),
        ("PerPerson", "perperson"),
        ("EmpEmployment", "empemployment"),
        ("EmpInitLoadJob", "empinitloadjob"),
        ("EmpJob", "empjob"),
        ("PerPersonal", "perpersonal"),
        ("EmpEmploymentTermination", "empemploymenttermination"),
    ]
    ENTITY_DEPENDENCIES = {
        "Position": [],
        "PerPerson": [],
        "PerPersonal": ["PerPerson"],
        "EmpEmployment": ["Position", "PerPerson"],
        "EmpInitLoadJob": ["Position", "PerPerson"],
        "EmpJob": ["Position", "PerPerson"],
        "EmpEmploymentTermination": ["EmpEmployment", "PerPerson"],
    }

    def __init__(
        self,
        auth_url: str,
        base_url: str,
        auth_credentials: dict,
        ordered_batches: list[pd.DataFrame],
        batches_summary: dict,
        job_code: dict,
        positions_cache_key: str = "positions_df",
        max_retries: int = 5,
    ):
        # Call parent's __init__
        super().__init__(
            auth_url=auth_url,
            base_url=base_url,
            auth_credentials=auth_credentials,
            ordered_batches=ordered_batches,
            batches_summary=batches_summary,
            job_code=job_code,
            positions_cache_key=positions_cache_key,
            max_retries=max_retries,
        )
        self.auth_api = AuthAPI(
            auth_url=auth_url,
            client_id=auth_credentials.get("client_id"),
            client_secret=auth_credentials.get("assertion"),
            grant_type=auth_credentials.get("grant_type"),
            company_id=auth_credentials.get("company_id"),
            max_retries=max_retries,
        )
        self.api_client = APIClient(
            base_url=base_url, token=self.auth_api.get_token(), max_retries=max_retries
        )
        self.upsert_client = UpsertClient(
            api_client=self.api_client, max_retries=max_retries
        )
        self.collected_payloads = {
            "Position": {},
            "PerPerson": {},
            "EmpEmployment": {},
            "EmpInitLoadJob": {},
            "EmpJob": {},
            "PerPersonal": {},
            "EmpEmploymentTermination": {},
        }
        self.postgres_cache = PostgresDataCache()
        self.oracle_cache = OracleDataCache()
        self.sap_cache = SAPDataCache()
        self.employees_cache = EmployeesDataCache()
        self.positions_cache_key = positions_cache_key
        self.job_code = job_code

    def _handle_employment_termination(self, row: pd.Series, ctx: UserExecutionContext):
        """
        Build EmpEmploymentTermination payload for a user.

        Args:
            row (pd.Series): The data row for the user containing termination details
            ctx (UserExecutionContext): The execution context of the user
        """
        try:
            user_id = ctx.user_id
            end_date = row.get("date_of_leave")
            terminate_event_id = row.get("exit_reason_id")

            if pd.isna(end_date):
                ctx.fail(f"Missing end_date (date_of_leave) for user {user_id}")
                return

            if pd.isna(terminate_event_id):
                ctx.fail(
                    f"Missing terminate_event_id (exit_reason_id) for user {user_id}"
                )
                return

            terminate_event_reason = self.exit_events.get(terminate_event_id)
            if not terminate_event_reason:
                ctx.fail(
                    f"Invalid terminate_event_id {terminate_event_id} for user {user_id}"
                )
                return

            emp_builder = EmploymentTerminationPayloadBuilder(
                user_id=user_id,
                end_date=end_date,
                terminate_event_reason=terminate_event_reason,
            )

            payload = emp_builder.build_emp_employment_termination_payload()

            if not payload:
                ctx.fail(
                    f"Failed to build EmpEmploymentTermination payload for user {user_id}"
                )
                return

            ctx.payloads["emp_termination"] = payload
            Logger.info(f"✓ EmpEmploymentTermination payload built for user {user_id}")

        except Exception as e:
            ctx.fail(
                f"Error building employment termination for user {ctx.user_id}: {e}"
            )

    def _process_single_user(self, row, ctx, results):
        """
        Process a single inactive employee for migration.
        The steps include:
        - Create or retrieve dummy position for the inactive employee.
        - Build payloads for Historical Position (Dummy Position), PerPerson, EmpEmployment, EmpJob (InitLoad only),PerPersonal, PerEmail, PerPhone, and Termination.
        """
        try:
            user_id = ctx.user_id
            Logger.info(f"Start processing user {user_id}")
            if pd.isna(user_id) or not str(user_id).strip():
                ctx.fail("Missing or null userid")
                return
            # Store original row for potential retry after Position cache refresh
            ctx.runtime["original_row"] = row
            # CREATE OR GET DUMMY POSITION AND ASSIGN TO USER CONTEXT TO BE USED IN EMPLOYMENT PROCESSING
            Logger.info(f"Creating or retrieving dummy position for user {user_id}")
            self._create_or_get_dummy_position(ctx)
            if not ctx.dummy_position:
                ctx.fail("Failed to create or retrieve dummy position")
                return
            Logger.info(f"Dummy position for user {user_id}: {ctx.dummy_position}")
            # Store Ec USERID in context
            ctx.ec_user_id = get_userid_from_personid(user_id)
            # 0️ Check if user has existing EmpJob
            ctx.has_existing_empjob = self._has_existing_empjob(
                user_id, ctx.ec_user_id, ctx.dummy_position
            )
            Logger.info(
                f"User {user_id} has existing EmpJob: {ctx.has_existing_empjob}"
            )
            # POSITION
            self._handle_position(row, ctx, results)
            if ctx.has_errors:
                return
            # PERSON
            Logger.info(f"About to call _handle_person for user {user_id}")
            self._handle_person(row, ctx)
            Logger.info(
                f"After _handle_person for user {user_id}, has_errors={ctx.has_errors}, errors={ctx.errors}"
            )
            if ctx.has_errors:
                Logger.error(
                    f"Early return after person handling for {user_id} due to errors: {ctx.errors}"
                )
                return
            # EMPLOYMENT
            Logger.info(
                f"About to call _handle_employment for user {user_id}, ctx.has_errors={ctx.has_errors}"
            )
            self._handle_employment(row, ctx, results)
            if ctx.has_errors:
                return
        except Exception as e:
            ctx.fail(f"Error processing user {ctx.user_id}: {e}")
