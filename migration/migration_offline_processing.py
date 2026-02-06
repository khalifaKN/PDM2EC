"""
This module contains the MigrationProcessor class.

The MigrationProcessor class orchestrates the bulk migration of PDM Users:

The main difference between MigrationProcessor and CoreProcessor is that the migration processor has specific extra and different logic around:
- Position: When migrating users, a dummy position shared by all company's employees in this country:
    - One country can has one or more dummy positions depends of the number of companies in that country.
    - The dummy position is created if not exists.
    - The dummy position is assigned to all migrating users in that company in that country.
    - The dummy position values depends on the country and company of the migrating users.
- EmpJob: Since this entity depends on Position, the following logic is required:
    - Step 1: Check if there's any user already existing in EC with the same pdm uid:
        - If yes, we delete the existing EmpJob record to avoid conflicts.
    - Step 2: Create EmpJob record (INITLOAD) with the dummy position assigned in the previous step.
    - Step 3: Create EmpJob (DATACHG) record to update the position to the correct one from PDM.
    - Note: This is required to keep trace of the position history in EC.
- All the users have not been migrated before, should be treated as new hires and go through the same logic as new hires:
    - Create Position if not exists.
    - Create PerPerson record.
    - Create PositionMatrixRelationships record.
    - Create EmpEmployment record.
    - Create EmpJob record with INITLOAD event reason.
    - Create EmpJob record with DATACHG event reason to update the position.
    - Create EmpJobRelationships records if needed.
    - Create PerPersonal, PerEmail, PerPhone records.
- For the existing users in EC, we need to compare their fields and create the necessary update payloads.
- The migration processor reuses all payload builders, field changes
  validation, error tracking, and notification systems from CoreProcessor.
- It extends CoreProcessor to specifically handle the migration of PDM Users.
- The functions need to be overridden from CoreProcessor are mainly:
    - _handle_employment: To handle Employment entities with the dummy position logic and the actual position.
    - _process_single_user: To handle the overall migration logic per user.
    - _build_update_payloads:
        1- To build the specific payloads:
        2- call _build_position_update func whatever the there's changes or not to ensure proper position treatment.
        3- call _handle_employment func whatever the there's changes or not to handle dummy position logic.
- New functions are added to handle migration-specific logic, such as:
    - _create_or_retrieve_dummy_positions: To create or retrieve dummy positions.
    - has_existing_empjob: To check if the user has existing EmpJob records.
    - resolve_position: Retrieve the position code from User Context
- Class Attributes to be overridden:
    - EXECUTION_PLAN: Defines the order of entity processing specific to migration and adding EmpInitLoadJob entity.
    - ENTITY_DEPENDENCIES: Defines dependencies between entities for migration and adding EmpInitLoadJob entity.
- CLass Instance to be overridden:
    - collected_payloads: To collect payloads for migration-specific entities.
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
from orchestrator.core_processing import CoreProcessor
from payload_builders.employment._employment import EmploymentPayloadBuilder
from payload_builders.position._position import PositionPayloadBuilder

from validator.employment.job_validator import JobExistenceValidator
from validator.position.position_validator import PositionValidator
from utils.date_converter import convert_to_unix_timestamp
import secrets
import pandas as pd

Logger = get_logger("offline_migration_processing")


class MigrationProcessorOffline(CoreProcessor):
    """
    Processor class to handle bulk migration of PDM Users.
    Extends CoreProcessor to implement migration-specific logic.
    job steps include:
    - Create or retrieve dummy positions per country/company.
    - Delete existing EmpJob records for users being migrated.
    - Create EmpJob records with INITLOAD event reason using dummy positions.
    - Create EmpJob records with DATACHG event reason to update to correct positions.
    - Reuse existing payload builders and validation from CoreProcessor.
    """

    EXECUTION_PLAN = [
        ("Position", "position"),
        ("PerPerson", "perperson"),
        ("EmpEmployment", "empemployment"),
        ("EmpInitLoadJob", "empinitloadjob"),
        ("EmpJob", "empjob"),
        ("UserRole", "userrole"),
        ("PerPersonal", "perpersonal"),
        ("PositionMatrixRelationships", "positionmatrixrelationships"),
        ("PerEmail", "peremail"),
        ("PerPhone", "perphone"),
        ("EmpJobRelationships", "empjobrelationships"),
    ]
    ENTITY_DEPENDENCIES = {
        "Position": [],
        "PerPerson": [],
        "PerPersonal": ["PerPerson"],
        "PerEmail": ["PerPerson"],
        "PerPhone": ["PerPerson"],
        "PositionMatrixRelationships": ["Position", "PerPerson"],
        "EmpEmployment": ["Position", "PerPerson"],
        "EmpInitLoadJob": ["Position", "PerPerson"],
        "EmpJob": ["Position", "PerPerson", "EmpInitLoadJob"],
        "EmpJobRelationships": ["Position", "PerPerson"],
        "UserRole": ["PerPerson"],
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
            "PositionMatrixRelationships": {},
            "EmpEmployment": {},
            "EmpInitLoadJob": {},
            "EmpJob": {},
            "EmpJobRelationships": {},
            "PerPersonal": {},
            "UserRole": {},
            "PerEmail": {},
            "PerPhone": {},
        }
        self.postgres_cache = PostgresDataCache()
        self.oracle_cache = OracleDataCache()
        self.sap_cache = SAPDataCache()
        self.employees_cache = EmployeesDataCache()
        self.positions_cache_key = positions_cache_key
        self.job_code = job_code
        self.hr_global_users = set(
            self.oracle_cache.get('pdm_data_df')[
                self.oracle_cache.get('pdm_data_df')['division'].str.lower() == 'human resources'
            ]['userid'].astype(str).str.lower()
        )
        self.sap_email_data = self.sap_cache.get('peremail_df')
    def _create_or_get_dummy_position(self, ctx: UserExecutionContext):
        """
        Mocking the creation or retrieval of a dummy position for the user.
        Return hard coded dummy position for demonstration."""
        try:
            if not self.job_code or pd.isna(self.job_code):
                raise ValueError("Jobcode is required to get or create dummy position.")
            row = ctx.runtime.get("original_row")
            if row is None:
                ctx.fail(
                    "Original row data not found in context for dummy position creation."
                )
                return None
            company = row.get("company")
            # get positions from cache
            positions_df = self.sap_cache.get(self.positions_cache_key)
            if positions_df is not None:
                filtered = positions_df[
                    (positions_df["company"] == company)
                    & (positions_df["jobcode"] == self.job_code)
                ]
                if not filtered.empty:
                    position_code = filtered.iloc[0]["code"]
                    ctx.dummy_position = position_code
                    return position_code
            return "DUMMY_POSITION_001"
        except Exception as e:
            return "DUMMY_POSITION_001"
         

    def _has_existing_empjob(
        self, user_id: str, ec_user_id: str, dummy_position: str
    ) -> bool:
        """
        Checks if the user has existing EmpJob records in EC cahce.

        Args:
            user_id (str): The user ID.
        Returns:
            bool: True if EmpJob records exist, False otherwise.
        """
        try:
            empjob_data = self.sap_cache.get("employees_df")
            if empjob_data is not None:
                mask = (
                    empjob_data["userid"].astype(str).str.lower().eq(ec_user_id.lower())
                )
                result = empjob_data[mask & (empjob_data["position"] != dummy_position)]

                return not result.empty
            return False
        except Exception as e:
            Logger.error(f"Error checking existing EmpJob for user {user_id}: {e}")
            return False

    def _process_single_user(
        self, row: pd.Series, ctx: UserExecutionContext, results: dict
    ):
        """
        Process a single user row to build payloads for position, person, employment, and relationships.
        Args:
            row (pd.Series): The data row for the user.
            ctx (UserExecutionContext): The execution context of the user.
            results (dict): A dictionary to store processing results.
        """
        try:
            user_id = ctx.user_id

            if pd.isna(user_id) or not str(user_id).strip():
                ctx.fail("Missing or null userid")
                return

            # Store original row for potential retry after Position cache refresh
            ctx.runtime["original_row"] = row

            # CREATE OR GET DUMMY POSITION AND ASSIGN TO USER CONTEXT TO BE USED IN EMPLOYMENT PROCESSING

            needs_hr_retry = bool(row.get("needs_hr_retry", False))
            Logger.info(f"Processing user {user_id}, needs_hr_retry={needs_hr_retry}")
            ctx.runtime["needs_hr_retry"] = needs_hr_retry
            dummy_position = self._create_or_get_dummy_position(ctx)
            ctx.dummy_position = dummy_position
            if not ctx.dummy_position:
                ctx.fail("Failed to create or retrieve dummy position")
                return

            # Store Ec USERID in context
            ctx.ec_user_id = get_userid_from_personid(user_id)

            # 0️ Check if user has existing EmpJob
            ctx.has_existing_empjob = self._has_existing_empjob(
                user_id, ctx.ec_user_id, ctx.dummy_position
            )

            # POSITION
            self._handle_position(row, ctx, results)
            if ctx.has_errors:
                return

            # PERSON
            self._handle_person(row, ctx)
            if ctx.has_errors:
                Logger.error(
                    f"Early return after person handling for {user_id} due to errors: {ctx.errors}"
                )
                return
            # User Role updates
            self._handle_ep_ec_roles(row, ctx)
            if ctx.has_errors:
                return

            # EMPLOYMENT
            self._handle_employment(row, ctx, results)
            if ctx.has_errors:
                return

            # POSITION MATRIX RELATIONSHIPS
            if ctx.runtime.get("needs_hr_retry", False):
                Logger.info(
                    f"HR retry needed for user {user_id}, skipping PositionMatrixRelationships & Relationships for now"
                )
                return  # Skip if HR retry is needed
            position_builder = ctx.builders.get("position")
            if position_builder:
                self._handle_position_matrix_relationship(row, ctx, position_builder)
                if ctx.has_errors:
                    return
            else:
                ctx.warn(
                    "Position builder not initialized for PositionMatrixRelationships"
                )

            # Skip relationships if waiting for position lookup
            if ctx.runtime.get("needs_position_lookup"):
                return

            # RELATIONSHIPS
            employment_builder = ctx.builders.get("employment")
            if not employment_builder:
                ctx.fail("Employment builder not initialized")
                return
            self._handle_relationships(row, ctx, employment_builder, results=results)
        except Exception as e:
            ctx.fail(f"Error processing user {ctx.user_id}: {e}")

    def _handle_position(
        self, row: pd.Series, ctx: UserExecutionContext, results: dict
    ):
        try:
            user_id = ctx.user_id

            position_validator = PositionValidator(
                record=row,
                pos_data=self.sap_cache.get("positions_df"),
                emp_data=self.sap_cache.get("employees_df"),
                user_id=user_id,
                ec_user_id=ctx.ec_user_id,
                results=results,
                required_fields=PositionPayloadBuilder.REQUIRED_FIELDS,
            )

            if not position_validator.validate_required_fields():
                missing = getattr(position_validator, "missing_fields", None)
                details = f" Missing fields: {', '.join(missing)}" if missing else ""
                ctx.fail(
                    f"Missing required fields for position creation for user {user_id}.{details}"
                )
                return

            # Check if position exists for this user (in employees table)
            existing_position_code_in_employees = (
                position_validator.position_code_exists_in_employees()
            )
            # Check if position exists for this user (in positions table)
            # existing_position_code_in_positions = (
            #     position_validator.position_code_exists_in_positions()
            # )

            # Get job mappings for builder creation
            job_validator = JobExistenceValidator(
                job_mappings=self.postgres_cache.get("jobs_titles_data_df"),
                job_code=row["jobcode"],
            )

            job_mapping = job_validator.get_job_mapping()
            if job_mapping.empty:
                ctx.fail(f"Job code {row['jobcode']} does not exist for user {user_id}")
                return

            # Always create position builder (needed for PositionMatrixRelationships)
            position_builder = PositionPayloadBuilder(
                record=row,
                job_mappings=job_mapping,
                is_scm=False,
                results=results,
                ec_user_id=ctx.ec_user_id,
            )
            ctx.builders["position"] = position_builder

            # Only create payload if position doesn't exist
            if (
                existing_position_code_in_employees
                # or existing_position_code_in_positions
            ):
                # Mark position as SUCCESS since it already exists (no action needed)
                ctx.runtime["entity_status"]["Position"] = "SUCCESS"

                # Store existing position code for employment processing
                ctx.position_code = (
                    existing_position_code_in_employees
                    # or existing_position_code_in_positions
                )
                # Don't return - continue processing employment and relationships
            else:
                # Position doesn't exist, create payload

                payload = position_builder.build_position()
                if not payload:
                    # include builder diagnostic if available
                    builder_err = getattr(position_builder, "last_error", None)
                    extra = f" Builder error: {builder_err}" if builder_err else ""
                    error_details = f"Failed to build position payload for user {user_id}. Check record fields: jobcode={row.get('jobcode')}, manager={row.get('manager')}, company={row.get('company')},cost_center={row.get('cost_center')},country_code={row.get('country_code')}.{extra}"
                    Logger.error(error_details)
                    ctx.fail(error_details)
                    return

                ctx.payloads["position"] = payload
                # Store that we're creating a position (employment handler will need to derive position code)
                # For mocking process in offline without calling EC APIs, we'll set a random value as position code in User Context
                if ctx.position_code is None:
                    ctx.position_code = f"POS_{secrets.token_hex(4).upper()}"
                ctx.runtime["position_being_created"] = True
                ctx.runtime["position_record"] = (
                    row  # Store row data for position lookup
                )
        except Exception as e:
            error_msg = f"Error building position payload for user {ctx.user_id}: {e}"
            Logger.error(error_msg, exc_info=True)
            ctx.fail(error_msg)

    def _handle_employment(
        self, row: pd.Series, ctx: UserExecutionContext, results: dict, is_update: bool = False
    ):
        """
        Build EmpEmployment and EmpJob payloads for the user.
        """
        try:
            user_id = ctx.user_id

            if is_update:
                for entity in ["EmpEmployment", "EmpJob"]:
                    # Check if Dependencies not in dirty entities, we put it as SUCCESS To proceed the retry after creating position
                    # and verify the function _can_execute_entity without blocking the user processing
                    # which check ctx.runtime["entity_status"].get(dep) == "SUCCESS" for dependencies, in this Case PerPerson, Position is dependency for Employment
                    deps = self.ENTITY_DEPENDENCIES.get(entity, [])
                    for dep in deps:
                        if dep not in ctx.dirty_entities:
                            ctx.runtime["entity_status"][dep] = "SUCCESS"
            # Validate dummy position
            dummy_position = ctx.dummy_position
            if not dummy_position:
                ctx.fail("Dummy position not set in context for employment processing")
                return

            # Build dummy INITLOAD EmpJob
            dummy_builder = self._build_employment_builder(
                row=row, user_id=user_id, position=dummy_position, seq_num=1
            )

            employment_payload = dummy_builder.build_empemployment_payload()
            if not employment_payload:
                ctx.fail(f"Failed to build employment payload for user {user_id}")
                return

            dummy_empjob_payload = dummy_builder.build_empjob_payload(
                migration_flag=True
            )
            if not dummy_empjob_payload:
                ctx.fail(f"Failed to build dummy empjob payload for user {user_id}")
                return

            # Determine actual position
            position = self._resolve_position(
                ctx=ctx,
            )

            if not position:
                ctx.builders["employment"] = dummy_builder
                ctx.payloads["empemployment"] = [employment_payload]
                ctx.runtime["needs_position_lookup"] = True
                return

            # Build actual EmpJob
            actual_builder = self._build_employment_builder(
                row=row,
                user_id=user_id,
                position=position,
                seq_num=2,
            )

            ctx.builders["employment"] = actual_builder

            actual_empjob_payload = actual_builder.build_empjob_payload(
                migration_flag=True
            )
            if not actual_empjob_payload:
                ctx.fail(f"Failed to build empjob payload for user {user_id}")
                return

            # Store both payloads
            ctx.payloads["empemployment"] = [employment_payload]
            ctx.payloads["empinitloadjob"] = [dummy_empjob_payload]
            ctx.payloads["empjob"] = [actual_empjob_payload]

            if actual_builder.calculated_start_date:
                ctx.empjob_start_date = actual_builder.calculated_start_date

        except Exception as e:
            ctx.fail(f"Error building employment payloads for user {ctx.user_id}: {e}")

    def _build_employment_builder(
        self, row: pd.Series, user_id: str, position: str, seq_num: int
    ) -> EmploymentPayloadBuilder:
        """
        Helper to build EmploymentPayloadBuilder instance.
        Args:
            row (pd.Series): The data row for the user.
            user_id (str): The user ID.
            position (str): The position code.
            seq_num (int): The sequence number for the job record.
        Returns:
            EmploymentPayloadBuilder: The built payload builder instance.
        """
        hire_date_raw = row.get("hiredate") or row.get("date_of_hire")
        start_of_employment_raw = row["start_of_employment"]

        manager_id = row.get("manager", "")
        if not manager_id or str(manager_id).strip().lower() in [
            "",
            "none",
            "no_manager",
        ]:
            manager_id = "NO_MANAGER"

        event_reason = "INITLOAD" if seq_num == 1 else "DATACHG"

        return EmploymentPayloadBuilder(
            user_id=user_id,
            person_id_external=user_id,
            hire_date=convert_to_unix_timestamp(hire_date_raw),
            start_of_employment=convert_to_unix_timestamp(start_of_employment_raw),
            seq_num=seq_num,
            company=row.get("company"),
            build_event_reason=event_reason,
            cost_center=row.get("cost_center"),
            position=position,
            manager_id=manager_id,
            manager_position_start_date=convert_to_unix_timestamp(
                row.get("manager_position_start_date")
            )
            if row.get("manager_position_start_date")
            else None,
        )

    def _resolve_position(self, ctx: UserExecutionContext) -> str:
        """
        Determine the correct position code for the EmpJob record.
        Args:
            row (pd.Series): The data row for the user.
            ctx (UserExecutionContext): The execution context of the user.
        Returns:
            str: The position code.
        """

        user_id = ctx.user_id

        if ctx.position_code:
            return ctx.position_code
        elif ctx.runtime.get("position_payload_built"):
            return None
        else:
            ctx.warn(
                f"Position code not found for user {user_id}, and no position payload built. Cannot resolve position."
            )
            return None

    def _build_update_payloads(
        self, row: pd.Series, ctx: UserExecutionContext, results: dict
    ):
        """
        Build payloads ONLY for entities that have dirty fields.
        This is different from new employee creation where ALL entities are built.

        - NB: For migration, we build update payloads for existing users.
         and we get the dummy position from the context for employment updates
         and reset the

        Args:
            row: User data row
            ctx: User execution context with dirty_entities set
        """
        user_id = ctx.user_id
        dirty_entities = ctx.dirty_entities
        ctx.ec_user_id = get_userid_from_personid(user_id)

        # Get or Create dummy position if needed
        dummy_position = self._create_or_get_dummy_position(ctx)
        if not dummy_position:
            ctx.fail(
                "Dummy position not set in context for employment processing (update phase)"
            )
            return
        ctx.dummy_position = dummy_position
        # Check if user has existing EmpJob
        ctx.has_existing_empjob = self._has_existing_empjob(
            user_id, ec_user_id=ctx.ec_user_id, dummy_position=dummy_position
        )

        # Position updates
        self._build_position_update(row, ctx, results, dummy_position=dummy_position)

        # Person-related updates (check any person entity is dirty)
        person_entities = {"PerPerson", "PerPersonal", "PerEmail", "PerPhone"}
        if person_entities & dirty_entities:
            self._build_person_updates(row, ctx, dirty_entities)

        # User Role updates
        if "UserRoles" in dirty_entities:
            self._handle_ep_ec_roles(row, ctx)
        # Employment updates
        # EMPLOYMENT
        self._handle_employment(row, ctx, results)
        if ctx.has_errors:
            return
        
        # Position relationship updates
        if "PositionMatrixRelationships" in dirty_entities:
            self._build_position_relationship_update(row, ctx, results)

        # RELATIONSHIPS
        employment_builder = ctx.builders.get("employment")
        if not employment_builder:
            ctx.fail("Employment builder not initialized")
            return
        self._handle_relationships(row, ctx, employment_builder, results=results)

    def _build_position_update(
        self,
        row: pd.Series,
        ctx: UserExecutionContext,
        results: dict,
        dummy_position: str,
    ):
        """
        Build Position payload for update (similar to _handle_position but update-specific).
        """
        try:
            user_id = ctx.user_id

            # Get existing position code from employees cache
            has_position = False
            employees_df = self.sap_cache.get("employees_df")
            # Check if user has existing position in employees cache
            if employees_df is not None and not employees_df.empty:
                # Retrieve personexternalid from get_userid_from_personid
                ec_user_id = ctx.ec_user_id
                emp_mask = (
                    employees_df["userid"]
                    .astype(str)
                    .str.lower()
                    .eq(ec_user_id.lower())
                )
                emp_result = employees_df[emp_mask]
                if not emp_result.empty:
                    existing_position = emp_result["position"].values[0]
                    if existing_position and existing_position != dummy_position:
                        has_position = True
                        ctx.position_code = existing_position

            # Validate required fields
            required_fields = [
                "jobcode",
                "address_code",
                "cost_center",
                "country_code",
                "company",
            ]
            for field in required_fields:
                if field not in row or pd.isna(row[field]):
                    ctx.fail(f"Missing required field for Position: {field}")
                    return

            # Get job mappings
            job_mappings = self.postgres_cache.get("jobs_titles_data_df")
            if job_mappings is None or job_mappings.empty:
                ctx.fail("Job mappings cache is empty")
                return

            job_code = str(row["jobcode"]).strip()
            job_match = job_mappings[job_mappings["jobcode"] == job_code]

            if job_match.empty:
                Logger.error(f"No job mapping found for jobcode {job_code}")
                ctx.fail(f"No job mapping found for jobcode {job_code}")
                return

            # Check if employee already has position with PositionValidator
            # position_validator = PositionValidator(
            #     record=row,
            #     pos_data=self.sap_cache.get("positions_df"),
            #     emp_data=self.sap_cache.get("employees_df"),
            #     user_id=user_id,
            #     ec_user_id=ctx.ec_user_id,
            #     results=results,
            #     required_fields=PositionPayloadBuilder.REQUIRED_FIELDS,
            # )
            # position_code = position_validator.position_code_exists_in_positions()
            # if position_code:
            #     has_position = True
            #     ctx.position_code = position_code
            # Build position payload with is_update=True
            position_builder = PositionPayloadBuilder(
                record=row.to_dict(),
                job_mappings=job_match,
                is_scm=row.get("is_scm_user", False),
                is_update=True if has_position else False,
                results=results,
                ec_user_id=ctx.ec_user_id,
            )
            if has_position:
                # Mark position as SUCCESS since it already exists (no action needed)
                ctx.runtime["entity_status"]["Position"] = "SUCCESS"
                return
            else:
                position_payload = position_builder.build_position(
                    dummy_position=dummy_position
                )
            if not position_payload:
                ctx.fail(f"Failed to build position update payload for user {user_id}")
                return
            if position_builder.position_code:
                if ctx.position_code != position_builder.position_code:
                    ctx.position_code = position_builder.position_code
            ctx.payloads["position"] = position_payload
            ctx.builders["position"] = position_builder
            ctx.runtime["position_payload_built"] = True
            # Mock processing - We'll set random position code as if created
            if not ctx.position_code:
                ctx.position_code = f"POS_{secrets.token_hex(4).upper()}"

        except Exception as e:
            ctx.fail(f"Error building position update for user {ctx.user_id}: {e}")
            Logger.error(f"Error building position update for user {ctx.user_id}: {e}")
    
    def _execute_batch_upserts(self, results, batch_user_ids, is_retry=False):
        """
        Execute batched upserts per entity (SAP-compliant).
        Refresh caches for Position and EmpJob after upserts.
        Args:
            results (Dict[str, UserExecutionContext]): Mapping of user_id to their execution context.
        """
        try:
            if is_retry:
                self._execute_hr_retry_upserts(results, batch_user_ids)
                return
            for entity_name, _ in self.EXECUTION_PLAN:
                payloads_per_user = self.collected_payloads.get(entity_name)
                if not payloads_per_user:
                    continue
                # Sync Position to Job after EmpJob success (triggers SAP PositionToJobInfoSyncRule)
                if entity_name == "EmpJob":
                    Logger.info("=" * 120)
                    Logger.info(
                        "POSITION-TO-JOB SYNC OPERATION (Not a Position creation/update)"
                    )
                    Logger.info("=" * 120)

                    position_sync_payloads = {}
                    Logger.info(f"Processing Position sync for users in batch: {batch_user_ids}")
                    for user_id, ctx in results.items():
                        # Only sync for users with successful Position AND EmpJob creation   
                        if user_id not in batch_user_ids:
                            Logger.info(f"User {user_id} not in current batch user IDs, skipping Position sync check.")
                            continue
                        # Force EmpJob status to success, but check first
                        if ctx.runtime.get("entity_status", {}).get("EmpJob") != "SUCCESS":
                            #Forcing EmpJob status to SUCCESS for Position sync, but logging the actual status for debugging
                            Logger.info(f"Forcing EmpJob status to SUCCESS for user {user_id} during Position sync. Actual status: {ctx.runtime.get('entity_status', {}).get('EmpJob')}")
                            ctx.runtime["entity_status"]["EmpJob"] = "SUCCESS"
                        Logger.info(f"Checking if Position sync is needed for user {user_id} after EmpJob upsert...")
                        if (
                            ctx.runtime.get("entity_status", {}).get("EmpJob")
                            == "SUCCESS"
                            and not ctx.has_errors
                            and ctx.position_code
                            and ctx.empjob_start_date
                        ):  
                            if ctx.payloads.get("position_sync") is not None:
                                continue
                            row = ctx.runtime.get("original_row")
                            position_builder = ctx.builders.get("position")

                            if position_builder is None:
                                Logger.warning(
                                    f"No position builder found in context for user {user_id} during Position sync. Attempting to build with original row data."
                                )
                                if row is None:
                                    Logger.error(
                                        f"No original row found for user {user_id} during Position sync. Cannot build position payload for sync."
                                    )
                                    ctx.fail(
                                        f"No original_row  and position_builder found for user {user_id} during Position sync"
                                    )
                                    continue
                                job_validator = JobExistenceValidator(
                                    job_mappings=self.postgres_cache.get(
                                        "jobs_titles_data_df"
                                    ),
                                    job_code=row["jobcode"],
                                )

                                job_mapping = job_validator.get_job_mapping()
                                if job_mapping.empty:
                                    Logger.error(
                                        f"Job code {row['jobcode']} does not exist for user {user_id} during Position sync"
                                    )
                                    ctx.fail(
                                        f"Job code {row['jobcode']} does not exist for user {user_id} during Position sync"
                                    )
                                    continue
                                position_builder = PositionPayloadBuilder(
                                    record=row,
                                    job_mappings=job_mapping,
                                    results=results,
                                    ec_user_id=ctx.ec_user_id,
                                )

                            Logger.info(f"Building Position sync payload for user {user_id} after EmpJob success using position builder: {position_builder}")

                            # Build Position payload with sync_pos_to_emp=True
                            sync_payload = position_builder.build_position(
                                sync_pos_to_emp=True,
                                effective_start_date_=ctx.empjob_start_date,
                                position_code_=ctx.position_code,
                            )

                            if sync_payload:
                                # Store in ctx.payloads for history tracking (payload_snapshot)
                                Logger.info(f"Built Position sync payload for user {user_id} after EmpJob success: {sync_payload}")
                                ctx.payloads["position_sync"] = sync_payload
                                position_sync_payloads[user_id] = [sync_payload]
                            else:
                                Logger.error(
                                        f"Failed to build Position sync payload for user {user_id} after EmpJob success. Check position builder state and original row data."
                                    )
                                ctx.fail(
                                    f"[POSITION SYNC] Failed to build sync payload for user {user_id}"
                                )
                        else:
                            Logger.info(f"No Position sync needed for user {user_id} after EmpJob upsert. Conditions not met: user in batch_user_ids={user_id in batch_user_ids}, EmpJob success={ctx.runtime.get('entity_status', {}).get('EmpJob') == 'SUCCESS'}, no errors={not ctx.has_errors}, position_code exists={bool(ctx.position_code)}, empjob_start_date exists={bool(ctx.empjob_start_date)}")

                for _, ctx in results.items():
                    if (
                        ctx.runtime.get("entity_status", {}).get(entity_name)
                        == "PENDING"
                    ):
                        ctx.runtime["entity_status"][entity_name] = "SUCCESS"

            # Log summary grouped by user
            self._log_batch_summary_by_user(results)

        except Exception as e:
            Logger.error(f"Fatal error during batch upserts: {e}")
            raise
    def _execute_hr_retry_upserts(self, results, batch_user_ids):
        """
        Execute retry upserts for users needing HR relationship fixes.
        Only processes PositionMatrixRelationships and EmpJobRelationships.
        """
        Logger.info("Executing HR retry upserts...")

        # First retry HR users relationship building
        self._retry_hr_users(results, batch_user_ids)