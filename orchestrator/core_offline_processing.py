from orchestrator.user_context import UserExecutionContext
from payload_builders.employment._employment import EmploymentPayloadBuilder
from payload_builders.person._person import PersonPayloadBuilder
from payload_builders.position._position import PositionPayloadBuilder
from api.api_client import APIClient
from api.auth_client import AuthAPI
from api.upsert_client import UpsertClient
from validator.employment.job_validator import JobExistenceValidator
from validator.employment.employment_validator import EmploymentExistenceValidator
from validator.position.position_validator import PositionValidator
from validator.person.person_validator import PersonValidator
from cache.postgres_cache import PostgresDataCache
from cache.oracle_cache import OracleDataCache
from cache.sap_cache import SAPDataCache
from cache.employees_cache import EmployeesDataCache
from utils.logger import get_logger
from mapper.retrieve_person_id_external import get_userid_from_personid
from orchestrator.core_processing import CoreProcessor

import pandas as pd
import secrets




Logger = get_logger("core_processor")


class CoreOfflineProcessor(CoreProcessor):
    """
    Orchestrates employee data synchronization between PDM (source) and SAP SuccessFactors (target).
    Handles both new employee creation and field-level updates for existing employees.

    Core Capabilities:
    1. New Employee Creation (process_batches_new_employees)
       - Processes ordered batches of new employees
       - Validates position, person, and employment data
       - Enforces entity dependencies (e.g., PerEmail requires PerPerson)
       - Builds and executes payloads in correct sequence

    2. Field-Level Updates (process_field_updates)
       - Detects dirty fields per user from change tracking
       - Builds update payloads only for modified entities
       - Handles specialized email operations (insert/delete/promote/demote)
       - Skips dependency checks since entities already exist in SAP

    Entity Processing Order (NEW EMPLOYEES):
    Position → PerPerson → EmpEmployment → EmpJob → Position (SYNC) → PerPersonal
    → PositionMatrixRelationships → PerEmail → PerPhone → EmpJobRelationships

    UPDATE Processing:
    - Only processes entities with detected changes based on dirty fields.
    - After detecting dirty fields, builds payloads only for those entities and follow the same order
      for just those entities.


    Dependencies:
    - AuthAPI: OAuth token management
    - APIClient: HTTP communication with SAP APIs
    - UpsertClient: Batched entity upsert operations
    - PostgresDataCache: Reference data (job codes, country mappings)
    - OracleDataCache: PDM source data
    - SAPDataCache: Existing SAP employee data
    - UserExecutionContext: Per-user processing state and error tracking


    Key Features:
    - Batch processing for performance optimization
    - Automatic cache refresh after Position/EmpJob upserts
    - Graceful error handling with context preservation
    - Retry logic for position-dependent payloads
    - Flexible email management with type conversions
    - Export of detailed processing results per user
    - Comprehensive logging for audit and debugging
    """

    EXECUTION_PLAN = [
        ("Position", "position"),
        ("PerPerson", "perperson"),
        ("EmpEmployment", "empemployment"),
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
        "EmpJob": ["Position", "PerPerson"],
        "EmpJobRelationships": ["Position", "PerPerson"],
        "UserRole": ["PerPerson"],
    }
    DIRTY_FIELD_TO_ENTITY = {
        # Position
        "jobcode": ["Position"],
        # Position & EmpJob
        "manager": ["Position", "EmpJob"],
        # PositionMatrixRelationships & EmpJobRelationships
        "matrix_manager": ["PositionMatrixRelationships", "EmpJobRelationships"],
        "hr": ["PositionMatrixRelationships", "EmpJobRelationships"],
        # Employment
        "start_of_employment": ["EmpEmployment"],
        "date_of_position": ["PerPersonal", "EmpJobRelationships"],
        "hiredate": ["EmpEmployment"],
        # Person
        "date_of_birth": ["PerPerson"],
        "firstname": ["PerPersonal"],
        "lastname": ["PerPersonal"],
        "mi": ["PerPersonal"],
        "nickname": ["PerPersonal"],
        "gender": ["PerPersonal"],
        # Email
        "email": ["PerEmail"],
        "private_email": ["PerEmail"],
        # Phone
        "biz_phone": ["PerPhone"],
        "custom_string_8": ["UserRole"],
    }

    def __init__(
        self,
        auth_url: str,
        base_url: str,
        auth_credentials: dict,
        ordered_batches: list[pd.DataFrame],
        batches_summary: dict,
        max_retries: int = 5,
    ):
        self.ordered_batches = ordered_batches
        self.batches_summary = batches_summary
        self.auth_credentials = auth_credentials
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
            "EmpJob": {},
            "EmpJobRelationships": {},
            "PerPersonal": {},
            "PerEmail": {},
            "PerPhone": {},
            "UserRole": {},
        }
        self.postgres_cache = PostgresDataCache()
        self.oracle_cache = OracleDataCache()
        self.sap_cache = SAPDataCache()
        self.employees_cache = EmployeesDataCache()
        self.hr_global_users = set(
            self.oracle_cache.get('pdm_data_df')[
                self.oracle_cache.get('pdm_data_df')['division'].str.lower() == 'human resources'
            ]['userid'].astype(str).str.lower()
        )
        self.sap_email_data = self.sap_cache.get('peremail_df')

    def _process_single_user(
        self, row: pd.Series, ctx: UserExecutionContext, results: dict
    ):
        """
        Process a single user row to build payloads for position, person, employment, and relationships.
        Args:
            row (pd.Series): The data row for the user.
            ctx (UserExecutionContext): The execution context of the user.
        """
        try:
            user_id = ctx.user_id
            if pd.isna(user_id) or not str(user_id).strip():
                ctx.fail("Missing or null userid")
                return

            # Store original row for potential retry after Position cache refresh
            ctx.runtime["original_row"] = row

            # 0 Store EC user ID if exists
            ctx.ec_user_id = get_userid_from_personid(person_id=user_id)

            # 1️ POSITION
            self._handle_position(row, ctx, results)
            if ctx.has_errors:
                return

            # 2️ PERSON
            self._handle_person(row, ctx)
            if ctx.has_errors:
                return

            # 3️ EMPLOYMENT
            self._handle_employment(row, ctx, results)
            if ctx.has_errors:
                return
            # 4 User Role
            self._handle_ep_ec_roles(row, ctx)
            if ctx.has_errors:
                ctx.warn("Errors encountered during UserRole handling")
            # 5 POSITION MATRIX RELATIONSHIPS
            position_builder = ctx.builders.get("position")
            if position_builder:
                self._handle_position_matrix_relationship(row, ctx, position_builder)
                if ctx.has_errors:
                    return
            else:
                ctx.warn(
                    "Position builder not initialized for PositionMatrixRelationships"
                )

            # Skip relationships if waiting for position_code
            if ctx.runtime.get("needs_position_lookup"):
                return

            # 6 RELATIONSHIPS
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
            existing_position_code_in_positions = (
                position_validator.position_code_exists_in_positions()
            )

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
                or existing_position_code_in_positions
            ):
                # Mark position as SUCCESS since it already exists (no action needed)
                ctx.runtime["entity_status"]["Position"] = "SUCCESS"

                # Store existing position code for employment processing
                ctx.position_code = (
                    existing_position_code_in_employees
                    or existing_position_code_in_positions
                )
            else:
                # Position doesn't exist, create payload
                payload = position_builder.build_position()
                if not payload:
                    # include builder diagnostic if available
                    builder_err = getattr(position_builder, "last_error", None)
                    extra = f"Builder error: {builder_err}" if builder_err else ""
                    error_details = f"Failed to build position payload for user {user_id}. Check record fields: jobcode={row.get('jobcode')}, manager={row.get('manager')}, company={row.get('company')},cost_center={row.get('cost_center')},country_code={row.get('country_code')}.{extra}"
                    ctx.fail(error_details)
                    return

                ctx.payloads["position"] = payload
                # Store that we're creating a position (employment handler will need to derive position code)
                # For mocking process in offline without calling EC APIs, we'll set a random value as position code in User Context
                ctx.position_code = f"POS_{secrets.token_hex(4).upper()}"
                ctx.runtime["position_being_created"] = True
                ctx.runtime["position_record"] = (
                    row  # Store row data for position lookup
                )
        except Exception as e:
            error_msg = f"Error building position payload for user {ctx.user_id}: {e}"
            ctx.fail(error_msg)

    def _build_update_payloads(
        self, row: pd.Series, ctx: UserExecutionContext, results: dict
    ):
        """
        Build payloads ONLY for entities that have dirty fields.
        This is different from new employee creation where ALL entities are built.

        Args:
            row: User data row
            ctx: User execution context with dirty_entities set
            results: Dictionary to store results
        """
        user_id = ctx.user_id
        dirty_entities = ctx.dirty_entities

        # Store EC user ID if exists
        ctx.ec_user_id = get_userid_from_personid(person_id=user_id)

        # Collect position_code and last start date from EmpJob
        position_code, start_date = (
            self._collect_position_code_effective_date_for_updates(
                user_id=user_id, ctx=ctx
            )
        )
        if position_code and start_date:
            ctx.position_code = position_code
            ctx.empjob_start_date = start_date

        # Position updates
        if "Position" in dirty_entities:
            self._build_position_update(row, ctx, results)

        # Person-related updates (check any person entity is dirty)
        person_entities = {"PerPerson", "PerPersonal", "PerEmail", "PerPhone"}
        if person_entities & dirty_entities:
            self._build_person_updates(row, ctx, dirty_entities)

        # Employment updates
        employment_entities = {"EmpEmployment", "EmpJob", "EmpJobRelationships"}
        if employment_entities & dirty_entities:
            self._build_employment_updates(row, ctx, dirty_entities, results)

        # Position relationship updates
        if "PositionMatrixRelationships" in dirty_entities:
            self._build_position_relationship_update(row, ctx, results)

        if "UserRole" in dirty_entities:
            self._handle_ep_ec_roles(row, ctx)

    def _build_position_update(
        self, row: pd.Series, ctx: UserExecutionContext, results: dict
    ):
        """
        Build Position payload for update (similar to _handle_position but update-specific).
        """
        try:
            user_id = ctx.user_id
            has_position = False

            # Get existing position code from employees cache
            employees_df = self.sap_cache.get("employees_df")
            if employees_df is not None and not employees_df.empty:
                # Retrieve personexternalid from get_userid_from_personid
                ec_user_id = ctx.ec_user_id
                # Put Mask to retrieve position for the given ec_user_id,and filter by jobcode != "T00001"
                emp_mask = (
                    employees_df["userid"]
                    .astype(str)
                    .str.lower()
                    .eq(ec_user_id.lower())
                ) & (employees_df["jobcode"] != "T00001")

                emp_result = employees_df[emp_mask]
                if not emp_result.empty:
                    existing_position = emp_result["position"].values[0]

                    if existing_position:
                        ctx.position_code = existing_position
                        has_position = True
                    else:
                        ctx.warn(
                            f"No position found in employee record for user {user_id} with ec_user_id {ec_user_id}"
                        )
                else:
                    ctx.warn(
                        f"No employee record found for user with pdm id: {user_id} and ec user id: {ec_user_id} in employees cache"
                    )

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
                ctx.fail(f"No job mapping found for jobcode {job_code}")
                return
            position_validator = PositionValidator(
                record=row,
                pos_data=self.sap_cache.get("positions_df"),
                emp_data=self.sap_cache.get("employees_df"),
                user_id=user_id,
                ec_user_id=ctx.ec_user_id,
                results=results,
                required_fields=PositionPayloadBuilder.REQUIRED_FIELDS,
            )
            position_code = position_validator.position_code_exists_in_positions()
            if position_code:
                has_position = True
                ctx.position_code = position_code
            # Build position payload with is_update=True
            position_builder = PositionPayloadBuilder(
                record=row.to_dict(),
                job_mappings=job_match,
                is_scm=row.get("is_scm_user", False),
                is_update=has_position,  # Key difference for updates
                results=results,
                ec_user_id=ctx.ec_user_id,
            )
            # Below not the same logic as migration, because we don't have any dummy positions here
            # and if _build_position_update won't be triggred unless there are dirty fields for Position and EmpJob not in dirty entities
            # because if EmpJob is in dirty entities, Position will be triggred after EmpJob update success to avoid conflicts.
            # For this we update existing position if found, else create new position
            # NB: If EmpJob is a part of the update, The position entity will be called after a SUCCESS EmpJob update
            # in order to trigger the PositionToJobInfoSyncRule in SAP.
            if has_position:
                # Update Position if EmpJob not in dirty entities
                if "EmpJob" not in ctx.dirty_entities:
                    position_payload = position_builder.build_position(
                        position_code_=ctx.position_code
                    )
            else:
                # If position Does NOT exists, Creating a new position with the provided details
                position_payload = position_builder.build_position()
            if not position_payload:
                ctx.fail(f"Failed to build position update payload for user {user_id}")
                Logger.error(f"Position update payload build failed for user {user_id}")
                return
            if position_builder.position_code:
                if ctx.position_code != position_builder.position_code:
                    ctx.position_code = position_builder.position_code

            ctx.payloads["position"] = position_payload
            ctx.builders["position"] = position_builder
            if not ctx.position_code:
                ctx.position_code = f"POS_{secrets.token_hex(4).upper()}"
        except Exception as e:
            ctx.fail(f"Error building position update for user {ctx.user_id}: {e}")
    
    def _execute_batch_upserts(self, results, batch_user_ids):
        """
        Execute batched upserts per entity (SAP-compliant).
        Refresh caches for Position and EmpJob after upserts.
        Args:
            results (Dict[str, UserExecutionContext]): Mapping of user_id to their execution context.
        """
        try:
            for entity_name, _ in self.EXECUTION_PLAN:
                payloads_per_user = self.collected_payloads.get(entity_name)
                Logger.info(
                    f"Processing upsert for entity: {entity_name} with {len(payloads_per_user) if payloads_per_user else 0} users"
                )
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
                    for user_id, ctx in results.items():
                        # Only sync for users with successful Position AND EmpJob creation
                        if (
                            user_id in batch_user_ids
                            and ctx.runtime.get("entity_status", {}).get("EmpJob")
                            == "SUCCESS"
                            and not ctx.has_errors
                            and ctx.position_code
                            and ctx.empjob_start_date
                        ):
                            row = ctx.runtime.get("original_row")
                            position_builder = ctx.builders.get("position")

                            if position_builder is None:
                                if row is None:
                                    ctx.warn(
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

                            # Build Position payload with sync_pos_to_emp=True
                            sync_payload = position_builder.build_position(
                                sync_pos_to_emp=True,
                                effective_start_date_=ctx.empjob_start_date,
                                position_code_=ctx.position_code,
                            )

                            if sync_payload:
                                # Store in ctx.payloads for history tracking (payload_snapshot)
                                ctx.payloads["position_sync"] = sync_payload
                                position_sync_payloads[user_id] = [sync_payload]
                            else:
                                ctx.warn(
                                    f"[POSITION SYNC] Failed to build sync payload for user {user_id}"
                                )

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