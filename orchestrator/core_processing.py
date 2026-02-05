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
from utils.date_converter import convert_to_unix_timestamp
from mapper.retrieve_person_id_external import get_userid_from_personid
from planning.convert_pdm_data import convert_pdm_data
from planning.email_resolver import EmailResolver
from payload_builders.user._user import build_user_role_payload
import pandas as pd


Logger = get_logger("core_processor")


class CoreProcessor:
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
            self.oracle_cache.get("pdm_data_df")[
                self.oracle_cache.get("pdm_data_df")["division"].str.lower()
                == "human resources"
            ]["userid"]
            .astype(str)
            .str.lower()
        )
        self.sap_email_data = self.sap_cache.get("peremail_df")

    def process_batches_new_employees(self):
        """
        Process new employee batches:
        1. Get batches of new employees.
        2. For each batch:
            - Process each user and build payloads.
            - Collect payloads for batch upsert.
            - Execute batched upserts per entity.
        Returns:
            Dict[str, UserExecutionContext]: Mapping of user_id to their execution context after processing.
        """
        try:
            batches = self.ordered_batches
            results = {}

            for i, batch_df in enumerate(batches, start=1):
                Logger.info(f"Processing batch {i} with {len(batch_df)} employees")
                batch_user_ids = set()  # To track user_ids in the current batch
                # Process each user and collect payloads
                for _, row in batch_df.iterrows():
                    user_id = row.get("userid", "Unknown")
                    if user_id in results:
                        Logger.info(
                            f"User {user_id} already processed in a previous batch, skipping."
                        )
                        continue
                    ctx = UserExecutionContext(user_id)
                    ctx.is_update = False
                    ctx.is_scm = (
                        row.get("is_peoplehub_scm_manually_included", "N") == "Y"
                    )
                    ctx.is_im = row.get("is_peoplehub_im_manually_included", "N") == "Y"
                    ctx.runtime["entity_status"] = {
                        entity: "PENDING" for entity, _ in self.EXECUTION_PLAN
                    }
                    try:
                        self._process_single_user(row, ctx, results)
                    except Exception as e:
                        Logger.error(f"Fatal error for user {user_id}: {e}")
                        ctx.fail(str(e))

                    results[user_id] = ctx
                    batch_user_ids.add(user_id)
                    self._collect_payloads(ctx)

                # Execute batched upserts per entity for this batch
                self._execute_batch_upserts(
                    results=results, batch_user_ids=batch_user_ids
                )

                # Reset collected payloads for next batch
                self._reset_collected_payloads()

            # Retry logic for users needing HR
            users_needing_hr_retry = {
                user_id: ctx
                for user_id, ctx in results.items()
                if ctx.runtime.get("needs_hr_retry", False)
            }
            self._execute_batch_upserts(
                results=users_needing_hr_retry,
                batch_user_ids=set(users_needing_hr_retry.keys()),
                is_retry=True,
            )

            return results
        except Exception as e:
            Logger.error(f"Fatal error during batch processing: {e}")
            raise

    def process_field_updates(self, field_changes_df: pd.DataFrame):
        """
        Process updates for existing employees based on dirty fields.

        Steps:
        1. Extract dirty entities per user from field changes.
        2. Extract users to process from PDM cache.
        3. For each user:
            - Create execution context.
            - Process only dirty entities.
            - Build payloads for dirty entities.
            - Collect payloads for batch upsert.
        4. Execute batched upserts per entity.
        Returns:
            Dict[str, UserExecutionContext]: Mapping of user_id to their execution context after processing.
        """
        Logger.info(
            f"Starting field updates processing for {len(field_changes_df)} changes"
        )

        dirty_entities_map = self._extract_dirty_entities(field_changes_df)
        Logger.info(f"Identified {len(dirty_entities_map)} users with dirty entities")

        results = {}
        self._reset_collected_payloads()

        # get user ids from dirty_entities_map
        batch_user_ids = set(dirty_entities_map.keys())

        dirty_user_ids = list(dirty_entities_map.keys())
        pdm_data_df = self.oracle_cache.get("pdm_data_df")
        if pdm_data_df is None or pdm_data_df.empty:
            Logger.error("PDM data cache is empty or not loaded")
            return results
        users_to_process_df = pdm_data_df[
            pdm_data_df["userid"]
            .astype(str)
            .str.lower()
            .isin([str(uid).lower() for uid in dirty_user_ids])
        ]
        # Convert dates and country codes for the row
        users_to_process_df = convert_pdm_data(users_to_process_df)

        if users_to_process_df is None or users_to_process_df.empty:
            Logger.error("No users found in PDM cache after filtering")
            return results

        # Logging users not found in PDM cache
        found_user_ids = users_to_process_df["userid"].astype(str).str.lower().tolist()
        for uid in dirty_user_ids:
            if str(uid).lower() not in found_user_ids:
                Logger.error(
                    f"User {uid} not found in PDM cache for updates processing"
                )
        Logger.info(
            f"Number of users found in PDM cache for updates processing: {len(found_user_ids)} out of {len(dirty_user_ids)}"
        )

        for user_id, entities_info in dirty_entities_map.items():
            if user_id in results:
                Logger.info(
                    f"User {user_id} already processed in a previous batch, skipping."
                )
                continue
            ctx = UserExecutionContext(user_id)
            ctx.is_update = True
            ctx.dirty_entities = entities_info.get("entities", set())

            # Store email actions separately for special handling
            ctx.runtime["email_actions"] = entities_info.get("email_actions", [])
            ctx.runtime["entity_status"] = {
                entity: "PENDING" for entity in ctx.dirty_entities
            }

            # Find user in PDM data
            user_mask = (
                users_to_process_df["userid"].astype(str).str.lower()
                == str(user_id).lower()
            )
            user_rows = users_to_process_df[user_mask]

            if user_rows.empty:
                ctx.fail(f"No data found for user {user_id} in PDM cache")
                results[user_id] = ctx
                continue

            row = user_rows.iloc[0]
            ctx.is_scm = row.get("is_peoplehub_scm_manually_included", "N") == "Y"
            ctx.is_im = row.get("is_peoplehub_im_manually_included", "N") == "Y"

            ctx.runtime["original_row"] = row

            try:
                Logger.info(
                    f"Processing updates for user {user_id}, dirty entities: {ctx.dirty_entities}"
                )

                # Build ONLY dirty entity payloads
                self._build_update_payloads(row, ctx, results)

                if not ctx.has_errors:
                    # Collect payloads for batch upsert
                    batch_user_ids.add(user_id)
                    self._collect_payloads(ctx)
                    results[user_id] = ctx
                else:
                    Logger.error(
                        f"Errors building payloads for user {user_id}: {ctx.errors}"
                    )
                    results[user_id] = ctx

            except Exception as e:
                ctx.fail(f"Error processing updates for {user_id}: {e}")
                results[user_id] = ctx

        # Execute batched upserts per entity
        Logger.info(f"Executing batch upserts for {len(results)} users")
        self._execute_batch_upserts(results=results, batch_user_ids=batch_user_ids)

        return results

    def _can_execute_entity(self, ctx: UserExecutionContext, entity_name: str) -> bool:
        """
        Check if all dependencies for the given entity have been successfully processed.
        """
        deps = self.ENTITY_DEPENDENCIES.get(entity_name, [])
        for dep in deps:
            dep_status = ctx.runtime["entity_status"].get(dep)
            if dep_status != "SUCCESS":
                Logger.info(
                    f"Dependency check for {entity_name}: {dep} status is {dep_status}, not SUCCESS"
                )
                return False
        return True

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
            needs_hr_retry = bool(row.get("needs_hr_retry", False))
            Logger.info(f"Processing user {user_id}, needs_hr_retry={needs_hr_retry}")
            ctx.runtime["needs_hr_retry"] = needs_hr_retry

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
                Logger.error(
                    f"Early return after person handling for {user_id} due to errors: {ctx.errors}"
                )
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
            if ctx.runtime.get("needs_hr_retry", False):
                Logger.info(
                    f"HR retry needed for user {user_id}, skipping PositionMatrixRelationships & Relationships for now"
                )
                return  # Skip if HR retry is needed
            if position_builder:
                self._handle_position_matrix_relationship(row, ctx, position_builder)
                if ctx.has_errors:
                    Logger.error(
                        f"Early return after position matrix for {user_id} due to errors: {ctx.errors}"
                    )
                    return
            else:
                ctx.warn(
                    "Position builder not initialized for PositionMatrixRelationships"
                )
                Logger.warning(
                    f"No Position builder for user {user_id} (position already exists), skipping PositionMatrixRelationships"
                )

            # Skip relationships if waiting for position_code or if HR retry is needed
            if ctx.runtime.get("needs_position_lookup") or ctx.runtime.get(
                "needs_hr_retry", False
            ):
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
        """
        Build Position payload for the user.
        Steps:
        1. Validate required fields.
        2. Check if position exists (user already has position assigned).
        3. Skip if position exists (don't update existing positions for new employees).
        4. If position doesn't exist, validate job code and build position payload.
        """
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
                Logger.info(
                    f"Position {existing_position_code_in_employees or existing_position_code_in_positions} already exists for user {user_id}. Position builder created but skipping payload creation."
                )
                # Mark position as SUCCESS since it already exists (no action needed)
                ctx.runtime["entity_status"]["Position"] = "SUCCESS"

                # Store existing position code for employment processing
                ctx.position_code = (
                    existing_position_code_in_employees
                    or existing_position_code_in_positions
                )
                # Don't return - continue processing employment and relationships
            else:
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
                ctx.runtime["position_being_created"] = True
                ctx.runtime["position_record"] = (
                    row  # Store row data for position lookup
                )
        except Exception as e:
            error_msg = f"Error building position payload for user {ctx.user_id}: {e}"
            Logger.error(error_msg, exc_info=True)
            ctx.fail(error_msg)

    def _handle_person(self, row: pd.Series, ctx: UserExecutionContext):
        """
        Build PerPerson, PerPersonal, PerEmail, and PerPhone payloads for the user.
        Steps:
        1. Validate required fields.
        2. Check if personIdExternal already exists.
        3. Build payloads using PersonPayloadBuilder.
        4. Add payloads to context.
        """
        try:
            user_id = ctx.user_id

            email_resolver = EmailResolver()
            resolved = email_resolver.resolve_user_email(
                userid=user_id,
                pdm_row=row,
                hr_global_users=self.hr_global_users,
                sap_email_data=self.sap_email_data,
            )
            safe_row = row.copy()
            safe_row["email"] = resolved["business_email"]
            safe_row["private_email"] = resolved["private_email"]
            ctx.runtime["original_row"] = safe_row
            row = safe_row
            Logger.info(f"Emails resolved for user {user_id}")
            person_validator = PersonValidator(
                record=row,
                required_fields=[
                    "firstname",
                    "lastname",
                    "userid",
                    "date_of_birth",
                    "date_of_position",
                    "email",
                ],
                person_df=self.sap_cache.get("perperson_df"),
            )

            if not person_validator.validate_required_fields():
                missing = getattr(person_validator, "missing_fields", None)
                details = f" Missing fields: {', '.join(missing)}" if missing else ""
                ctx.fail(
                    f"Missing required fields for person creation for user {user_id}.{details}"
                )
                return

            if person_validator.personid_exists():
                ctx.warn(f"personIdExternal {user_id} already exists")

            fields_to_check = [
                "firstname",
                "lastname",
                "date_of_birth",
                "date_of_position",
                "email",
                "private_email",
                "gender",
                "phone",
            ]
            # Check for changes to avoid unnecessary payload building
            # Only skip if person already exists AND no changes detected
            if (
                person_validator.personid_exists()
                and not person_validator.check_changes(fields_to_check)
            ):
                Logger.info(
                    f"No changes detected for person {user_id}, skipping payload building"
                )
                # Mark person as SUCCESS since it already exists and is up-to-date
                ctx.runtime["entity_status"]["PerPerson"] = "SUCCESS"
                return

            Logger.info(f"Building person payloads for user {user_id}")

            employment_start_date = row.get("date_of_position")

            person_builder = PersonPayloadBuilder(
                first_name=row["firstname"],
                last_name=row["lastname"],
                person_id_external=user_id,
                date_of_birth=row["date_of_birth"],
                start_date=employment_start_date,
                email=row["email"],
                nickname=row.get("nickname", None),
                middle_name=row.get("mi", None),
                private_email=row.get("private_email", None),
                gender=row.get("gender", None),
                phone=row.get("phone", None),
                postgres_cache=self.postgres_cache,
            )
            # PerPerson payload
            perperson_payload = person_builder.build_perperson_payload()
            if not perperson_payload:
                err = getattr(person_builder, "last_error", None)
                extra = f" Builder error: {err}" if err else ""
                ctx.fail(
                    f"Failed to build perperson payload for user {user_id}.{extra}"
                )
                return
            ctx.payloads["perperson"] = perperson_payload

            # PerPersonal payload
            perpersonal_payload = person_builder.build_perpersonal_payload()
            if not perpersonal_payload:
                err = getattr(person_builder, "last_error", None)
                extra = f" Builder error: {err}" if err else ""
                ctx.fail(
                    f"Failed to build perpersonal payload for user {user_id}.{extra}"
                )
                return
            ctx.payloads["perpersonal"] = perpersonal_payload

            # PerEmail payload (Business Email)

            business_email = row.get("email")
            private_email = row.get("private_email")

            # Business email is mandatory
            if (
                business_email is None
                or pd.isna(business_email)
                or str(business_email).strip() == ""
            ):
                ctx.warn(
                    f"Email is missing for user {user_id}, skipping PerEmail payload"
                )
                return

            business_email_norm = str(business_email).strip().lower()

            # Always store peremail as a list (consistent)
            peremail_payloads = []

            # ---- Build business email payload
            business_payload = person_builder.build_peremail_payload(
                is_business_email=True
            )
            if not business_payload:
                err = getattr(person_builder, "last_error", None)
                extra = f" Builder error: {err}" if err else ""
                ctx.fail(
                    f"Failed to build PerEmail (business) payload for user {user_id}.{extra}"
                )
                return

            peremail_payloads.append(business_payload)

            # ---- Private email payload (optional)
            if (
                private_email is None
                or pd.isna(private_email)
                or str(private_email).strip() == ""
            ):
                pass
            else:
                private_email_norm = str(private_email).strip().lower()

                # Only create private email if different from business
                if private_email_norm != business_email_norm:
                    private_payload = person_builder.build_peremail_payload(
                        is_business_email=False
                    )

                    if not private_payload:
                        err = getattr(person_builder, "last_error", None)
                        extra = f" Builder error: {err}" if err else ""
                        ctx.warn(
                            f"Failed to build PerEmail (private) payload for user {user_id}.{extra}"
                        )
                    else:
                        peremail_payloads.append(private_payload)

            # Save only if we have at least 1 payload
            ctx.payloads["peremail"] = peremail_payloads
            # PerPhone payload
            if row.get("phone") is None or pd.isna(row.get("phone")):
                Logger.info(
                    f"Phone is missing for user {user_id}, skipping PerPhone payload"
                )
            else:
                perphone_payload = person_builder.build_perphone_payload()
                if not perphone_payload:
                    err = getattr(person_builder, "last_error", None)
                    extra = f" Builder error: {err}" if err else ""
                    ctx.fail(
                        f"Failed to build perphone payload for user {user_id}.{extra}"
                    )
                    return
                ctx.payloads["perphone"] = perphone_payload
        except Exception as e:
            ctx.fail(f"Error building person payloads for user {ctx.user_id}: {e}")

    def _handle_position_matrix_relationship(
        self,
        row: pd.Series,
        ctx: UserExecutionContext,
        position_builder: PositionPayloadBuilder,
    ):
        """
        Build PositionMatrixRelationships payload for the user.
        Steps:
        1. Check for Matrix Manager and HR fields.
        2. Build PositionMatrixRelationships payloads if fields are present.
        3. Add payloads to context.
        """
        try:
            user_id = ctx.user_id
            matrixRelatedPersonId = (
                row.get("matrix_manager", "").strip()
                if pd.notna(row.get("matrix_manager"))
                else ""
            )
            hrRelatedPersonId = (
                row.get("hr", "").strip() if pd.notna(row.get("hr")) else ""
            )
            if matrixRelatedPersonId not in ["", "NO_HR", "NO_MANAGER"]:
                position_matrix_payload = (
                    position_builder.build_position_matrix_relationship_payload(
                        relation_userid=matrixRelatedPersonId,
                        relation_type="matrix manager",
                        user_position_code_=ctx.position_code,
                    )
                )
                if position_matrix_payload:
                    ctx.payloads["positionmatrixrelationships"] = (
                        position_matrix_payload
                    )
                else:
                    Logger.warning(
                        f"Position Tech Manager relationship payload failed for user {user_id}, continuing anyway"
                    )
            if hrRelatedPersonId not in ["", "NO_HR", "NO_MANAGER"]:
                position_matrix_payload = (
                    position_builder.build_position_matrix_relationship_payload(
                        relation_userid=hrRelatedPersonId,
                        relation_type="hr manager",
                        user_position_code_=ctx.position_code,
                    )
                )
                if position_matrix_payload:
                    # If both relationships exist, store as a list
                    if "positionmatrixrelationships" in ctx.payloads:
                        existing_payload = ctx.payloads["positionmatrixrelationships"]
                        if isinstance(existing_payload, list):
                            existing_payload.append(position_matrix_payload)
                        else:
                            ctx.payloads["positionmatrixrelationships"] = [
                                existing_payload,
                                position_matrix_payload,
                            ]
                    else:
                        ctx.payloads["positionmatrixrelationships"] = (
                            position_matrix_payload
                        )
                else:
                    Logger.warning(
                        f"Position HR relationship payload failed for user {user_id}, continuing anyway"
                    )
        except Exception as e:
            Logger.warning(
                f"Error building PositionMatrixRelationships payload for user {user_id}: {e} - continuing processing"
            )

    def _handle_employment(
        self, row: pd.Series, ctx: UserExecutionContext, results: dict
    ):
        """
        Build EmpEmployment and EmpJob payloads for the user.
        Steps:
        1. Check if employment exists to determine seqnumber.
        2. Check if position exists or is being created.
        3. Build employment and job payloads.
        """
        try:
            user_id = ctx.user_id
            employment_validator = EmploymentExistenceValidator(
                user_id=user_id, ec_user_id=ctx.ec_user_id, results=results
            )
            emp_mapping = employment_validator.get_job_mapping()
            position, seq_num, start_date = None, None, None

            if not emp_mapping.empty:
                # User already has employment record in SAP
                position = emp_mapping.iloc[0]["position"]
                seq_num = emp_mapping.iloc[0]["seqnumber"]
                start_date = emp_mapping.iloc[0]["startdate"]
                seq_num = int(seq_num) + 1  # Increment for new job record
            else:
                seq_num = 1
                # Check if we're creating a new position in this batch
                if ctx.runtime.get("position_being_created"):
                    # Use position_code from context (set after Position upsert success)
                    position = ctx.position_code
                    if not position:
                        # Position not yet created, mark for retry after Position upsert
                        ctx.runtime["needs_position_lookup"] = True
                        return
                else:
                    position = ctx.position_code
                    if not position:
                        # User has no position and we didn't create one - this is an error
                        position = (
                            employment_validator.position_code_exists_in_employees()
                        )
                        if not position:
                            ctx.fail(
                                f"Cannot determine position code for employment creation for user {user_id}"
                            )
                            return

            try:
                seq_num = int(seq_num)
            except (ValueError, TypeError):
                Logger.warning(
                    f"Could not convert seq_num to int for user {user_id}, defaulting to 1"
                )
                seq_num = 1

            hire_date_raw = row.get("hiredate") or row.get("date_of_hire")
            start_of_employment_raw = row["start_of_employment"]

            # Get manager ID from the row (could be 'manager' or other field)
            manager_id = row.get("manager", "")
            if not manager_id or str(manager_id).strip().lower() in [
                "",
                "none",
                "no_manager",
            ]:
                manager_id = "NO_MANAGER"

            # Use INITLOAD for first job record (seqNumber=1), DATACHG for subsequent records
            event_reason = "INITLOAD" if seq_num == 1 else "DATACHG"

            employment_builder = EmploymentPayloadBuilder(
                user_id=user_id,
                person_id_external=user_id,
                hire_date=convert_to_unix_timestamp(hire_date_raw),
                start_of_employment=convert_to_unix_timestamp(start_of_employment_raw),
                seq_num=seq_num,
                company=row.get("companycode") or row.get("company"),
                build_event_reason=event_reason,
                cost_center=row.get("costcenter") or row.get("cost_center"),
                position=position,
                manager_id=manager_id,
                start_date=start_date,
                manager_position_start_date=convert_to_unix_timestamp(
                    row.get("manager_position_start_date")
                )
                if row.get("manager_position_start_date")
                else None,
            )
            ctx.builders["employment"] = employment_builder
            employment_payload = employment_builder.build_empemployment_payload()
            if not employment_payload:
                ctx.fail(f"Failed to build employment payload for user {user_id}")
                return
            ctx.payloads["empemployment"] = employment_payload

            empjob_payload = employment_builder.build_empjob_payload()
            if not empjob_payload:
                ctx.fail(f"Failed to build empjob payload for user {user_id}")
                return
            ctx.payloads["empjob"] = empjob_payload

            # Store calculated startDate in context for reuse
            if employment_builder.calculated_start_date:
                ctx.empjob_start_date = employment_builder.calculated_start_date
        except Exception as e:
            ctx.fail(f"Error building employment payloads for user {ctx.user_id}: {e}")

    def _handle_relationships(
        self,
        row: pd.Series,
        ctx: UserExecutionContext,
        employment_builder: EmploymentPayloadBuilder,
        results: dict = None,
    ):
        """
        Build EmpJobRelationships payloads (HR / Matrix Manager).
        This function assumes employment_builder is already fully initialized.
        Skip if manager (rel_user_id) is not available.
        Args:
            row (pd.Series): The data row for the user.
            ctx (UserExecutionContext): The execution context of the user.
            employment_builder (EmploymentPayloadBuilder): The employment payload builder, retrieved from ctx.
        Steps:
        1. Check for HR and Matrix Manager fields.
        2. Build relationship payloads if fields are present.
        3. Add relationship payloads to context.
        """
        try:
            if not employment_builder:
                ctx.fail("Employment builder not initialized for relationships")
                return

            rel_payloads = []

            # HR relationship
            self._process_relationship(
                ctx=ctx,
                row=row,
                builder=employment_builder,
                results=results,
                rel_payloads=rel_payloads,
                relation_user=row.get("hr"),
                relationship_type="18387",
                label="HR",
            )

            # Matrix Manager relationship
            self._process_relationship(
                ctx=ctx,
                row=row,
                builder=employment_builder,
                results=results,
                rel_payloads=rel_payloads,
                relation_user=row.get("matrix_manager"),
                relationship_type="18385",
                label="Matrix Manager",
            )

            if rel_payloads:
                ctx.payloads["empjobrelationships"] = rel_payloads
            else:
                Logger.warning(f"No relationships to build for user {ctx.user_id}")

        except Exception as e:
            ctx.fail(f"Error building relationships for user {ctx.user_id}: {e}")

    def _process_relationship(
        self,
        *,
        ctx: UserExecutionContext,
        row: pd.Series,
        builder: EmploymentPayloadBuilder,
        results: dict,
        rel_payloads: list,
        relation_user: str,
        relationship_type: str,
        label: str,
    ):
        if not relation_user or not pd.notna(relation_user):
            return

        relation_user = str(relation_user).strip()
        if relation_user in {"", "NO_HR", "NO_MANAGER"}:
            return

        existing_rel, existing_start_date = self._get_relationship_if_exists(
            user_id=ctx.user_id,
            relation_type=relationship_type,
        )

        # Same relationship already exists → skip only this relationship
        if existing_rel and existing_rel.lower() == relation_user.lower():
            return

        # Different relationship exists → delete old one
        if existing_rel and existing_rel.lower() != relation_user.lower():
            Logger.info(
                f"User {ctx.user_id} has existing {label} relationship with {existing_rel}, "
                f"but new {label} is {relation_user}. Deleting old relationship."
            )
            delete_payload = builder.build_empjob_relationships_payload(
                old_rel_user_id=existing_rel,
                relationship_type=relationship_type,
                relationship_start_date=existing_start_date,
            )
            if delete_payload:
                rel_payloads.append(delete_payload)
            else:
                ctx.fail(f"Failed to build delete payload for old {label} relationship")
                return

        relationship_start_date = self._resolve_relationship_start_date(
            relation_user_id=relation_user,
            employee_row=row,
            existing_start_date=existing_start_date,
            results=results,
        )

        create_payload = builder.build_empjob_relationships_payload(
            rel_user_id=relation_user,
            relationship_type=relationship_type,
            relationship_start_date=relationship_start_date,
        )

        if create_payload:
            rel_payloads.append(create_payload)
        else:
            ctx.fail(f"{label} relationship payload failed")

    def _resolve_relationship_start_date(
        self,
        *,
        relation_user_id: str,
        employee_row: pd.Series,
        existing_start_date: str | None,
        results: dict,
    ) -> str:
        """
        Resolve relationship start date ensuring SAP business key uniqueness.
        """

        # Default: use relation user's position date
        relationship_start_date = self._get_relationship_start_date(
            relation_user_id=relation_user_id,
            results=results,
        )

        # Fallback to employee start date
        if not relationship_start_date:
            relationship_start_date = convert_to_unix_timestamp(
                employee_row.get("start_of_employment")
            )

        # Avoid SAP business key collision
        if existing_start_date:
            existing_ts = int(existing_start_date.strip("/Date()")) // 1000
            new_ts = int(relationship_start_date.strip("/Date()")) // 1000

            if new_ts <= existing_ts:
                new_ts = existing_ts + 86400  # +1 day
                relationship_start_date = f"/Date({new_ts * 1000})/"

        return relationship_start_date

    def _get_relationship_start_date(self, relation_user_id: str, results: dict):
        """
        Retrieve the relationship start date from either Results if its in current Processing or EmpJob cache.
        """
        user_id = str(relation_user_id).strip().lower()
        ctx = results.get(user_id)
        if ctx:
            row = ctx.runtime["original_row"]
            position_date = row.get("date_of_position")
            if position_date:
                return convert_to_unix_timestamp(position_date)

        # Fallback to EmpJob cache
        emp_cache = self.sap_cache.get("empjob_data_df")
        if emp_cache is not None and not emp_cache.empty:
            rel_empjob_row = emp_cache[
                emp_cache["userid"].astype(str).str.lower() == user_id
            ]
            if not rel_empjob_row.empty:
                start_date = rel_empjob_row.iloc[0]["startdate"]
                return start_date

    def _get_relationship_if_exists(self, user_id: str, relation_type: str):
        """
        Retrieve existing relationship of a given type for a user from EmpJobRelationships cache if exists.
        Return:
        Relation ID and Start Date.
        """
        empjob_rel_cache = self.sap_cache.get("empjobrelationships_df")
        if empjob_rel_cache is not None and not empjob_rel_cache.empty:
            rel_rows = empjob_rel_cache[
                (empjob_rel_cache["userid"].astype(str).str.lower() == user_id.lower())
                & (empjob_rel_cache["relationshiptype"] == relation_type)
            ]
            if not rel_rows.empty:
                relation_row = rel_rows.iloc[0]
                return relation_row["reluserid"], relation_row["startdate"]

        return None, None

    def _handle_ep_ec_roles(self, row: pd.Series, ctx: UserExecutionContext):
        """
        Retrieve Role from PDM and from EC and compare:
        1. If PDM is not null and different then EC role, prepare to update EC role to match PDM.
        2. If PDM role is null and EC role exists, prepare to put default Role in EC. which is 'EP'.
        Steps:
        1. Retrieve current PDM role from row (custom_string_8).
        2. Retrieve current EC role from Postgres cache (using ep_ec_role)
        3. Compare roles and build User payload following the logic above.
        """
        try:
            user_id = ctx.ec_user_id
            if not user_id:
                user_id = get_userid_from_personid(person_id=user_id)
            pdm_role = (
                row.get("custom_string_8", "").strip()
                if pd.notna(row.get("custom_string_8"))
                else ""
            )
            ec_role = ""

            # Mark dependencies as SUCCESS if not dirty to not block processing
            deps = self.ENTITY_DEPENDENCIES.get("UserRole", [])
            for dep in deps:
                if dep not in ctx.dirty_entities:
                    ctx.runtime["entity_status"][dep] = "SUCCESS"

            # Retrieve EC role from Postgres cache
            ec_roles_df = self.postgres_cache.get("ec_data_df")
            if ec_roles_df is not None and not ec_roles_df.empty:
                user_role_row = ec_roles_df[
                    ec_roles_df["userid"].astype(str).str.lower() == user_id.lower()
                ]
                if not user_role_row.empty:
                    raw_role = user_role_row.iloc[0].get("ep_ec_role")
                    if pd.isna(raw_role):
                        ec_role = ""
                    else:
                        ec_role = str(raw_role).strip()

            # Determine if update is needed
            if pdm_role and pdm_role != ec_role:
                # PDM role is set and different from EC role - update EC to match PDM

                user_payload = build_user_role_payload(
                    user_id=user_id,
                    role_code=pdm_role,
                )
                if not user_payload:
                    ctx.fail(
                        f"Failed to build User payload for role update for user {user_id}"
                    )
                    return
                ctx.payloads["userrole"] = user_payload
            elif not pdm_role and ec_role:
                # PDM role is null but EC role exists - set default 'EP' role in EC
                user_payload = build_user_role_payload(
                    user_id=user_id,
                    role_code="19677",
                )
                if not user_payload:
                    ctx.warn(
                        f"Failed to build User payload for default role assignment for user {user_id}"
                    )
                    return
                ctx.payloads["userrole"] = user_payload
            else:
                return  # No action needed
        except Exception as e:
            Logger.error(
                f"Error handling EP/EC roles for user {ctx.user_id}: {e}", exc_info=True
            )
            ctx.warn(f"Error handling EP/EC roles for user {ctx.user_id}: {e}")

    def _reset_collected_payloads(self):
        """
        Reset collected payloads for the next batch processing.
        """
        self.collected_payloads = {entity: {} for entity, _ in self.EXECUTION_PLAN}

    def _collect_payloads(self, ctx: UserExecutionContext):
        """
        Collect payloads from user execution context for batch upserts.
        Args:
            ctx (UserExecutionContext): The execution context of the user.
        """
        for entity_name, payload_key in self.EXECUTION_PLAN:
            if payload_key not in ctx.payloads:
                continue

            payload = ctx.payloads[payload_key]
            if isinstance(payload, dict):
                payload = [payload]

            self.collected_payloads[entity_name][ctx.user_id] = payload

    def _execute_batch_upserts(self, results, batch_user_ids, is_retry=False):
        """
        Execute batched upserts per entity (SAP-compliant).
        Refresh caches for Position and EmpJob after upserts.
        Args:
            results (Dict[str, UserExecutionContext]): Mapping of user_id to their execution context.
            batch_user_ids (set): Set of user IDs in the current batch.
            is_retry (bool): Whether this is a retry operation for HR users.
        """
        try:
            if is_retry:
                self._execute_hr_retry_upserts(results, batch_user_ids)
                return

            for entity_name, _ in self.EXECUTION_PLAN:
                payloads_per_user = self.collected_payloads.get(entity_name)
                Logger.info(
                    f"Processing upsert for entity: {entity_name} with {len(payloads_per_user) if payloads_per_user else 0} users"
                )

                if not payloads_per_user:
                    continue

                # Filter eligible users for this entity
                eligible_payloads = self._filter_eligible_payloads(
                    entity_name, payloads_per_user, results
                )

                if not eligible_payloads:
                    Logger.info(f"No eligible users for {entity_name}")
                    continue

                # Execute upserts with entity-specific handling
                Logger.info(
                    f"Upserting {entity_name} for {len(eligible_payloads)} users"
                )

                if entity_name == "PerEmail":
                    self._execute_email_upserts(entity_name, eligible_payloads, results)
                else:
                    self._execute_standard_upserts(
                        entity_name, eligible_payloads, results
                    )

                # Post-processing after specific entities
                if entity_name == "Position":
                    self._retry_position_dependent_entities(results, batch_user_ids)

                if entity_name == "EmpJob":
                    self._execute_position_sync(results, batch_user_ids)

                # Mark remaining PENDING users as SKIPPED
                self._mark_pending_as_skipped(results, entity_name)

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

        # Then execute upserts for relationship entities
        for entity in ["PositionMatrixRelationships", "EmpJobRelationships"]:
            payloads_per_user = self.collected_payloads.get(entity)

            # Keep only users to be retried
            payloads_per_user = {
                uid: payload
                for uid, payload in (payloads_per_user or {}).items()
                if uid in batch_user_ids
            }

            eligible_payloads = self._filter_eligible_payloads(
                entity, payloads_per_user, results
            )

            if not eligible_payloads:
                Logger.info(f"No eligible users for {entity} in retry")
                continue

            responses = self.upsert_client.upsert_entity_for_users(
                entity_name=entity, user_payloads=eligible_payloads
            )

            self._process_upsert_responses(
                entity, responses, results, is_warning_only=True
            )

    def _filter_eligible_payloads(self, entity_name, payloads_per_user, results):
        """
        Filter payloads to only include eligible users for the given entity.

        Args:
            entity_name: Name of the entity being processed
            payloads_per_user: Dict mapping user_id to their payloads
            results: Dict of UserExecutionContext objects

        Returns:
            Dict of eligible payloads ready for upsert
        """
        eligible_payloads = {}

        for user_id, payload in payloads_per_user.items():
            ctx = results[user_id]

            # Skip users with errors (only for new employee creation)
            if not ctx.is_update and ctx.has_errors:
                Logger.info(
                    f"{entity_name} skipped for {user_id}: has_errors=True, errors={ctx.errors}"
                )
                ctx.runtime["entity_status"][entity_name] = "SKIPPED"
                continue

            # For updates, skip dependency checks since entities already exist in SAP
            # Only enforce dependencies for new employee creation
            if not ctx.is_update and not self._can_execute_entity(ctx, entity_name):
                ctx.runtime["entity_status"][entity_name] = "SKIPPED"
                continue

            eligible_payloads[user_id] = payload
        return eligible_payloads

    def _execute_email_upserts(self, entity_name, eligible_payloads, results):
        """
        Execute email upserts with strict ordering required by SAP:
        DEMOTE -> DELETE -> UPDATE_TYPE -> PROMOTE -> INSERT
        """
        action_order = ["DEMOTE", "DELETE", "UPDATE_TYPE", "PROMOTE", "INSERT"]
        all_responses = {}

        for action in action_order:
            chunk_payloads = []
            chunk_user_index = []

            for user_id, payloads in eligible_payloads.items():
                items = payloads if isinstance(payloads, list) else [payloads]

                for p in items:
                    p_action = p.get("_email_action")
                    # Treat untagged payloads (built for new employees) as INSERTs
                    is_untagged = p_action is None

                    if p_action == action or (is_untagged and action == "INSERT"):
                        # Make a shallow copy and remove internal marker before sending
                        send_p = {k: v for k, v in p.items() if k != "_email_action"}
                        chunk_payloads.append(send_p)
                        chunk_user_index.append(user_id)

            if not chunk_payloads:
                continue

            # Call upsert client for this action chunk
            responses = self.upsert_client.upsert_entity_for_users(
                entity_name=entity_name,
                user_payloads={
                    uid: [p] for uid, p in zip(chunk_user_index, chunk_payloads)
                },
            )

            # Merge responses: keep earlier FAILURE if present
            for uid, res in responses.items():
                existing = all_responses.get(uid)
                if existing and existing.get("status") == "FAILED":
                    continue  # Preserve failure
                all_responses[uid] = res
        # Process all collected responses
        self._process_upsert_responses(entity_name, all_responses, results)

    def _execute_standard_upserts(self, entity_name, eligible_payloads, results):
        """
        Execute standard upserts for non-email entities.
        """
        responses = self.upsert_client.upsert_entity_for_users(
            entity_name=entity_name, user_payloads=eligible_payloads
        )

        # Determine if entity failures should be warnings only
        is_warning_only = entity_name in [
            "PositionMatrixRelationships",
            "EmpJobRelationships",
        ]

        self._process_upsert_responses(entity_name, responses, results, is_warning_only)

    def _process_upsert_responses(
        self, entity_name, responses, results, is_warning_only=False
    ):
        """
        Process upsert responses and update user contexts.

        Args:
            entity_name: Name of the entity
            responses: Dict mapping user_id to API response
            results: Dict of UserExecutionContext objects
            is_warning_only: If True, failures are treated as warnings instead of errors
        """
        for user_id, result in responses.items():
            ctx = results[user_id]
            ctx.runtime[entity_name] = result

            if result["status"] == "FAILED":
                ctx.runtime["entity_status"][entity_name] = "FAILED"

                # Build detailed error message with API response
                error_details = (
                    f"{entity_name} failed - Message: {result.get('message')}"
                )
                if result.get("httpCode"):
                    error_details += f", HTTP Code: {result.get('httpCode')}"
                if result.get("key"):
                    error_details += f", Key: {result.get('key')}"

                if is_warning_only:
                    Logger.warning(f"{error_details} - continuing anyway")
                    ctx.warn(error_details)
                else:
                    Logger.error(error_details)
                    ctx.fail(error_details)
            else:
                ctx.runtime["entity_status"][entity_name] = "SUCCESS"

                # Store position_code from response for employment processing
                if entity_name == "Position" and result.get("key"):
                    key = result.get("key")
                    # Extract position code from compound key
                    # Key format: "Position/code=1020001,Position/effectiveStartDate=2026-01-14T00:00:00.000Z"
                    if "code=" in key:
                        position_code = key.split("code=")[1].split(",")[0]
                        ctx.position_code = position_code
                    else:
                        Logger.warning(f"Could not parse position code from key: {key}")

    def _retry_position_dependent_entities(self, results, batch_user_ids):
        """
        Retry employment and position matrix relationships for users who needed position_code.
        Called after Position entity upserts are complete.
        """
        Logger.info(
            "Retrying employment & position matrix for users who need position_code"
        )

        for user_id, ctx in results.items():
            if (
                user_id in batch_user_ids
                and ctx.runtime.get("needs_position_lookup")
                and not ctx.has_errors
                and ctx.position_code
            ):
                row = ctx.runtime.get("original_row")
                if row is None:
                    Logger.warning(
                        f"No original_row found for user {user_id} during retry"
                    )
                    continue

                # Retry position matrix relationships
                position_builder = ctx.builders.get("position")
                if position_builder:
                    self._handle_position_matrix_relationship(
                        row, ctx, position_builder
                    )

                # Retry employment
                if self._can_execute_entity(ctx, "EmpEmployment"):
                    self._handle_employment(row, ctx, results)

                    # Retry EmpJobRelationships if employment rebuilt successfully
                    if not ctx.has_errors and self._can_execute_entity(
                        ctx, "EmpJobRelationships"
                    ):
                        self._handle_relationships(
                            row, ctx, ctx.builders.get("employment"), results=results
                        )

                    # Collect payloads again if retry successful
                    if not ctx.has_errors:
                        self._collect_payloads(ctx)
                else:
                    continue

    def _execute_position_sync(self, results, batch_user_ids):
        """
        Sync Position to Job after EmpJob success (triggers SAP PositionToJobInfoSyncRule).
        This is NOT a Position creation/update - it's a sync operation to propagate EmpJob changes.
        """
        Logger.info("=" * 120)
        Logger.info("POSITION-TO-JOB SYNC OPERATION (Not a Position creation/update)")
        Logger.info("=" * 120)

        position_sync_payloads = {}

        for user_id, ctx in results.items():
            if not self._should_execute_position_sync(ctx, user_id, batch_user_ids):
                continue

            row = ctx.runtime.get("original_row")
            position_builder = ctx.builders.get("position")

            # Build or retrieve position builder
            if position_builder is None:
                position_builder = self._build_position_builder_for_sync(
                    row, ctx, results, user_id
                )
                if position_builder is None:
                    continue

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
                Logger.warning(
                    f"[POSITION SYNC] Failed to build sync payload for user {user_id}"
                )

        # Batch upsert Position sync payloads
        if position_sync_payloads:
            Logger.info(
                f"[POSITION SYNC] Upserting {len(position_sync_payloads)} sync operations to trigger PositionToJobInfoSyncRule"
            )

            # API requires entity_name="Position" but we track separately
            sync_responses = self.upsert_client.upsert_entity_for_users(
                entity_name="Position", user_payloads=position_sync_payloads
            )

            for user_id, result in sync_responses.items():
                # Store sync result separately (not in entity_status)
                results[user_id].runtime["Position_SYNC"] = result

                if result["status"] == "FAILED":
                    Logger.warning(
                        f"[POSITION SYNC] Sync failed for user {user_id}: {result.get('message')}"
                    )
                    # Don't fail the entire process - this is just a sync operation
                    results[user_id].warn(
                        f"Position-to-Job sync failed: {result.get('message')}"
                    )
                else:
                    continue

        Logger.info("=" * 80)

    def _should_execute_position_sync(self, ctx, user_id, batch_user_ids):
        """
        Determine if position sync should be executed for a user.
        """
        return (
            user_id in batch_user_ids
            and ctx.runtime.get("entity_status", {}).get("EmpJob") == "SUCCESS"
            and not ctx.has_errors
            and ctx.position_code
            and ctx.empjob_start_date
        )

    def _build_position_builder_for_sync(self, row, ctx, results, user_id):
        """
        Build a position builder for position sync operation.
        """
        if row is None:
            Logger.warning(
                f"No original_row and position_builder found for user {user_id} during Position sync"
            )
            return None

        job_validator = JobExistenceValidator(
            job_mappings=self.postgres_cache.get("jobs_titles_data_df"),
            job_code=row["jobcode"],
        )

        job_mapping = job_validator.get_job_mapping()
        if job_mapping.empty:
            Logger.warning(
                f"Job code {row['jobcode']} does not exist for user {user_id} during Position sync"
            )
            ctx.fail(
                f"Job code {row['jobcode']} does not exist for user {user_id} during Position sync"
            )
            return None

        return PositionPayloadBuilder(
            record=row,
            job_mappings=job_mapping,
            results=results,
            ec_user_id=ctx.ec_user_id,
        )

    def _mark_pending_as_skipped(self, results, entity_name):
        """
        Mark any remaining PENDING users as SKIPPED for the given entity.
        """
        for user_id, ctx in results.items():
            if ctx.runtime.get("entity_status", {}).get(entity_name) == "PENDING":
                ctx.runtime["entity_status"][entity_name] = "SKIPPED"

    def _retry_hr_users(self, results: dict, batch_user_ids: set):
        """
        Retry processing for users who failed due to missing HR relationship.
        This is a common issue since HR relationships are processed at the end and may be missing during employment processing.
        """
        Logger.info("Retrying users with missing HR relationship...")

        for user_id, ctx in results.items():
            if not self._should_retry_hr_user(ctx, user_id, batch_user_ids):
                continue

            row = ctx.runtime.get("original_row")
            if row is None:
                Logger.warning(
                    f"No original_row found for user {user_id} during HR retry"
                )
                continue

            # Retry relationships (which includes HR)
            if ctx.builders.get("employment") is not None:
                self._handle_relationships(
                    row=row,
                    ctx=ctx,
                    employment_builder=ctx.builders.get("employment"),
                    results=results,
                )
            else:
                Logger.warning(
                    f"No employment builder found for user {user_id} during HR retry"
                )
                ctx.warn(
                    f"Cannot retry HR relationship for user {user_id} because employment builder is missing"
                )
            
            if ctx.builders.get("position") is not None:
                self._handle_position_matrix_relationship(
                    row=row, ctx=ctx, position_builder=ctx.builders.get("position")
                )
            else:
                ctx.warn(
                    f"No position builder found for user {user_id} during HR retry - cannot retry position matrix relationship"
                )

            # If retry successful, collect payloads again for batch upsert
            if not ctx.has_errors:
                self._collect_payloads(ctx)

    def _should_retry_hr_user(self, ctx, user_id, batch_user_ids):
        """
        Determine if a user should be retried for HR relationship issues.
        """
        return (
            user_id in batch_user_ids
        )

    def _log_batch_summary_by_user(self, results: dict):
        """
        Log a summary of batch processing results grouped by user.
        Shows which entities were processed successfully/failed for each user.
        Includes payloads for failed entities.
        """
        import json

        Logger.info("\n" + "=" * 80)
        Logger.info("BATCH PROCESSING SUMMARY (BY USER)")
        Logger.info("=" * 80)

        for user_id, ctx in results.items():
            entity_statuses = ctx.runtime.get("entity_status", {})
            processed_entities = {
                k: v for k, v in entity_statuses.items() if v != "PENDING"
            }

            if not processed_entities:
                continue

            Logger.info(f"\n{'─' * 80}")
            if ctx.is_update:
                Logger.info(f"Update User: {user_id}")
            else:
                Logger.info(f"New User Creation: {user_id}")
            Logger.info(f"{'─' * 80}")

            # Group by status
            success = [e for e, s in processed_entities.items() if s == "SUCCESS"]
            failed = [e for e, s in processed_entities.items() if s == "FAILED"]
            skipped = [e for e, s in processed_entities.items() if s == "SKIPPED"]

            if success:
                Logger.info(f"  ✓ SUCCESS: {', '.join(success)}")
            if failed:
                Logger.error(f"  ✗ FAILED: {', '.join(failed)}")
                if ctx.errors:
                    for error in ctx.errors:
                        Logger.error(f"    - {error}")

                # Log payloads for failed entities
                for entity in failed:
                    payload_key = None
                    for entity_name, key in self.EXECUTION_PLAN:
                        if entity_name == entity:
                            payload_key = key
                            break
                        if entity == "Position_SYNC":
                            payload_key = "position_sync"
                            break
                    if payload_key and payload_key in ctx.payloads:
                        payload = ctx.payloads[payload_key]
                        try:
                            pretty_payload = json.dumps(payload, indent=2)
                            Logger.error(
                                f"    Failed payload for {entity}:\n{pretty_payload}"
                            )
                        except (TypeError, ValueError):
                            Logger.error(f"    Failed payload for {entity}: {payload}")

            # Show warnings if any exist
            if ctx.warnings:
                Logger.warning("  ⚠ WARNINGS:")
                for warning in ctx.warnings:
                    Logger.warning(f"    - {warning}")

            if skipped:
                Logger.info(f"  ⊘ SKIPPED: {', '.join(skipped)}")

        Logger.info("\n" + "=" * 80)

    def _extract_dirty_entities(self, df: pd.DataFrame) -> dict[str, dict]:
        """
        Map each user to dirty entities and store structured email actions.

        Args:
            df: pd.DataFrame with columns ["userid", "field_name"]

        Returns:
            Dict[user_id] = {
                "entities": set of entity names to update,
                "email_actions": list of dicts:
                    {
                        "action": str,        # insert / delete / update_type / promote / demote
                        "type": int,          # 18242 = business, 18240 = private
                        "email": str | None   # actual email, if known
                    }
            }
        """
        dirty_entities = {}

        for _, row in df.iterrows():
            user_id = row.get("userid")
            field = row.get("field_name")
            pdm_value = row.get("pdm_value")
            ec_value = row.get("ec_value")

            if user_id not in dirty_entities:
                dirty_entities[user_id] = {"entities": set(), "email_actions": []}

            if field.startswith("email::"):
                # Parse structured email action
                # e.g., email::insert::18242, email::promote::18242, email::insert::18240
                parts = field.split("::")
                if len(parts) == 3:
                    action_name = parts[1].lower()
                    type_str = parts[2]

                    # Type can be numeric (18242, 18240) or text (business, personal)
                    if type_str.isdigit():
                        email_type = int(type_str)
                    else:
                        email_type = 18242 if "business" in type_str.lower() else 18240

                    dirty_entities[user_id]["entities"].add("PerEmail")

                    # Use whichever value is filled (only one will be present at a time)
                    email_to_use = pdm_value if pd.notna(pdm_value) else ec_value

                    dirty_entities[user_id]["email_actions"].append(
                        {
                            "action": action_name,  # insert / delete / update_type / promote / demote
                            "type": email_type,
                            "email": email_to_use if pd.notna(email_to_use) else None,
                        }
                    )
                continue

            # Map normal fields to their target entities
            entities = self.DIRTY_FIELD_TO_ENTITY.get(field)
            if entities:
                if isinstance(entities, list):
                    dirty_entities[user_id]["entities"].update(entities)
                else:
                    dirty_entities[user_id]["entities"].add(entities)
            else:
                Logger.warning(
                    f"Field '{field}' not found in DIRTY_FIELD_TO_ENTITY mapping for user {user_id}"
                )

        return dirty_entities

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
                        Logger.warning(
                            f"No existing position found in employees cache for user {user_id}"
                        )
                else:
                    Logger.warning(
                        f"No employee record found in employees cache for user {user_id}"
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
                return
            if position_builder.position_code:
                if ctx.position_code != position_builder.position_code:
                    ctx.position_code = position_builder.position_code
            ctx.payloads["position"] = position_payload
            ctx.builders["position"] = position_builder

        except Exception as e:
            ctx.fail(f"Error building position update for user {ctx.user_id}: {e}")

    def _build_person_updates(
        self, row: pd.Series, ctx: UserExecutionContext, dirty_entities: set
    ):
        """
        Build person-related payloads (PerPerson, PerPersonal, PerEmail, PerPhone) for updates.
        Only builds the entities that are in dirty_entities.
        """
        try:
            user_id = ctx.user_id
            # Mark dependencies as SUCCESS if not dirty to not block processing
            for entity in ["PerPerson", "PerPersonal", "PerEmail", "PerPhone"]:
                deps = self.ENTITY_DEPENDENCIES.get(entity, [])
                for dep in deps:
                    if dep not in ctx.dirty_entities:
                        ctx.runtime["entity_status"][dep] = "SUCCESS"

            # Build PersonPayloadBuilder once for all person entities
            # Use start_of_employment (hire date) for PerPersonal, not date_of_position
            employment_start_date = row.get("date_of_position")

            person_builder = PersonPayloadBuilder(
                first_name=row.get("firstname"),
                last_name=row.get("lastname"),
                person_id_external=user_id,
                date_of_birth=row.get("date_of_birth"),
                start_date=employment_start_date,
                nickname=row.get("nickname", None),
                middle_name=row.get("mi", None),
                email=row.get("email"),
                private_email=row.get("private_email"),
                gender=row.get("gender"),
                phone=row.get("phone"),
                postgres_cache=self.postgres_cache,
            )

            # PerPerson (usually doesn't change, but include if dirty since we have date_of_birth can change)
            if "PerPerson" in dirty_entities:
                perperson_payload = person_builder.build_perperson_payload()
                if perperson_payload:
                    ctx.payloads["perperson"] = perperson_payload
                else:
                    ctx.warn(
                        f"Failed to build perperson update payload for user {user_id}"
                    )

            # PerPersonal (name, gender changes)
            if "PerPersonal" in dirty_entities:
                perpersonal_payload = person_builder.build_perpersonal_payload()
                if perpersonal_payload:
                    ctx.payloads["perpersonal"] = perpersonal_payload
                else:
                    ctx.warn(
                        f"Failed to build perpersonal update payload for user {user_id}"
                    )

            # PerEmail (special handling via email actions): For more details, see _handle_email_updates method:
            # This allows more control over email insertions, updates, deletions, promotions, and demotions.
            if "PerEmail" in dirty_entities:
                self._handle_email_updates(ctx, person_builder)

            # PerPhone
            if "PerPhone" in dirty_entities:
                if row.get("phone") and not pd.isna(row.get("phone")):
                    perphone_payload = person_builder.build_perphone_payload()
                    if perphone_payload:
                        ctx.payloads["perphone"] = perphone_payload
                    else:
                        ctx.warn(
                            f"Failed to build perphone update payload for user {user_id}"
                        )

        except Exception as e:
            ctx.fail(f"Error building person updates for user {ctx.user_id}: {e}")

    def _handle_email_updates(
        self,
        ctx: "UserExecutionContext",
        person_builder: "PersonPayloadBuilder",
    ):
        """
        Handle email updates for a user based on EmailValidator decisions.
        Transforms flat email_actions list from _extract_dirty_entities into structured format.

        Input format (from _extract_dirty_entities):
            [{"action": "insert", "type": 18242, "email": "test@example.com"}, ...]

        Processed format:
            {
                "insert": [(email, type), ...],
                "delete": [(email, type), ...],
                "update_type": [(email, old_type, new_type), ...],
                "primary": {
                    "promote": (email, type) or None,
                    "demote": (email, type) or None
                }
            }
        """
        try:
            user_id = ctx.user_id
            email_actions_flat = ctx.runtime.get("email_actions", [])
            if not email_actions_flat:
                return

            # Transform flat list into structured format
            email_actions = {
                "insert": [],
                "delete": [],
                "update_type": [],
                "primary": {"promote": None, "demote": None},
            }

            for item in email_actions_flat:
                action = item.get("action", "").lower()
                email = item.get("email")
                email_type = item.get("type")

                if action == "insert":
                    email_actions["insert"].append((email, email_type))
                elif action == "delete":
                    email_actions["delete"].append((email, email_type))
                elif action == "update_type":
                    email_actions["update_type"].append((email, email_type))
                elif action == "promote":
                    email_actions["primary"]["promote"] = (email, email_type)
                elif action == "demote":
                    email_actions["primary"]["demote"] = (email, email_type)
                else:
                    ctx.warn(f"Unknown email action '{action}' for user {user_id}")

            if not any(
                [
                    email_actions["insert"],
                    email_actions["delete"],
                    email_actions["update_type"],
                    email_actions["primary"]["promote"],
                    email_actions["primary"]["demote"],
                ]
            ):
                ctx.warn(f"No valid email actions after transformation for {user_id}")
                return

            payloads = []

            # Get promote/demote info for deduplication
            primary = email_actions.get("primary", {})
            promote = primary.get("promote")  # (email, type)
            demote = primary.get("demote")  # (email, type)

            # DEMOTE - Must happen first to remove primary flag before promoting another
            if demote:
                email, email_type = demote
                payload = person_builder.build_peremail_payload_action(
                    email=email,
                    email_type=email_type,
                    is_primary=False,
                    action="UPDATE",
                )
                if payload:
                    payload["_email_action"] = "DEMOTE"
                    payloads.append(payload)

            # DELETE existing emails
            for email, email_type in email_actions.get("delete", []):
                payload = person_builder.build_peremail_payload_action(
                    email=email, email_type=email_type, action="DELETE"
                )
                if payload:
                    payload["_email_action"] = "DELETE"
                    payloads.append(payload)

            # UPDATE email type
            for email, new_type in email_actions.get("update_type", []):
                payload = person_builder.build_peremail_payload_action(
                    email=email, email_type=new_type, is_primary=False, action="UPDATE"
                )
                if payload:
                    payload["_email_action"] = "UPDATE_TYPE"
                    payloads.append(payload)

            # PROMOTE - Set new primary email (if email doesn't exist, this also inserts it)
            if promote:
                email, email_type = promote
                payload = person_builder.build_peremail_payload_action(
                    email=email, email_type=email_type, is_primary=True, action="UPDATE"
                )
                if payload:
                    payload["_email_action"] = "PROMOTE"
                    payloads.append(payload)

            # INSERT new emails (skip if email is being promoted - promote handles both insert + primary)
            for email, email_type in email_actions.get("insert", []):
                # Skip if this email is being promoted (promote will handle the insert)
                if promote and email == promote[0] and email_type == promote[1]:
                    continue

                is_primary = (
                    email_type == 18242
                )  # business emails are primary by default
                payload = person_builder.build_peremail_payload_action(
                    email=email,
                    email_type=email_type,
                    is_primary=is_primary,
                    action="INSERT",
                )
                if payload:
                    payload["_email_action"] = "INSERT"
                    payloads.append(payload)

            if payloads:
                deduped = []
                seen = set()
                for p in payloads:
                    try:
                        key = (
                            p.get("emailAddress"),
                            str(p.get("emailType"))
                            if p.get("emailType") is not None
                            else None,
                            p.get("operation") if "operation" in p else None,
                            bool(p.get("isPrimary")) if "isPrimary" in p else None,
                        )
                    except Exception:
                        key = None

                    if key and key in seen:
                        continue

                    if key:
                        seen.add(key)
                    deduped.append(p)

                ctx.payloads["peremail"] = deduped if len(deduped) > 1 else deduped[0]
            else:
                ctx.warn(
                    f"No payloads generated for user {user_id} despite email actions"
                )

        except Exception as e:
            ctx.fail(f"Error handling email updates for {user_id}: {e}")

    def _build_employment_updates(
        self,
        row: pd.Series,
        ctx: UserExecutionContext,
        dirty_entities: set,
        results: dict,
    ):
        """
        Build employment-related payloads (EmpEmployment, EmpJob, EmpJobRelationships) for updates.
        Only builds the entities that are in dirty_entities.
        """
        try:
            # Mark dependencies as SUCCESS if not dirty to not block processing
            for entity in ["EmpEmployment", "EmpJob", "EmpJobRelationships"]:
                deps = self.ENTITY_DEPENDENCIES.get(entity, [])
                for dep in deps:
                    if dep not in ctx.dirty_entities:
                        ctx.runtime["entity_status"][dep] = "SUCCESS"

            user_id = ctx.user_id

            # Get position code from context or employees cache
            position_code = ctx.position_code
            if not position_code:
                # Fallback: try employees cache for existing users
                employees_df = self.sap_cache.get("employees_df")
                if employees_df is not None:
                    # Filter to retrieve match with user_id and jobcode different than the historical dummy jobcode T00001
                    match = employees_df[
                        (employees_df["userid"] == user_id)
                        & (employees_df["jobcode"] != "T00001")
                    ]
                    if not match.empty:
                        position_code = match["position"].values[0]
                        ctx.position_code = position_code  # Store for future use

            if not position_code:
                ctx.fail(
                    f"Cannot build employment updates without position code for user {user_id}"
                )
                return

            # Get seq number for EmpJob updates
            seq_number = None
            start_date = None
            build_event_reason = "INITLOAD"
            if "EmpJob" in dirty_entities:
                employees_df = self.sap_cache.get("employees_df")
                if employees_df is not None:
                    match = employees_df[employees_df["userid"] == user_id]
                    if not match.empty:
                        current_seq = match["seqnumber"].values[0]
                        start_date = match["startdate"].values[0]
                        try:
                            seq_number = str(int(current_seq) + 1)
                            build_event_reason = "DATACHG"
                            Logger.info(
                                f"Calculated seq_number {seq_number} for EmpJob update for user {user_id}"
                            )
                        except (ValueError, TypeError):
                            seq_number = "1"
                            Logger.warning(
                                f"Invalid current seqnumber '{current_seq}' for user {user_id}, defaulting to 1"
                            )

            # Build employment builder - convert dates before passing
            hire_date_converted = convert_to_unix_timestamp(
                row.get("hiredate")
            ) or convert_to_unix_timestamp(row.get("start_of_employment"))
            start_of_employment_converted = convert_to_unix_timestamp(
                row.get("start_of_employment")
            )

            employment_builder = EmploymentPayloadBuilder(
                user_id=user_id,
                person_id_external=user_id,
                hire_date=hire_date_converted,
                start_of_employment=start_of_employment_converted,
                company=row.get("company"),
                build_event_reason=build_event_reason,
                cost_center=row.get("cost_center"),
                seq_num=seq_number,
                position=position_code,
                manager_id=row.get("manager"),
                start_date=start_date if start_date else None,
                manager_position_start_date=convert_to_unix_timestamp(
                    row.get("manager_position_start_date")
                )
                if row.get("manager_position_start_date")
                else None,
            )

            ctx.builders["employment"] = employment_builder

            # EmpEmployment (hire date changes)
            if "EmpEmployment" in dirty_entities:
                empemployment_payload = employment_builder.build_empemployment_payload()
                if empemployment_payload:
                    ctx.payloads["empemployment"] = empemployment_payload
                else:
                    ctx.warn(
                        f"Failed to build empemployment update payload for user {user_id}"
                    )

            # EmpJob (position, cost center, manager changes)
            if "EmpJob" in dirty_entities:
                empjob_payload = employment_builder.build_empjob_payload()
                if empjob_payload:
                    ctx.payloads["empjob"] = empjob_payload
                else:
                    ctx.warn(
                        f"Failed to build empjob update payload for user {user_id}"
                    )

            # EmpJobRelationships (manager, matrix manager, HR changes)
            if "EmpJobRelationships" in dirty_entities:
                self._handle_relationships(
                    row, ctx, employment_builder, results=results
                )

        except Exception as e:
            ctx.fail(f"Error building employment updates for user {ctx.user_id}: {e}")

    def _build_position_relationship_update(
        self, row: pd.Series, ctx: UserExecutionContext, results: dict
    ):
        """
        Build PositionMatrixRelationship payloads for update (both matrix_manager and hr).
        """
        try:
            # Mark dependencies as SUCCESS if not dirty to not block processing
            deps = self.ENTITY_DEPENDENCIES.get("PositionMatrixRelationships", [])
            for dep in deps:
                if dep not in ctx.dirty_entities:
                    ctx.runtime["entity_status"][dep] = "SUCCESS"

            user_id = ctx.user_id

            position_builder = ctx.builders.get("position")
            if not position_builder:
                # Build a new PositionPayloadBuilder if not present
                job_mappings = self.postgres_cache.get("jobs_titles_data_df")
                position_builder = PositionPayloadBuilder(
                    record=row.to_dict(),
                    job_mappings=job_mappings,
                    is_scm=row.get("is_scm_user", False),
                    is_update=True,
                    results=results,
                    ec_user_id=ctx.ec_user_id,
                )

            # Get matrix manager and hr info
            matrix_manager = (
                row.get("matrix_manager", "").strip()
                if pd.notna(row.get("matrix_manager"))
                else ""
            )
            hr_manager = row.get("hr", "").strip() if pd.notna(row.get("hr")) else ""

            if not matrix_manager and not hr_manager:
                ctx.warn(
                    f"No matrix manager or HR for user {user_id}, skipping PositionMatrixRelationships"
                )
                return

            payloads = []

            # Matrix Manager relationship
            if matrix_manager and matrix_manager not in ["", "NO_HR", "NO_MANAGER"]:
                matrix_payload = (
                    position_builder.build_position_matrix_relationship_payload(
                        relation_userid=matrix_manager,
                        relation_type="matrix manager",
                        user_position_code_=ctx.position_code,
                    )
                )
                if matrix_payload:
                    payloads.append(matrix_payload)
                else:
                    ctx.warn(
                        f"Failed to build matrix manager relationship for user {user_id}"
                    )

            # HR relationship
            if hr_manager and hr_manager not in ["", "NO_HR", "NO_MANAGER"]:
                hr_payload = (
                    position_builder.build_position_matrix_relationship_payload(
                        relation_userid=hr_manager,
                        relation_type="hr manager",
                        user_position_code_=ctx.position_code,
                    )
                )
                if hr_payload:
                    payloads.append(hr_payload)
                else:
                    ctx.warn(f"Failed to build HR relationship for user {user_id}")

            if payloads:
                ctx.payloads["positionmatrixrelationships"] = (
                    payloads if len(payloads) > 1 else payloads[0]
                )
            else:
                ctx.warn(
                    f"Failed to build any position matrix relationship updates for user {user_id}"
                )

        except Exception as e:
            ctx.fail(
                f"Error building position matrix relationship update for user {ctx.user_id}: {e}"
            )

    def _collect_position_code_effective_date_for_updates(
        self, user_id, ctx: UserExecutionContext
    ):
        """
        For updates, collect the existing position code effective date from employees cache.
        This is needed for certain update operations.
        Args:
            user_id: User ID to look up.
        Returns:
            Tuple of (position_code, start_date) or (None, None) if not found
        """
        try:
            employees_df = self.sap_cache.get("employees_df")
            if employees_df is not None and not employees_df.empty:
                # Retrieve EC user ID from context
                ec_user_id = ctx.ec_user_id
                emp_mask = (
                    employees_df["userid"]
                    .astype(str)
                    .str.lower()
                    .eq(ec_user_id.lower())
                )
                emp_result = employees_df[emp_mask]
                if not emp_result.empty:
                    start_date = emp_result["startdate"].values[0]
                    position_code = emp_result["position"].values[0]
                    return position_code, start_date
                else:
                    ctx.warn(
                        f"No employee record found for user with pdm id: {user_id} and ec user id: {ec_user_id} in employees cache"
                    )
                    return None, None
            else:
                ctx.warn("Employees cache is empty or not available")
                return None, None
        except Exception as e:
            Logger.error(
                f"Error collecting position code effective date for user {user_id}: {e}"
            )

    def _format_messages(self, title: str, messages: list[str]) -> str:
        if not messages:
            return None

        lines = [f"{title} ({len(messages)}):"]

        for idx, msg in enumerate(messages, start=1):
            # Indent multi-line messages nicely
            indented = "\n     ".join(msg.splitlines())
            lines.append(f"  {idx}) {indented}")

        return "\n".join(lines)

    def extract_history_data(self, results: dict, operation: str) -> dict:
        """
        Extract history data from processing results for database logging.
        """

        results_list = []
        success_count = 0
        warning_count = 0

        for user_id, ctx in results.items():
            has_errors = ctx.has_errors
            has_warnings = ctx.has_warnings

            if not has_errors:
                success_count += 1
                if has_warnings:
                    warning_count += 1

            error_message = (
                self._format_messages("ERRORS", ctx.errors) if has_errors else None
            )
            warning_message = (
                self._format_messages("WARNINGS", ctx.warnings)
                if has_warnings
                else None
            )
            # Detect Failed entities : Warning and Failed
            # failed_entities = [entity for entity, status if status in (FAILED, PENDING, SKIPPED)]
            # ctx.runtime["entity_status"][entity_name]
            # Successful entities: SUCCESS ONLY
            failed_entities = [
                entity
                for entity, status in ctx.runtime["entity_status"].items()
                if status != "SUCCESS" and status != "SKIPPED"
            ]
            success_entities = [
                entity
                for entity, status in ctx.runtime["entity_status"].items()
                if status == "SUCCESS"
            ]
            skipped_entities = [
                entity
                for entity, status in ctx.runtime["entity_status"].items()
                if status == "SKIPPED"
            ]
            base_record = {
                "user_id": user_id,
                "operation": operation,
                "is_scm": getattr(ctx, "is_scm", None),
                "is_im": getattr(ctx, "is_im", None),
                "payload_snapshot": ctx.payloads if ctx.payloads else None,
            }

            if has_errors:
                results_list.append(
                    {
                        **base_record,
                        "status": "FAILED",
                        "error_message": error_message,
                        "warning_message": warning_message,
                        "failed_entities": failed_entities if failed_entities else None,
                        "success_entities": success_entities
                        if success_entities
                        else None,
                        "skipped_entities": skipped_entities
                        if skipped_entities
                        else None,
                    }
                )

            elif has_warnings:
                results_list.append(
                    {
                        **base_record,
                        "status": "WARNING",
                        "warning_message": warning_message,
                        "failed_entities": failed_entities if failed_entities else None,
                        "success_entities": success_entities
                        if success_entities
                        else None,
                        "skipped_entities": skipped_entities
                        if skipped_entities
                        else None,
                    }
                )

            else:
                results_list.append(
                    {
                        **base_record,
                        "status": "SUCCESS",
                        "error_message": None,
                        "failed_entities": failed_entities if failed_entities else None,
                        "success_entities": success_entities
                        if success_entities
                        else None,
                        "skipped_entities": skipped_entities
                        if skipped_entities
                        else None,
                    }
                )

        return {
            "results": results_list,
            "success_count": success_count,
            "warning_count": warning_count,
            "failed_count": sum(1 for r in results_list if r["status"] == "FAILED"),
        }
