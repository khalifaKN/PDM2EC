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
from payload_builders.position._dummy_position import DummyPositionPayloadBuilder
from payload_builders.employment._employment import EmploymentPayloadBuilder
from payload_builders.position._position import PositionPayloadBuilder

from validator.position.position_validator import PositionValidator
from utils.date_converter import convert_to_unix_timestamp
import json

import pandas as pd

Logger = get_logger("migration_processing")


class MigrationProcessor(CoreProcessor):
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
        Creates or retrieves a dummy position for the given country and company.
        This dummy position is used for migrating users before their actual position is created.
        """
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

            # Create dummy position if not exists
            job_titles_data = self.postgres_cache.get("jobs_titles_data_df")
            if job_titles_data is None:
                raise ValueError(
                    "Job Titles cache is missing. Cannot create dummy position."
                )

            job_data_filtered = job_titles_data[
                job_titles_data["jobcode"] == self.job_code
            ]

            if job_data_filtered.empty:
                # Build job_data_filtered with static/default values if needed
                job_data_filtered = pd.DataFrame(
                    [
                        {
                            "bufu_id": "2764",
                            "cust_geographicalscope": "2924",
                            "cust_subunit": "2926",
                        }
                    ]
                )

            dummy_position_data = {
                "company": company,
                "cost_center": row.get("cost_center"),
                "country_code": row.get("country_code"),
                "bufu_id": job_data_filtered["bufu_id"].values[0],
                "jobcode": self.job_code,
                "address_code": row.get("address_code"),
                "cust_geographicalscope": job_data_filtered[
                    "cust_geographicalscope"
                ].values[0],
                "cust_subunit": job_data_filtered["cust_subunit"].values[0],
            }
            # Initialize builder and build payload
            builder = DummyPositionPayloadBuilder(dummy_position_data)
            payload = builder.build_dummy_position_payload()

            # Log dummy position payload for visibility
            Logger.info(
                f"Creating dummy position for company {company}, jobcode {self.job_code}"
            )
            # Log the payload in a readable format
            Logger.info(f"Dummy Position Payload: {json.dumps(payload, indent=2)}")

            # Upsert dummy position
            response = self.upsert_client.upsert_entity(
                entity_name="Dummy Position",
                payload=payload,
            )
            # Retrieve position code from response
            position_key = response.get("key")
            if not position_key:
                raise ValueError(
                    f"Failed to create dummy position for company {company} "
                    f"and jobcode {self.job_code}. Response: {response}"
                )

            # Example:
            # "Position/code=1020018,Position/effectiveStartDate=2025-09-29T00:00:00.000Z"
            position_code = position_key.split(",")[0].split("=")[1]

            # Update cache: put the new dummy position in the positions cache with minimal info
            new_entry = pd.DataFrame(
                [{"code": position_code, "company": company, "jobcode": self.job_code}]
            )

            updated_cache = (
                pd.concat([positions_df, new_entry], ignore_index=True)
                if positions_df is not None
                else new_entry
            )

            self.sap_cache.set(self.positions_cache_key, updated_cache)
            # Set dummy position in context
            ctx.dummy_position = position_code
            return position_code

        except Exception as e:
            raise RuntimeError(
                f"Error in _create_or_get_dummy_position for jobcode {self.job_code}: {e}"
            ) from e

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

            self._create_or_get_dummy_position(ctx)
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
                Logger.error(
                    f"Early return after UserRoles handling for {user_id} due to errors: {ctx.errors}"
                )
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
            ctx.fail(
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
                        Logger.info(
                            f"Found existing position code {existing_position} for user {user_id}"
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

            # Check if employee already has position with PositionValidator
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


        except Exception as e:
            ctx.fail(f"Error building position update for user {ctx.user_id}: {e}")