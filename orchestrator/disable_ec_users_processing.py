from payload_builders.employment._terminate_emp import EmploymentTerminationPayloadBuilder
from payload_builders.user._user import build_user_inactive_payload
from orchestrator.user_context import UserExecutionContext
from utils.logger import get_logger
from api.upsert_client import UpsertClient
from api.api_client import APIClient
from api.auth_client import AuthAPI
import json
import pandas as pd

Logger = get_logger("disable_users_processing")

class DisableUsersProcessor:
    """
    Orchestrates user deactivation and employment termination in SAP SuccessFactors.
    
    Core Capabilities:
    1. Employment Termination (EmpEmploymentTermination)
       - Builds termination payloads with end date and reason
       - Processes termination in batches for performance
    
    2. User Account Deactivation (User entity)
       - Sets user status to inactive
       - Prevents login access to the system
    
    Processing Flow:
    1. EmpEmploymentTermination → 2. User (inactive)
    
    Dependencies:
    - AuthAPI: OAuth token management
    - APIClient: HTTP communication with SAP APIs
    - UpsertClient: Batched entity upsert operations
    
    Key Features:
    - Batch processing for performance optimization
    - Graceful error handling with context preservation
    - Detailed logging with per-user summaries
    """

    EXECUTION_PLAN = [
        ("EmpEmploymentTermination", "emp_termination"),
        ("User", "user_inactive"),
    ]

    @staticmethod
    def _order_payloads(payloads: dict) -> dict:
        """
        Orders payload dictionary keys according to EXECUTION_PLAN.
        Keys not in EXECUTION_PLAN are placed at the end.
        
        Args:
            payloads: Dictionary of payloads with entity keys
            
        Returns:
            Ordered dictionary following EXECUTION_PLAN sequence
        """
        if not payloads:
            return payloads
        
        ordered_payloads = {}
        # First add keys in EXECUTION_PLAN order
        for _, payload_key in DisableUsersProcessor.EXECUTION_PLAN:
            if payload_key in payloads:
                ordered_payloads[payload_key] = payloads[payload_key]
        
        # Then add any remaining keys not in EXECUTION_PLAN
        for key, value in payloads.items():
            if key not in ordered_payloads:
                ordered_payloads[key] = value
        
        return ordered_payloads

    def __init__(self, inactive_user_df: pd.DataFrame, auth_url: str, auth_credentials: dict, base_url: str, exit_events: dict, max_retries: int = 5):
        self.inactive_user_df = inactive_user_df
        self.inactive_users_list = self.inactive_user_df['userid'].tolist()
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
            "EmpEmploymentTermination": {},
            "User": {},
        }
        self.exit_events = exit_events
    def process_disable_users(self):
        """
        Process user deactivation and employment termination.
        
        Steps:
        1. For each user, build termination and deactivation payloads
        2. Collect payloads for batch upsert
        3. Execute batched upserts per entity
        
        Returns:
            Dict[str, UserExecutionContext]: Mapping of user_id to their execution context after processing.
        """
        try:
            results = {}
            Logger.info(f"Processing {len(self.inactive_users_list)} users for deactivation")

            # Process each user and collect payloads
            for user_id in self.inactive_users_list:
                ctx = UserExecutionContext(user_id)
                ctx.runtime["entity_status"] = {
                    entity: "PENDING" for entity, _ in self.EXECUTION_PLAN
                }
                
                try:
                    # Find user record
                    user_mask = self.inactive_user_df['userid'] == user_id
                    user_rows = self.inactive_user_df[user_mask]
                    
                    if user_rows.empty:
                        ctx.fail(f"No data found for user {user_id} in inactive users dataframe")
                        results[user_id] = ctx
                        continue
                    
                    row = user_rows.iloc[0]
                    ctx.is_scm = row.get("is_peoplehub_scm_manually_included", "N") == "Y"
                    ctx.is_im = row.get("is_peoplehub_im_manually_included", "N") == "Y"
                    
                    # Build payloads
                    self._handle_employment_termination(row, ctx)
                    if not ctx.has_errors:
                        self._handle_disable_user(ctx)
                    
                    # Collect payloads for batch upsert
                    self._collect_payloads(ctx)
                    results[user_id] = ctx
                    
                except Exception as e:
                    Logger.error(f"Error processing user {user_id}: {e}")
                    ctx.fail(f"Error processing user {user_id}: {e}")
                    results[user_id] = ctx

            # Execute batched upserts per entity
            Logger.info(f"Executing batch upserts for {len(results)} users")
            self._execute_batch_upserts(results)

            return results
            
        except Exception as e:
            Logger.error(f"Fatal error during disable users processing: {e}")
            raise

    def _handle_employment_termination(self, row: pd.Series, ctx: UserExecutionContext):
        """
        Build EmpEmploymentTermination payload for a user.
        
        Args:
            row (pd.Series): The data row for the user containing termination details
            ctx (UserExecutionContext): The execution context of the user
        """
        try:
            user_id = ctx.user_id            
            end_date = row.get('date_of_leave')
            terminate_event_id = row.get('exit_reason_id')
            
            if pd.isna(end_date):
                ctx.fail(f"Missing end_date (date_of_leave) for user {user_id}")
                return
            
            if pd.isna(terminate_event_id):
                ctx.fail(f"Missing terminate_event_id (exit_reason_id) for user {user_id}")
                return
            
            terminate_event_reason = self.exit_events.get(terminate_event_id)
            if not terminate_event_reason:
                ctx.fail(f"Invalid terminate_event_id {terminate_event_id} for user {user_id}")
                return

            emp_builder = EmploymentTerminationPayloadBuilder(
                user_id=user_id,
                end_date=end_date,
                terminate_event_reason=terminate_event_reason
            )
            
            payload = emp_builder.build_emp_employment_termination_payload()
            
            if not payload:
                ctx.fail(f"Failed to build EmpEmploymentTermination payload for user {user_id}")
                return
            
            ctx.payloads["emp_termination"] = payload
            Logger.info(f"✓ EmpEmploymentTermination payload built for user {user_id}")
            
        except Exception as e:
            ctx.fail(f"Error building employment termination for user {ctx.user_id}: {e}")

    def _handle_disable_user(self, ctx: UserExecutionContext):
        """
        Build User inactive payload for a user.
        
        Args:
            ctx (UserExecutionContext): The execution context of the user
        """
        try:
            user_id = ctx.user_id            
            payload = build_user_inactive_payload(user_id)
            
            if not payload:
                ctx.fail(f"Failed to build User inactive payload for user {user_id}")
                return
            
            ctx.payloads["user_inactive"] = payload
            Logger.info(f"✓ User inactive payload built for user {user_id}")
            
        except Exception as e:
            ctx.fail(f"Error building user inactive payload for user {ctx.user_id}: {e}")

    def _collect_payloads(self, ctx: UserExecutionContext):
        """
        Collect payloads from user execution context for batch upserts.
        
        Args:
            ctx (UserExecutionContext): The execution context of the user
        """
        for entity_name, payload_key in self.EXECUTION_PLAN:
            if payload_key not in ctx.payloads:
                continue

            payload = ctx.payloads[payload_key]
            if isinstance(payload, dict):
                payload = [payload]

            self.collected_payloads[entity_name][ctx.user_id] = payload

    def _execute_batch_upserts(self, results: dict):
        """
        Execute batched upserts per entity (SAP-compliant).
        
        Args:
            results (Dict[str, UserExecutionContext]): Mapping of user_id to their execution context
        """
        try:
            for entity_name, _ in self.EXECUTION_PLAN:
                payloads_per_user = self.collected_payloads.get(entity_name)
                if not payloads_per_user:
                    continue

                eligible_payloads = {}

                for user_id, payload in payloads_per_user.items():
                    ctx = results[user_id]

                    if ctx.has_errors:
                        Logger.info(f"{entity_name} skipped for {user_id}: has_errors=True, errors={ctx.errors}")
                        ctx.runtime["entity_status"][entity_name] = "SKIPPED"
                        continue

                    eligible_payloads[user_id] = payload

                if not eligible_payloads:
                    Logger.info(f"No eligible users for {entity_name}")
                    continue

                Logger.info(f"Upserting {entity_name} for {len(eligible_payloads)} users")
                responses = self.upsert_client.upsert_entity_for_users(
                    entity_name=entity_name, user_payloads=eligible_payloads
                )

                for user_id, result in responses.items():
                    ctx = results[user_id]
                    ctx.runtime[entity_name] = result

                    if result["status"] == "FAILED":
                        ctx.runtime["entity_status"][entity_name] = "FAILED"
                        
                        # Build detailed error message with API response
                        error_details = f"{entity_name} failed - Message: {result.get('message')}"
                        if result.get('httpCode'):
                            error_details += f", HTTP Code: {result.get('httpCode')}"
                        if result.get('key'):
                            error_details += f", Key: {result.get('key')}"
                        
                        ctx.fail(error_details)
                    else:
                        ctx.runtime["entity_status"][entity_name] = "SUCCESS"
                        Logger.info(f"{entity_name} upsert succeeded for user {user_id}")

                # Mark any remaining PENDING users as SKIPPED
                for user_id, ctx in results.items():
                    if ctx.runtime["entity_status"][entity_name] == "PENDING":
                        ctx.runtime["entity_status"][entity_name] = "SKIPPED"
            
            # Log summary grouped by user
            self._log_batch_summary_by_user(results)

        except Exception as e:
            Logger.error(f"Fatal error during batch upserts: {e}")
            raise

    def _log_batch_summary_by_user(self, results: dict):
        """
        Log a summary of batch processing results grouped by user.
        Shows which entities were processed successfully/failed for each user.
        Includes payloads for failed entities.
        """
        Logger.info("\n" + "="*80)
        Logger.info("USER DEACTIVATION SUMMARY (BY USER)")
        Logger.info("="*80)
        
        for user_id, ctx in results.items():
            entity_statuses = ctx.runtime.get("entity_status", {})
            processed_entities = {k: v for k, v in entity_statuses.items() if v != "PENDING"}
            
            if not processed_entities:
                continue
                
            Logger.info(f"\n{'─'*80}")
            Logger.info(f"User: {user_id}")
            Logger.info(f"{'─'*80}")
            
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
                else:
                    Logger.error("    - No error messages recorded.")
                
                # Log payloads for failed entities
                for entity in failed:
                    payload_key = None
                    for entity_name, key in self.EXECUTION_PLAN:
                        if entity_name == entity:
                            payload_key = key
                            break
                    
                    if payload_key and payload_key in ctx.payloads:
                        payload = ctx.payloads[payload_key]
                        try:
                            pretty_payload = json.dumps(payload, indent=2)
                            Logger.error(f"    Failed payload for {entity}:\n{pretty_payload}")
                        except (TypeError, ValueError):
                            Logger.error(f"    Failed payload for {entity}: {payload}")
            
            # Show warnings if any exist
            if ctx.warnings:
                Logger.warning("  ⚠ WARNINGS:")
                for warning in ctx.warnings:
                    Logger.warning(f"    - {warning}")
            
            if skipped:
                Logger.info(f"  ⊘ SKIPPED: {', '.join(skipped)}")
        
        Logger.info("\n" + "="*80)

    def extract_history_data(self, results: dict) -> dict:
        """
        Extract history data from processing results for database logging.
        
        Args:
            results: Dict[str, UserExecutionContext] from processing
            
        Returns:
            dict with keys:
                - results: List of failure records for user_sync_results table
                - success_count: Number of successful operations
                - warning_count: Number of warnings
                - failed_count: Number of failed operations
        """
        results_list = []
        success_count = 0
        warning_count = 0
        for user_id, ctx in results.items():
            # Count successes and warnings
            if not ctx.has_errors:
                success_count += 1
                if ctx.has_warnings:
                    warning_count += 1
            
            # Collect failures
            if ctx.has_errors:
                for error_msg in ctx.errors:
                    results_list.append({
                        'user_id': user_id,
                        'operation': 'TERMINATE',
                        'status': 'FAILED',
                        'error_message': error_msg,
                        'is_scm': ctx.is_scm if hasattr(ctx, 'is_scm') else None,
                        'is_im': ctx.is_im if hasattr(ctx, 'is_im') else None,
                        'payload_snapshot': self._order_payloads(ctx.payloads) if ctx.payloads else None
                    })
            
            # Collect warnings (even if operation succeeded)
            if ctx.has_warnings and not ctx.has_errors:
                for warn_msg in ctx.warnings:
                    results_list.append({
                        'user_id': user_id,
                        'operation': 'TERMINATE',
                        'status': 'WARNING',
                        'warning_message': warn_msg,
                        'is_scm': ctx.is_scm if hasattr(ctx, 'is_scm') else None,
                        'is_im': ctx.is_im if hasattr(ctx, 'is_im') else None,
                        'payload_snapshot': self._order_payloads(ctx.payloads) if ctx.payloads else None
                    })
            # Collect Successes without warnings/errors
            if not ctx.has_errors and not ctx.has_warnings:
                results_list.append({
                    'user_id': user_id,
                    'operation': 'TERMINATE',
                    'status': 'SUCCESS',
                    'error_message': None,
                    'is_scm': ctx.is_scm if hasattr(ctx, 'is_scm') else None,
                    'is_im': ctx.is_im if hasattr(ctx, 'is_im') else None,
                    'payload_snapshot': self._order_payloads(ctx.payloads) if ctx.payloads else None
                })
        return {
            'results': results_list,
            'success_count': success_count,
            'warning_count': warning_count,
            'failed_count': len([f for f in results_list if f['status'] == 'FAILED'])
        }