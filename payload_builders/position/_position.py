from utils.logger import get_logger
from mapper.country_mapper import get_iso3_numeric
from payload_builders.position.payloads.position import get_position_payload
from payload_builders.position.payloads.position_matrix_relationship import get_position_matrix_relationship_payload
from cache.postgres_cache import PostgresDataCache
from cache.sap_cache import SAPDataCache
from config.wh_per_country import wh_per_country
import pandas as pd

Logger = get_logger("build_position_payloads")


class PositionPayloadBuilder:
    """ 
    Builds payloads for Position and PositionMatrixRelationships entities.
    Handles validation, field mapping, and payload construction.
    Supports both creation and update scenarios.
    Args:
        record (dict): The user record containing position data.
        job_mappings (pd.DataFrame): DataFrame containing job mapping data.
        results (dict): Dictionary to hold results for position code tracking.
        ec_user_id (str): The EC USERID of the user being processed.
        non_missing_manager_data (pd.DataFrame, optional): DataFrame with non-missing manager data.
        is_scm (bool, optional): Flag indicating if the user is SCM. Defaults to False.
        is_update (bool, optional): Flag indicating if this is an update operation. Defaults to False.

    """


    REQUIRED_FIELDS = [
        "jobcode",
        "address_code",
        "cost_center",
        "country_code",
        "company",
    ]

    def __init__(
        self,
        record: dict,
        job_mappings,
        results: dict,
        ec_user_id: str ,
        non_missing_manager_data=None,
        is_scm: bool = False,
        is_update: bool = False,
    ):
        self.record = record
        self.job_mappings = job_mappings
        self.non_missing_manager_data = non_missing_manager_data
        self.is_scm = is_scm
        self.is_update = is_update
        # Extract user_id from record for position lookup validation
        self.user_id = record.get("userid") if isinstance(record, dict) else getattr(record, "userid", None)
        self.postgres_cache = PostgresDataCache()
        self.sap_cache = SAPDataCache()
        self.results = results if results is not None else {}
        self.ec_user_id = ec_user_id
        self.positions_df = self.sap_cache.get("positions_df")
        self.employees_df = self.sap_cache.get("employees_df")
        self.last_error = None
        self.position_code = None

    @staticmethod
    def _clean(value):
        if value is None:
            return ""
        return str(value).replace(".0", "")

    def _validate(self):
        for field in self.REQUIRED_FIELDS:
            if field not in self.record:
                raise ValueError(f"Missing required field: {field}")

        required_cols = ["bufu_id", "cust_geographicalscope", "cust_subunit"]
        if not all(col in self.job_mappings.columns for col in required_cols):
            raise ValueError("Invalid job_mappings")

    def _apply_base_fields(self, payload):
        fields = {
            "company": self._clean(self.record["company"]),
            "costCenter": self._clean(self.record["cost_center"]),
            "cust_Country_Of_Registration": self._clean(self.record.get("country_iso3") or get_iso3_numeric(self.record.get("country_code"))),
            "division": self._clean(self.job_mappings["bufu_id"].values[0]),
            "jobCode": self._clean(self.record["jobcode"]),
            "location": self._clean(self.record["address_code"]),
            "cust_geographicalScope": self._clean(self.job_mappings["cust_geographicalscope"].values[0]),
            "cust_subUnit": self._clean(self.job_mappings["cust_subunit"].values[0]),
        }
        # Only add fields that are not empty strings
        payload.update({k: v for k, v in fields.items() if v != ""})

    def _get_userid_from_personid(self, person_id):
        """Retrieve USERID from PERSON_ID_EXTERNAL using cached data if they are different."""
        different_userid_personid_df = self.postgres_cache.get(
            "different_userid_personid_data_df"
        )
        if different_userid_personid_df is not None:
            match = different_userid_personid_df[
                different_userid_personid_df["person_id_external"] == person_id
            ]
            if not match.empty:
                return match["userid"].values[0]
        return person_id

    def _get_position_code_from_employees(self, userid, dummy_position: str=None):
        """Retrieve manager's position code using cached EC data."""
        employees_df = self.employees_df
        if employees_df is not None:
            match = employees_df[employees_df["userid"] == userid]
            if not match.empty:
                if dummy_position and str(match["position"].values[0]).strip().lower() == dummy_position.strip().lower():
                    Logger.info(f"Position code for user ID {userid} is a dummy position ({dummy_position}), skipping assignment.")
                    return None
                return match["position"].values[0]
        return None

    def _get_position_code_from_positions(self,record,ec_user_id: str=None):
        """Retrieve position code using cached positions data."""
        
        try:
            pos_data = self.positions_df
            job_code = str(record.get('jobcode')).strip().lower()
            location_code = str(record.get('address_code')).strip().lower()
            cost_center = str(record.get('cost_center')).strip().lower()
            company = str(record.get('company')).strip().lower()
            if 'positioncriticality' in pos_data.columns:
                pos_data['_is_critical'] = (
                    pd.to_numeric(
                        pos_data['positioncriticality'],
                        errors='coerce'
                    )
                    .fillna(0)
                    .astype(int)
                    == 1
                )
            else:
                pos_data['_is_critical'] = False
            mask = (
                (pos_data['jobcode'].astype(str).str.lower() == job_code) &
                (pos_data['location'].astype(str).str.lower() == location_code) &
                (pos_data['costcenter'].astype(str).str.lower() == cost_center) &
                (pos_data['company'].astype(str).str.lower() == company) &
                (~pos_data['_is_critical'])  # Exclude critical positions
            )

            result = pos_data[mask]

            if not result.empty:
                for code in result['code']:
                    emp_mask = self.employees_df['position'].astype(str).str.lower().eq(code.lower())
                    emp_result = self.employees_df[emp_mask]
                    # Check if position is assigned to another user
                    if not emp_result.empty:
                        assigned_userid = str(emp_result['userid'].values[0]).strip().lower()
                        current_userid = str(ec_user_id if ec_user_id else self.ec_user_id).strip().lower()
                        # Check if this position is assigned to a different user
                        if assigned_userid != current_userid:
                            Logger.info(
                                f"Position code {code} is already assigned to another user ID {assigned_userid}."
                            )
                    in_batch = False
                    # Check if position code is assigned to another user in current batch
                    results_df = pd.DataFrame.from_dict(
                        {
                            user_id: ctx.position_code
                            for user_id, ctx in self.results.items()
                            if ctx.position_code
                        },
                        orient="index",
                        columns=["position_code"]
                    ).reset_index(names="user_id")
                    results_df["user_id"] = results_df["user_id"].astype(str).str.lower()
                    results_df["position_code"] = results_df["position_code"].astype(str).str.lower()
                    code_l = code.lower()
                    current_user_l = str(self.user_id).strip().lower()
                    in_batch = (
                        (results_df["position_code"] == code_l) &
                        (results_df["user_id"] != current_user_l)
                    ).any()

                    if in_batch:
                        Logger.info(
                            f"Position code {code} is already assigned to another user in current processing batch."
                        )
                        continue
                    Logger.info(f"Position code {code} found for user ID {self.user_id} and not assigned to another user.")
                    return code

            Logger.info(f"No position code found for user ID {self.user_id}.")
            return None
        except Exception as e:
            msg = f"Error checking position code existence for user ID {self.user_id}: {str(e)}"
            Logger.error(msg)
            return None
        
    def _get_relation_position_from_context(self, relation_ec_userid: str):
        """Check if Relation position code is present in current results context."""
        
        try:
            user_id = str(relation_ec_userid).strip().lower()
            ctx = self.results.get(user_id)
    
            if ctx and ctx.position_code:
                position_code = str(ctx.position_code).strip().lower()
                Logger.info(
                    f"Position code {position_code} found in context for Relation user ID {relation_ec_userid}."
                )
                return position_code
            return None
    
        except Exception:
            Logger.error(
                f"Error checking position code in context for Relation user ID {relation_ec_userid}",
                exc_info=True
            )
            return None
    
    def _get_position_uri(self, position_code):
        """Retrieve position URI using cached positions data."""
        positions_df = self.positions_df
        if positions_df is not None:
            match = positions_df[positions_df["code"] == position_code]
            if not match.empty:
                # Try common metadata formats
                if "__metadata.uri" in match.columns:
                    return match["__metadata.uri"].values[0]
                # Some caches store __metadata as a dict/object column
                if "__metadata" in match.columns:
                    try:
                        meta = match["__metadata"].values[0]
                        if isinstance(meta, dict) and meta.get("uri"):
                            return meta.get("uri")
                    except Exception:
                        Logger.warning(f"Could not extract uri from __metadata for position {position_code}")
                # Fallback to common alternative column names
                for col in ["uri", "position_uri", "__metadata_uri"]:
                    if col in match.columns:
                        return match[col].values[0]
                Logger.warning(f"No position URI found for code {position_code} - metadata column missing or unexpected format")
        return None

    def get_position_data(self, position_code):
        """Retrieve effective start date and standard hours for a position."""
        positions_df = self.positions_df
        if positions_df is not None:
            match = positions_df[positions_df["code"] == position_code]
            if not match.empty:
                # Check if columns exist before accessing
                Logger.info(f"Retrieving position data for position code: {position_code}")
                Logger.info(f"Position data columns: {match.columns.tolist()}")
                if "effectivestartdate" in match.columns and "standardhours" in match.columns:
                    effective_start = match["effectivestartdate"].values[0]
                    standard_hours = match["standardhours"].values[0]
                    return effective_start, standard_hours
                else:
                    Logger.warning(f"Missing effectiveStartDate or standardHours columns for position {position_code}")
        # Return default values
        return "/Date(-2208988800000)/", wh_per_country.get(
            (self.record.get("country_iso3") or get_iso3_numeric(self.record.get("country_code"))), 40
        )

    def _apply_scm_fields(self, payload):
        if self.is_scm:
            payload["positionCriticality"] = "1"

    def _apply_manager(self, payload):
        manager_id = self.record.get("manager")
        if not manager_id:
            Logger.debug(f"No manager for user {self.record.get('userid')}, skipping manager fields")
            return
        
        manager_userid = self._get_userid_from_personid(manager_id)
        if not manager_userid:
            Logger.warning(f"Could not resolve manager personid {manager_id} to userid for user {self.record.get('userid')}")
            return
        
        # Try to get manager position from employees cache
        manager_position_code = self._get_position_code_from_employees(manager_userid)
        
        # If not found, try from positions cache using non_missing_manager_data
        if not manager_position_code:
            manager_position_code = self._get_relation_position_from_context(manager_userid)
        
        # If we found the manager position code, use parentPosition
        if manager_position_code:
            parent_uri = self._get_position_uri(manager_position_code)
            if parent_uri:
                payload["parentPosition"] = {"__metadata": {"uri": parent_uri}}
                return
        
        # Fallback: if no position code found, use cust_Supervisor with userid
        Logger.debug(f"No position code found for manager {manager_userid}, using cust_Supervisor fallback")
        payload["cust_Supervisor"] = self._clean(manager_userid)

    def build_position(self, sync_pos_to_emp: bool = False, effective_start_date_=None, position_code_=None, dummy_position: str=None):
        try:
            self._validate()
            payload = get_position_payload()
            position_code = None
            
            # Mode 1: SYNC_TO_JOB - Update position to trigger SAP PositionToJobInfoSyncRule
            if sync_pos_to_emp:
                if not position_code_ or not effective_start_date_:
                    raise ValueError("sync_pos_to_emp requires position_code_ and effective_start_date_")
                payload["code"] = self._clean(position_code_)
                payload["effectiveStartDate"] = effective_start_date_
                # Get existing standardHours for this position
                _, standard_hours = self.get_position_data(position_code_)
                payload["standardHours"] = self._clean(standard_hours)
            
            # Mode 2: UPDATE - Update existing position fields
            elif self.is_update:
                if not position_code_:
                    position_code = self._get_position_code_from_employees(self.record["userid"], dummy_position=dummy_position)
                    if not position_code:
                        position_code = self._get_position_code_from_positions(self.record)
                else:
                    position_code = position_code_
                
                if not position_code:
                    # Fallback to CREATE if position doesn't exist
                    self.is_update = False
                    payload["effectiveStartDate"] = "/Date(-2208988800000)/"
                    payload["standardHours"] = "40"
                else:
                    self.position_code = position_code
                    payload["code"] = self._clean(position_code)
                    effective_start, standard_hours = self.get_position_data(position_code)
                    payload["effectiveStartDate"] = effective_start
                    payload["standardHours"] = self._clean(standard_hours)
            
            # Mode 3: CREATE - New position creation
            else:
                payload["effectiveStartDate"] = "/Date(-2208988800000)/"
                payload["standardHours"] = "40"
            
            # COMMON fields for all modes
            self._apply_base_fields(payload)
            self._apply_scm_fields(payload)
            self._apply_manager(payload)
            return payload
        except Exception as e:
            self.last_error = str(e)
            Logger.error(f"Position payload build failed for user {self.record.get('userid', 'UNKNOWN')}: {e}", exc_info=True)
            Logger.error(f"Failed to build position payload for user {self.record.get('userid', 'UNKNOWN')}. Check record fields: jobcode={self.record.get('jobcode')}, manager={self.record.get('manager')}, company={self.record.get('company')},cost_center={self.record.get('cost_center')},country_code={self.record.get('country_code')}")
            return None
    
    def build_position_matrix_relationship_payload(self,relation_userid: str,relation_type: str, user_position_code_= None):
        """
        Alternative method to garantee the link between user and his related relations
        by using Position Matrix Relationship payload which requires:
        - Position_code: User Position Code
        - Position_effectiveStartDate: User Position Effective Start Date
        - matrixRelationshipType: Relationship Type
        - relatedPosition: Position of the relation
        """
        try:
            if not self.record["userid"] or not relation_userid or not relation_type:
                Logger.error("Missing required fields for Position Matrix Relationship payload")
                return None
            payload = get_position_matrix_relationship_payload()

            # Get User Position Code
            user_position_code = user_position_code_ if user_position_code_ is not None else None
            if not user_position_code:
                if self.is_update:
                    user_position_code = self._get_position_code_from_employees(
                        self.record["userid"]
                    )

            if not user_position_code:
                Logger.error("Unable to determine User Position Code")
                return None

            payload["Position_code"] = self._clean(user_position_code)

            # Get User Position Effective Start Date
            effective_start, _ = self.get_position_data(user_position_code)
            payload["Position_effectiveStartDate"] = effective_start

            # Set matrixRelationshipType using the relation_type parameter
            payload["matrixRelationshipType"] = self._clean(relation_type)
            
            #relatedPosition_code
            relation_userid_ = self._get_userid_from_personid(relation_userid)
            if not relation_userid_:
                Logger.error("Unable to determine related USERID")
                return None
            related_position_code = self._get_position_code_from_employees(relation_userid_)
            if not related_position_code:
                related_position_code = self._get_relation_position_from_context(relation_userid_)
            if not related_position_code:
                Logger.error("Unable to determine Related Position Code")
                return None
            payload["relatedPosition"] = related_position_code

            return payload
        except Exception as e:
            Logger.error(f"Position Matrix Relationship payload build failed: {e}")
            return None