from utils.logger import get_logger
import pandas as pd
from cache.sap_cache import SAPDataCache

logger = get_logger('employment_existence_validator')

class EmploymentExistenceValidator:
    """
    Validator class to check the existence of employement records in SAP system 
    from the cached employment mappings.
    
    """

    def __init__(self, user_id: str, ec_user_id: str, results: dict, raise_if_missing=False):
        self.emp_data = SAPDataCache().get("employees_df")
        self.pos_data = SAPDataCache().get("positions_df")
        self.user_id = user_id
        self.ec_user_id = ec_user_id
        self.results = results
        self.raise_if_missing = raise_if_missing

    def get_job_mapping(self) -> pd.DataFrame:
        """
        Retrieves the employment mapping for the specified user ID.
        Returns:
            pd.DataFrame: DataFrame containing the employment mapping : position and seqNumber.
            if the user ID does not exist and raise_if_missing is True, raises a ValueError.
            if the user ID does not exist and raise_if_missing is False, returns an empty DataFrame.
        """
        try:
            mask = self.emp_data['userid'].astype(str).str.lower().eq(self.ec_user_id.lower())
            result = self.emp_data[mask].copy()

            if result.empty:
                msg = f"User ID {self.user_id} not found in employment data."
                if self.raise_if_missing:
                    logger.error(msg)
                    raise ValueError(msg)
                logger.info(msg)
            else:
                logger.info(f"User ID {self.user_id} exists in SAP system.")
                result = result[['position','seqnumber', 'startdate']]
        
            return result
        except Exception as e:
            logger.error(f"Error retrieving job mapping for user {self.user_id}: {str(e)}")
            return pd.DataFrame()
    def get_additional_position_info(self, position_code):
        """Retrieve additional position info like jobCode, division, standardHours, jobTitle, location."""
        employees_df = self.emp_data
        try:
            if employees_df is not None:
                match = employees_df[employees_df["position"] == position_code]
                if not match.empty:
                    job_code = match["jobcode"].values[0]
                    division = match["division"].values[0]
                    job_title = match["jobTitle"].values[0]
                    location = match["location"].values[0]
                    return {
                        "jobcode": job_code,
                        "division": division,
                        "job_title": job_title,
                        "location_code": location
                    }
        except Exception as e:
            logger.error(f"Error retrieving additional position info for position {position_code}: {str(e)}")
        return {
            "jobcode": None,
            "division": None,
            "job_title": None,
            "location_code": None
        }
    
    def get_position_code_from_positions(self,record) -> str | None:
        """
        Returns the position code if it exists and is not assigned to another user.
        """
        try:

            job_code = str(record.get('jobcode')).strip().lower()
            location_code = str(record.get('address_code')).strip().lower()
            cost_center = str(record.get('cost_center')).strip().lower()
            company = str(record.get('company')).strip().lower()
            if 'positioncriticality' in self.pos_data.columns:
                self.pos_data['_is_critical'] = (
                    pd.to_numeric(
                        self.pos_data['positioncriticality'],
                        errors='coerce'
                    )
                    .fillna(0)
                    .astype(int)
                    == 1
                )
            else:
                self.pos_data['_is_critical'] = False

            mask = (
                (self.pos_data['jobcode'].astype(str).str.lower() == job_code) &
                (self.pos_data['location'].astype(str).str.lower() == location_code) &
                (self.pos_data['costcenter'].astype(str).str.lower() == cost_center) &
                (self.pos_data['company'].astype(str).str.lower() == company) &
                (~self.pos_data['_is_critical'] )  # Exclude critical positions which belong maybe to SCM
            )

            result = self.pos_data[mask]

            if not result.empty:
                for code in result['code']:
                    emp_mask = self.emp_data['position'].astype(str).str.lower().eq(code.lower())
                    emp_result = self.emp_data[emp_mask]

                    if not emp_result.empty:
                        assigned_userid = emp_result['userid'].values[0]
                        if assigned_userid.lower() != self.ec_user_id.lower():
                            logger.info(
                                f"Position code {code} is already assigned to another user ID {assigned_userid}."
                            )
                            continue 
                    # Check if position code is assigned to another user in current batch
                    in_batch = False
                    results_df = pd.DataFrame.from_dict(
                        {
                            user_id: ctx.position_code
                            for user_id, ctx in self.results.items()
                            if ctx.position_code
                        },
                        orient="index",
                        columns=["position_code"]
                    ).reset_index(names="user_id")
                    results_df["user_id"] = results_df["user_id"].str.lower()
                    results_df["position_code"] = results_df["position_code"].str.lower()
                    code_l = code.lower()
                    in_batch = (
                        (results_df["position_code"] == code_l) &
                        (results_df["user_id"] != self.user_id.lower())
                    ).any()

                    if in_batch:
                        logger.info(
                            f"Position code {code} is already assigned to another user in current processing batch."
                        )
                        continue
                    logger.info(f"Position code {code} found for user ID {self.user_id} and not assigned to another user.")
                    return code

            logger.info(f"No position code found for user ID {self.user_id}.")
            return None

        except Exception as e:
            msg = f"Error checking position code existence for user ID {self.user_id}: {str(e)}"
            if self.raise_if_missing:
                logger.error(msg)
                raise ValueError(msg)
            logger.info(msg)
            return None