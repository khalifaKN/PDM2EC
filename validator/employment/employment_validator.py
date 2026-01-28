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
    
    def position_code_exists_in_employees(self) -> str:
        """
        Checks if the position code exists for the given user ID in the employment data.
        Returns:
            str: The position code if it exists, else an empty string.
        """
        try:
            mask = self.emp_data['userid'].astype(str).str.lower().eq(self.ec_user_id.lower())
            result = self.emp_data[mask]
            if not result.empty:
                position_code = result['position'].values[0]
                logger.info(f"Position code {position_code} found for user ID {self.user_id}.")
                return position_code
            logger.info(f"No position code found for user ID {self.user_id}.")
            return None
        except Exception as e:
            msg = f"Error checking position code existence for user ID {self.user_id}: {str(e)}"
            if self.raise_if_missing:
                logger.error(msg)
                raise ValueError(msg)
            logger.info(msg)
            return None