from utils.logger import get_logger
import pandas as pd

logger = get_logger('inactive_employees_extractor')

class InactiveEmployeesExtractor:
    """
    Extractor class to identify inactive employees: Exist in SAP EC and not in PDM.
    """

    def __init__(self, pdm_users: pd.DataFrame, sap_users: pd.DataFrame):
        self.pdm_users = pdm_users
        self.sap_users = sap_users
        
    def extract_inactive_employees(self) -> pd.DataFrame:
        """
        Extracts inactive employees present in SAP but not in PDM.

        Returns:
            pd.DataFrame: DataFrame containing inactive employees.
        """
        try:
            # Convert userid to string and normalize to lowercase for accurate comparison
            pdm_users_id = self.pdm_users['userid'].astype(str).str.lower()
            sap_users_id = self.sap_users['userid'].astype(str).str.lower()

            # Identify existing employees
            inactive_employees = self.sap_users[~sap_users_id.isin(pdm_users_id)].copy()

            logger.info(f"Extracted {len(inactive_employees)} inactive employees from SAP data.")
            return inactive_employees
        except Exception as e:
            logger.error(f"Error extracting inactive employees: {e}")
            raise e