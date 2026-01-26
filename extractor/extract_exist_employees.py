from utils.logger import get_logger
import pandas as pd

logger = get_logger('exist_employees_extractor')

class ExistEmployeesExtractor:
    """
    Extractor class to identify existing employees present in both PDM and SAP.
    """

    def __init__(self, pdm_users: pd.DataFrame, sap_users: pd.DataFrame):
        self.pdm_users = pdm_users
        self.sap_users = sap_users
        
    def extract_existing_employees(self) -> pd.DataFrame:
        """
        Extracts existing employees present in both PDM and SAP.

        Returns:
            pd.DataFrame: DataFrame containing existing employees.
        """
        try:
            # Convert userid to string and normalize to lowercase for accurate comparison
            pdm_users_id = self.pdm_users['userid'].astype(str).str.lower()
            sap_users_id = self.sap_users['userid'].astype(str).str.lower()

            # Identify existing employees
            existing_employees = self.pdm_users[pdm_users_id.isin(sap_users_id)].copy()

            logger.info(f"Extracted {len(existing_employees)} existing employees from PDM data.")
            return existing_employees
        except Exception as e:
            logger.error(f"Error extracting existing employees: {e}")
            raise e