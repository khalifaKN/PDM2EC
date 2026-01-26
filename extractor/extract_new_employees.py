from utils.logger import get_logger
import pandas as pd
from cache.employees_cache import EmployeesDataCache

logger = get_logger('new_employees_extractor')

class NewEmployeesExtractor:
    """
    Extractor class to identify new employees in PDM not present in SAP.
    """

    def __init__(self, pdm_users: pd.DataFrame, sap_users: pd.DataFrame, is_migration: bool = False):
        self.pdm_users = pdm_users
        self.sap_users = sap_users
        self.is_migration = is_migration
        
    def extract_new_employees(self) -> pd.DataFrame:
        """
        Extracts new employees present in PDM but not in SAP.

        Returns:
            pd.DataFrame: DataFrame containing new employees.
        """
        try:
            # Normalize email columns to lowercase for accurate comparison
            pdm_users_id = self.pdm_users['userid'].astype(str).str.lower()
            sap_users_id = self.sap_users['userid'].astype(str).str.lower()
            

            # Identify new employees
            new_employees_= self.pdm_users[~pdm_users_id.isin(sap_users_id)].copy()
            # Exclude any user not IM/SCM
            if not self.is_migration:
                new_employees = new_employees_[
                    (new_employees_['is_peoplehub_im_manually_included'] == 'Y') |
                    (new_employees_['is_peoplehub_scm_manually_included'] == 'Y')
                ]
                # Count Excluded users
                excluded_count = len(new_employees_) - len(new_employees)
                if excluded_count > 0:
                    logger.info(f"Excluded {excluded_count} new employees not flagged as IM/SCM.")
                else:
                    logger.info("No new employees excluded; all are flagged as IM/SCM.")
            else:
                new_employees = new_employees_
            # Cache the new employees data
            employees_cache = EmployeesDataCache()
            employees_cache.set('new_employees_df', new_employees)
            logger.info(f"Extracted {len(new_employees)} new employees from PDM data.")
            return new_employees, len(new_employees)
        except Exception as e:
            logger.error(f"Error extracting new employees: {e}")
            raise e