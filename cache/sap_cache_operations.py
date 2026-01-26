from cache.sap_cache import SAPDataCache
from extractor.sap_info_cache_handler import SAPInfoCacheHandler
from utils.logger import get_logger

Logger = get_logger('sap_cache_operations')


def update_position_cache(base_url: str, auth_url: str, auth_credentials: dict):
    """
    Updates the position data cache.

    Args:
        base_url (str): The base URL for the SAP API.
        auth_url (str): The authentication URL for the SAP API.
        auth_credentials (dict): A dictionary containing authentication credentials.
    """    
    SAPDataCache.clear_key("positions_df")
    Logger.info("Positions cache cleared.")
    
    # Refetch and store updated positions data
    handler = SAPInfoCacheHandler(
        base_url=base_url,
        auth_url=auth_url,
        auth_credentials=auth_credentials,
        max_retries=5
    )
    handler.extract_and_cache_sap_data(position_flag=True)
    Logger.info("Position cache updated successfully.")


def update_empjob_cache(base_url: str, auth_url: str, auth_credentials: dict):
    """
    Updates the employment job data cache.
    
    Args:
        base_url (str): The base URL for the SAP API.
        auth_url (str): The authentication URL for the SAP API.
        auth_credentials (dict): A dictionary containing authentication credentials.
    """      
    SAPDataCache.clear_key("employees_df")
    Logger.info("Employees cache cleared.")
    
    # Refetch and store updated employees data
    handler = SAPInfoCacheHandler(
        base_url=base_url,
        auth_url=auth_url,
        auth_credentials=auth_credentials,
        max_retries=5
    )
    handler.extract_and_cache_sap_data(empjob_flag=True)
    Logger.info("Employee job cache updated successfully.")
