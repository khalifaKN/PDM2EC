from cache.sap_cache import SAPDataCache
from utils.logger import get_logger
from config.sf_apis import uris_params,get_apis
from api.api_client import APIClient
from api.auth_client import AuthAPI
from utils.extract_params import extract_sap_params_safe
import pandas as pd

Logger = get_logger('sap_data_extraction_test')


class SAPInfoCacheHandler:
    """
    Handler class to extract and cache SAP data.
    """

    def __init__(self,base_url: str,auth_url: str, auth_credentials: dict, max_retries: int = 5,entites: list = None):
        self.base_url = base_url
        self.auth_url = auth_url
        self.client_id = auth_credentials['client_id']
        self.client_secret = auth_credentials['assertion']
        self.grant_type = auth_credentials['grant_type']
        self.company_id = auth_credentials['company_id']
        self.max_retries = max_retries
        self.sap_cache = SAPDataCache()
        self.entities = entites if entites else ['positions', 'employees', 'perPerson', 'perPersonal']

    def _fetch_cache_sap_data(self, entity: str, api_client: APIClient) -> pd.DataFrame:
        """
        Fetches and caches SAP data for a given entity.
        Args:
            entity (str): The SAP entity to fetch data for.
            api_client (APIClient): The API client to use for fetching data.
        """
        cached_data = self.sap_cache.get(f'{entity}_df')
        if cached_data is not None:
            Logger.info(f"Retrieved {entity} data from cache.")
            return cached_data
        params = uris_params[entity]
        params_dict = extract_sap_params_safe(params)
        entity_data = api_client.fetch_all(get_apis[entity], params=params_dict)
        entity_df = pd.DataFrame(entity_data)
        entity_df.columns = [col.lower() for col in entity_df.columns]

        self.sap_cache.set(f'{entity.lower()}_df', entity_df)
        Logger.info(f"Fetched and cached {len(entity_df)} records for {entity}.")


    def extract_and_cache_sap_data(self,position_flag: bool = False,empjob_flag: bool = False):
        """
        Extracts SAP data using the API client and caches it.
        """
        # Initialize Auth API URL
        auth_url_ = f"{self.base_url}{self.auth_url}"
        # Initialize SAP data cache and API client
        auth_api = AuthAPI(
            auth_url=auth_url_,
            client_id=self.client_id,
            client_secret=self.client_secret,
            grant_type=self.grant_type,
            company_id=self.company_id,
            max_retries=self.max_retries
        )
        # Obtain token and create API client
        token = auth_api.get_token()
        api_client = APIClient(base_url=self.base_url, token={'access_token': token},max_retries=self.max_retries)
        # Test data extraction and caching for positions
        if position_flag:
            self._fetch_cache_sap_data('positions', api_client)
        elif empjob_flag:
            self._fetch_cache_sap_data('employees', api_client)
        else:
            for entity in self.entities:
                self._fetch_cache_sap_data(entity, api_client)
