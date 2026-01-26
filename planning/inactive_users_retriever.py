from db.oracle_connection import OracleDatabaseConnection
from cache.employees_cache import EmployeesDataCache
from utils.logger import get_logger

import pandas as pd



logger = get_logger('inactive_users_retriever')

class InactiveUsersRetriever:
    """
    Class to retrieve inactive users IDs.
    """

    def __init__(
            self, 
            inactive_users: pd.DataFrame,
            oracle_dsn: dict,
            extract_pdm_inactive_records_query: str, 
            cache_key: str='inactive_users', 
            inactive_users_flag_nb: int =100):
        self.inactive_users = inactive_users
        self.oracle_connection = OracleDatabaseConnection(oracle_dsn)
        self.inactive_users_flag_nb = inactive_users_flag_nb
        self.extract_pdm_inactive_records_query = extract_pdm_inactive_records_query
        self.employees_cache = EmployeesDataCache() 
        self.cache_key = cache_key
    def _retrieve_inactive_users_ids(self) -> set:
        """
        Retrieves a set of inactive user IDs.

        Returns:
            set: A set containing inactive user IDs.
        """
        try:
            inactive_user_ids = set(
                self.inactive_users['userid'].astype(str).str.lower().unique()
            )
            logger.info(f"Retrieved {len(inactive_user_ids)} inactive user IDs.")
            return inactive_user_ids
        except Exception as e:
            logger.error(f"Error retrieving inactive user IDs: {e}")
            raise e
    
    def _fetch_inactive_user_details(self) -> pd.DataFrame:
        """
        Fetches details of inactive users from the PDM database.
        Handles Oracle's 1000 item IN clause limit by batching queries.

        Returns:
            pd.DataFrame: DataFrame containing details of inactive users.
        """
        connection = None
        cursor = None
        try:
            inactive_user_ids = list(self._retrieve_inactive_users_ids())

            if len(inactive_user_ids) == 0:
                logger.warning("No inactive user IDs to fetch details for.")
                return pd.DataFrame()
            
            # If inactive_users_flag_nb is set (int), treat as a hard upper limit; if None, do not enforce
            if self.inactive_users_flag_nb is not None and len(inactive_user_ids) > self.inactive_users_flag_nb:
                logger.error("Number of inactive users exceeds the expected threshold.")
                raise ValueError("Exceeded inactive users threshold.")
            
            # Oracle IN clause limit is 1000, so batch the queries
            batch_size = 1000
            all_results = []
            columns = None
            
            connection = self.oracle_connection.get_oracle_db_connection()
            cursor = connection.cursor()
            
            # Process in batches
            for i in range(0, len(inactive_user_ids), batch_size):
                batch_ids = inactive_user_ids[i:i + batch_size]
                user_ids_str = ','.join(f"'{uid}'" for uid in batch_ids)
                query = self.extract_pdm_inactive_records_query.format(user_ids=user_ids_str)
                
                cursor.execute(query)
                
                if columns is None:
                    columns = [desc[0].lower() for desc in cursor.description]
                
                batch_results = cursor.fetchall()
                all_results.extend(batch_results)
                
                logger.info(f"Fetched batch {i//batch_size + 1}: {len(batch_results)} inactive users from PDM")
            
            inactive_user_details_df = pd.DataFrame(all_results, columns=columns)

            logger.info(f"Total fetched details for {len(inactive_user_details_df)} inactive users from PDM.")
            if inactive_user_details_df.empty:
                logger.warning("No inactive user details found in PDM for the given user IDs.")
            if len(inactive_user_details_df) < len(self.inactive_users):
                logger.warning("Some inactive users from SAP EC were not found in PDM.")
            if self.inactive_users_flag_nb is not None and len(inactive_user_details_df) > self.inactive_users_flag_nb:
                logger.error("Number of inactive users fetched from PDM exceeds the expected threshold.")
                raise ValueError("Exceeded inactive users threshold.")
            return inactive_user_details_df
        except Exception as e:
            logger.error(f"Error fetching inactive user details: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()
            else:
                logger.warning("Cursor was not initialized before closing.")
            if connection:
                connection.close()
            else:
                logger.warning("Connection was not initialized before closing.")

    def _store_inactive_users_details(self, final_inactive_users_df: pd.DataFrame, cache_key: str) -> None:
        """
        Stores inactive users details in the employees cache.

        Args:
            final_inactive_users_df (pd.DataFrame): DataFrame containing final inactive users details.
            cache_key (str): Key to store/retrieve changes in/from EmployeesDataCache.
        """
        try:
            self.employees_cache.set(f"{cache_key}_df", final_inactive_users_df)
            logger.info(f"Stored {len(final_inactive_users_df)} inactive users details in cache with key '{cache_key}_df'.")
        except Exception as e:
            logger.error(f"Error storing inactive users details in cache: {e}")
            raise e
        

    def store_inactive_users_details(self) -> bool:
        """
        Public method to get inactive users details.
        Step:
            1- Before returning, it checks some criterias:
                - if the user has the necessary fields
                - convert pdm_uid to string 
                - convert date of leave to datetime
                - convert exit reason id to integer if needed
            2- Join with inactive users to have all details in one dataframe based on userid (which is pdm_uid here)
        Returns:
            bool: True if successful, False otherwise.
        """
        no_nullable_fields = ['pdm_uid', 'exit_reason_id']

        inactive_user_details_df = self._fetch_inactive_user_details()
        # Check if inactive_user_details_df is not empty
        if inactive_user_details_df.empty:
            logger.warning("No inactive user details to process.")
            return pd.DataFrame()  # Return empty DataFrame if no details found
        for field in no_nullable_fields:
            if field not in inactive_user_details_df.columns:
                logger.error(f"Missing required field '{field}' in inactive user details.")
                raise ValueError(f"Missing required field '{field}'.")
        inactive_user_details_df['pdm_uid'] = inactive_user_details_df['pdm_uid'].astype(str).str.lower()
        inactive_user_details_df['date_of_leave'] = pd.to_datetime(inactive_user_details_df['date_of_leave'], errors='coerce')
        inactive_user_details_df['exit_reason_id'] = pd.to_numeric(inactive_user_details_df['exit_reason_id'], errors='coerce').astype('Int64')

        # Count how many inactive users have nulls in non-nullable fields and log warn messages if any
        for field in no_nullable_fields:
            if inactive_user_details_df[field].isnull().any():
                null_count = inactive_user_details_df[field].isnull().sum()
                logger.warning(f"{null_count} inactive users have null values in non-nullable field '{field}'.")
        
        # Remove inactive users with nulls in non-nullable fields
        inactive_user_details_df = inactive_user_details_df.dropna(subset=no_nullable_fields)
        # Merge inactive users with their details, remove pdm_uid column keep only userid
        merged_inactive_users_df = pd.merge(
            self.inactive_users,
            inactive_user_details_df,
            left_on='userid',
            right_on='pdm_uid',
            how='inner'
        )
        merged_inactive_users_df = merged_inactive_users_df.drop(columns=['pdm_uid'])
        self._store_inactive_users_details(merged_inactive_users_df, self.cache_key)
        return True