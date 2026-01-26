from utils.logger import get_logger
from extractor.postgres_extractor import PostgresDBExtractor
from extractor.oracle_extractor import OracleDBExtractor
from cache.postgres_cache import PostgresDataCache
from cache.oracle_cache import OracleDataCache
import pandas as pd

logger = get_logger('data_extraction')


class CacheDataExtractor:
    """
    Class to extract data from PostgreSQL and Oracle databases and cache them.
    """

    def __init__(
            self,
            postgres_extractor: PostgresDBExtractor,
            oracle_extractor: OracleDBExtractor,
            extract_pdm_records_query: str,
            extract_ec_records_query: str,
            extract_jobs_titles_records_query: str,
            extract_different_userid_personid_query: str,
            ):
        self.postgres_extractor = postgres_extractor
        self.oracle_extractor = oracle_extractor
        self.extract_ec_records_query = extract_ec_records_query
        self.extract_jobs_titles_records_query = extract_jobs_titles_records_query
        self.extract_different_userid_personid_query = extract_different_userid_personid_query
        self.extract_pdm_records_query = extract_pdm_records_query
        

    def extract_and_cache_data(self):
        """
        Extracts data from PostgreSQL and Oracle databases and caches them.
        """
        
        postgres_extractor = self.postgres_extractor
        oracle_extractor = self.oracle_extractor
        logger.info("Starting data extraction from PostgreSQL Database.")
        # Extact EC columns and data
        ec_data, ec_columns = postgres_extractor.extract_data(self.extract_ec_records_query)
        logger.info(f"Extracted {len(ec_data)} records from PostgreSQL Database.")
        # Extact Job Titles columns and data
        jobs_titles_data, jobs_titles_columns = postgres_extractor.extract_data(self.extract_jobs_titles_records_query)
        logger.info(f"Extracted {len(jobs_titles_data)} records from PostgreSQL Database (Job Titles).")
        # Extract employee having different USERID and PERSON_ID_EXTERNAL
        different_userid_personid_data, different_userid_personid_columns = postgres_extractor.extract_data(self.extract_different_userid_personid_query)
        logger.info(f"Extracted {len(different_userid_personid_data)} records with different USERID and PERSON_ID_EXTERNAL from PostgreSQL Database.")
        # Extract PDM columns and data
        pdm_data, pdm_columns = oracle_extractor.extract_data(self.extract_pdm_records_query)
        logger.info(f"Extracted {len(pdm_data)} records from Oracle Database.")

        # Create DataFrames with column names and normalize to lowercase
        pd_ec_data = pd.DataFrame(ec_data, columns=[col.lower() for col in ec_columns])
        pd_pdm_data = pd.DataFrame(pdm_data, columns=[col.lower() for col in pdm_columns])
        
        dates_columns = ['date_of_birth', 'date_of_position', 'hiredate']
        for date_col in dates_columns:
            if date_col in pd_ec_data.columns:
                # Replace dashes with slashes only for non-null values (preserve NULL as None/NaN)
                pd_ec_data[date_col] = pd_ec_data[date_col].apply(lambda x: x.replace('-', '/') if pd.notna(x) and x is not None else x)
        
        pd_jobs_titles_data = pd.DataFrame(jobs_titles_data, columns=[col.lower() for col in jobs_titles_columns])
        pd_different_userid_personid_data = pd.DataFrame(different_userid_personid_data, columns=[col.lower() for col in different_userid_personid_columns])
        postgres_cache = PostgresDataCache()
        oracle_cache = OracleDataCache()


        postgres_cache.set('ec_data_df', pd_ec_data)
        postgres_cache.set('jobs_titles_data_df', pd_jobs_titles_data)
        postgres_cache.set('different_userid_personid_data_df', pd_different_userid_personid_data)
        oracle_cache.set('pdm_data_df', pd_pdm_data)


        logger.info("Data extraction completed successfully.")

        #Sample to show data
        logger.info("PostgreSQL Data Sample:")
        logger.info(pd_ec_data.head())
        logger.info("Oracle Data Sample:")
        logger.info(pd_pdm_data.head(15))