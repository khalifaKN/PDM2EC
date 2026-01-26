
from extractor.postgres_extractor import PostgresDBExtractor
from extractor.oracle_extractor import OracleDBExtractor
from config.db import postgres_url, oracle_dsn
from config.api_credentials import auth_credentials
from config.sf_apis import base_url,auth_endpoint
from extractor.cache_data_extractor import CacheDataExtractor
from extractor.extract_exist_employees import ExistEmployeesExtractor
from extractor. import NewEmployeesExtractor 
from planning.employee_creation_order_resolver import EmployeeCreationOrderResolver
from utils.logger import get_logger
from extractor.sap_info_cache_handler import SAPInfoCacheHandler
from cache.postgres_cache import PostgresDataCache
from cache.oracle_cache import OracleDataCache
from cache.sap_cache import SAPDataCache
from db.psycopg2_connection import Psycopg2DatabaseConnection
import os
from orchestrator.core_processing import CoreProcessor
logger = get_logger('test_pipeline')
entities = ['positions', 'employees', 'perPerson', 'perPersonal']

if __name__ == "__main__":

    #Step 1: DB Connections and Data Extraction
        # Create extractors
    postgres_extractor = PostgresDBExtractor(postgres_url)
    oracle_extractor = OracleDBExtractor(oracle_dsn)
        # Create CacheDataExtractor and extract & cache data
    cache_data_extractor = CacheDataExtractor(postgres_extractor, oracle_extractor)
    cache_data_extractor.extract_and_cache_data()
    #Step 2: SAP Data Extraction and Caching
    sap_info_cache_handler = SAPInfoCacheHandler(base_url=base_url,auth_url=auth_endpoint,auth_credentials=auth_credentials,entites=entities)
    sap_info_cache_handler.extract_and_cache_sap_data()
    #Step 3: Retrieve Cached Data
    postgres_cache = PostgresDataCache()
    oracle_cache = OracleDataCache()
    cached_ec_data = postgres_cache.get('ec_data_df')
    cached_pdm_data = oracle_cache.get('pdm_data_df')
    # sap_cache = SAPDataCache()
    # cached_positions_data = sap_cache.get('positions_df')
    # cached_employees_data = sap_cache.get('employees_df')
    # cached_perperson_data = sap_cache.get('perperson_df')
    # cached_perpersonal_data = sap_cache.get('perpersonal_df')

    # to_csv_dir = './test/test_outputs/sap_cached_data'
    # os.makedirs(to_csv_dir, exist_ok=True)
    # cached_positions_data.to_csv(os.path.join(to_csv_dir, 'cached_positions_data.csv'), index=False)
    # cached_employees_data.to_csv(os.path.join(to_csv_dir, 'cached_employees_data.csv'), index=False)
    # cached_perperson_data.to_csv(os.path.join(to_csv_dir, 'cached_perperson_data.csv'), index=False)
    # cached_perpersonal_data.to_csv(os.path.join(to_csv_dir, 'cached_perpersonal_data.csv'), index=False)
    # logger.info(f"Cached SAP data saved to {to_csv_dir}/")
    #Step 4: Extract Existing Employees
        # Retrieve cached data
    exist_employees_extractor = ExistEmployeesExtractor(pdm_users=cached_pdm_data, sap_users=cached_ec_data)
    existing_employees_df = exist_employees_extractor.extract_existing_employees()
    #Step 5: Extract New Employees
    new_employees_extractor = NewEmployeesExtractor(pdm_users=cached_pdm_data, sap_users=cached_ec_data)
    new_employees_df = new_employees_extractor.()
    
    # # Debug: Check if any "new" employees actually exist in live SAP cache
    # if len(new_employees_df) > 0:
    #     new_userids = new_employees_df['userid'].astype(str).str.lower().tolist()
    #     existing_in_sap = cached_employees_data[
    #         cached_employees_data['userid'].astype(str).str.lower().isin(new_userids)
    #     ]
    #     if len(existing_in_sap) > 0:
    #         logger.warning(f"⚠️  WARNING: {len(existing_in_sap)} users marked as 'new' already exist in live SAP:")
    #         for uid in existing_in_sap['userid'].tolist():
    #             logger.warning(f"   - User {uid} is in SAP but NOT in PostgreSQL staging (ec_data)")
    #         logger.warning("   This may cause 'hire record already exists' errors!")

    
    # # Step 5.5: Convert dates and add country_iso3 field
    # new_employees_df = convert_pdm_dates(new_employees_df)
    # logger.info("Converted dates and added country_iso3 for new employees")
    
    # Step 5.5: Resolve creation order for new employees
    creation_order_resolver = EmployeeCreationOrderResolver(
        new_employees=new_employees_df,
        existing_employees=existing_employees_df,
    )
    
    batches = creation_order_resolver.get_ordered_batches()
    summary = creation_order_resolver.get_dependency_summary()
    logger.info("Employee Creation Order Summary:")
    for key, value in summary.items():
        logger.info(f"  {key}: {value}")
    # Optional: Save batches to CSV for verification
    output_dir = './test/test_outputs'
    batches_dir = os.path.join(output_dir, "creation_batches")
    os.makedirs(batches_dir, exist_ok=True)

    for i, batch_df in enumerate(batches, start=1):
        batch_file = os.path.join(batches_dir, f"batch_{i}.csv")
        try:
            batch_df.to_csv(batch_file, index=False)
            logger.info(f"Saved batch {i} to {batch_file}")
        except PermissionError:
            logger.warning(f"Could not save batch {i} - file is open in another program. Skipping...")
            

    # Step 6: Persist New Employees to SAP EC
    # core_processor_params = {
    #     'new_employees_df': new_employees_df,
    #     'existing_employees_df': existing_employees_df,
    #     'auth_credentials': auth_credentials,
    #     'base_url': base_url,
    #     'auth_url': f"{base_url} {auth_endpoint}",
    #     'ordered_batches': batches,
    #     'batches_summary': summary
    # }

    # core_processor = CoreProcessor(**core_processor_params)
    # core_processor.process_batches_new_employees()

    # #Step 6: Retrieve PDM Updates for Existing Employees
    # postgres_db_connector = Psycopg2DatabaseConnection(postgres_url)
    # pdm_updates_retriver = PDMUpdatesRetriever(pdm_data=cached_pdm_data, ec_data=cached_ec_data, chunk_size=10000, postgres_connector=postgres_db_connector)
    # pdm_updates_retriver.persist_changes_chunked()
    # Optional: Save outputs to CSV for verification


    # output_dir = './test/test_outputs'
    # os.makedirs(output_dir, exist_ok=True)
    # existing_employees_df.to_csv(os.path.join(output_dir, 'existing_employees.csv'), index=False)
    # new_employees_df.to_csv(os.path.join(output_dir, 'new_employees.csv'), index=False)
    

    
    # logger.info(f"Pipeline completed. Outputs saved to {output_dir}/")
    # logger.info(f"- Existing employees: {len(existing_employees_df)}")
    # logger.info(f"- New employees: {len(new_employees_df)}")
