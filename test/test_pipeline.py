"""
Test Pipeline for PDM to EC Data Synchronization

This pipeline demonstrates the complete flow:
1. Extract data from PostgreSQL and Oracle databases
2. Cache SAP SuccessFactors data
3. Identify new and existing employees
4. Process new employee creation with dependency resolution
5. Detect field changes for existing employees
6. Process field updates for existing employees

Usage:
    python -m test.test_pipeline

Troubleshooting:
    If the pipeline hangs during database extraction:
    1. Kill process: taskkill /F /IM python.exe
    2. Set EXTRACT_DATABASE_DATA = False to use cached data
    3. Set EXTRACT_SAP_DATA = False to use cached SAP data
    4. Run specific steps by enabling/disabling flags below
    
    For first-time setup:
    - Enable EXTRACT_DATABASE_DATA and EXTRACT_SAP_DATA
    - Disable PROCESS_NEW_EMPLOYEES and PROCESS_FIELD_UPDATES
    - Let data extraction complete first
    
    For testing with existing cache:
    - Disable EXTRACT_DATABASE_DATA and EXTRACT_SAP_DATA
    - Enable PROCESS_NEW_EMPLOYEES and/or PROCESS_FIELD_UPDATES
"""

from extractor.extract_inactive_employees import InactiveEmployeesExtractor
from extractor.postgres_extractor import PostgresDBExtractor
from extractor.oracle_extractor import OracleDBExtractor
from extractor.cache_data_extractor import CacheDataExtractor
from extractor.extract_exist_employees import ExistEmployeesExtractor
from extractor.extract_new_employees import NewEmployeesExtractor 
from extractor.sap_info_cache_handler import SAPInfoCacheHandler
from planning.employee_creation_order_resolver import EmployeeCreationOrderResolver
from planning.scm_im_updates_retriver import SCM_IM_UpdatesRetriever
from planning.retrieve_standard_users_changes import StandardUsersUpdatesRetriever
from planning.inactive_users_retriever import InactiveUsersRetriever
from planning.convert_pdm_data import convert_pdm_data
from planning.excluded_users_retriever import ExcludedUsersRetriever
from config.db import postgres_url, oracle_dsn
from config.api_credentials import auth_credentials
from config.sf_apis import base_url, auth_endpoint
from config.exclusion_standards import EXLUSION_STANDARDS
from config.tables_names import (
    regular_pipeline_summary_tables,
    regular_field_changes_tables
)
from queries.postgres_queries import (
    extract_ec_records_query, 
    extract_jobs_titles_records_query, 
    extract_different_userid_personid_query
    )
from queries.oracle_queries import extract_pdm_records_query,extract_pdm_inactive_records_query
from orchestrator.core_processing import CoreProcessor
from orchestrator.disable_ec_users_processing import DisableUsersProcessor
from orchestrator.notification_processing import NotificationHandler
from cache.postgres_cache import PostgresDataCache
from cache.oracle_cache import OracleDataCache
from cache.sap_cache import SAPDataCache
from cache.employees_cache import EmployeesDataCache
from utils.logger import get_logger
from db.psycopg2_connection import Psycopg2DatabaseConnection
from loader.pipeline_history_loader import PipelineHistoryLoader


import os
import pandas as pd
from datetime import datetime


logger = get_logger('test_pipeline')

# SAP entities to cache
SAP_ENTITIES = ['positions', 'employees', 'perPerson', 'perPersonal','perEmail']

# Test configuration flags
EXTRACT_DATABASE_DATA = True     # Step 1: Extract from PostgreSQL/Oracle
EXTRACT_SAP_DATA = True          # Step 2: Extract from SAP
PROCESS_NEW_EMPLOYEES = True     # Step 7: Process new employee creation
PROCESS_FIELD_UPDATES = True     # Step 8-9: Detect and process field updates
PROCESS_INACTIVE_USERS = True    # Step 10: Process inactive users (terminate & disable)
SAVE_DEBUG_OUTPUTS = True        # Save CSV files for debugging
PROCESS_NOTIFICATIONS = True     # Step 11: Send notification email

# Database timeout settings (in seconds)
DB_QUERY_TIMEOUT = 300  
DB_CONNECTION_TIMEOUT = 30 

def extract_and_cache_database_data():
    """
    Step 1: Extract and cache data from PostgreSQL and Oracle databases.
    """
    if not EXTRACT_DATABASE_DATA:
        logger.info("Skipping database extraction (disabled by config)")
        return
    
    logger.info("=" * 80)
    logger.info("STEP 1: Extracting and caching database data")
    logger.info("=" * 80)
    
    try:
        postgres_extractor = PostgresDBExtractor(postgres_url)
        oracle_extractor = OracleDBExtractor(oracle_dsn)
        
        cache_data_extractor = CacheDataExtractor(
            postgres_extractor, 
            oracle_extractor,
            extract_pdm_records_query,
            extract_ec_records_query,
            extract_jobs_titles_records_query,
            extract_different_userid_personid_query
        )
        
        logger.info("Starting data extraction (this may take several minutes)...")
        logger.info(f"Database query timeout: {DB_QUERY_TIMEOUT}s")
        
        cache_data_extractor.extract_and_cache_data()
        
        logger.info("✓ Database data extraction and caching completed\n")
    except Exception as e:
        logger.error(f"Database extraction failed: {e}", exc_info=True)
        logger.error("Try reducing data volume or increasing timeout")
        raise


def extract_and_cache_sap_data():
    """
    Step 2: Extract and cache SAP SuccessFactors data.
    """
    if not EXTRACT_SAP_DATA:
        logger.info("Skipping SAP extraction (disabled by config)")
        return
    
    logger.info("=" * 80)
    logger.info("STEP 2: Extracting and caching SAP SuccessFactors data")
    logger.info("=" * 80)
    
    try:
        sap_info_cache_handler = SAPInfoCacheHandler(
            base_url=base_url,
            auth_url=auth_endpoint,
            auth_credentials=auth_credentials,
            entites=SAP_ENTITIES
        )
        
        logger.info("Starting SAP data extraction (this may take several minutes)...")
        sap_info_cache_handler.extract_and_cache_sap_data()
        
        logger.info("✓ SAP data extraction and caching completed\n")
    except Exception as e:
        logger.error(f"SAP extraction failed: {e}", exc_info=True)
        raise


def load_cached_data():
    """
    Step 3: Load cached data from PostgreSQL, Oracle, and SAP caches.
    Returns:
        tuple: (postgres_cache, oracle_cache, sap_cache, cached_ec_data, cached_pdm_data)
    """
    logger.info("=" * 80)
    logger.info("STEP 3: Loading cached data")
    logger.info("=" * 80)
    
    postgres_cache = PostgresDataCache()
    oracle_cache = OracleDataCache()
    sap_cache = SAPDataCache()
    
    cached_ec_data = postgres_cache.get('ec_data_df')
    cached_pdm_data = oracle_cache.get('pdm_data_df')
    
    logger.info(f"Loaded EC data: {len(cached_ec_data)} records")
    logger.info(f"Loaded PDM data: {len(cached_pdm_data)} records")
    logger.info("✓ Cached data loaded successfully\n")
    
    return postgres_cache, oracle_cache, sap_cache, cached_ec_data, cached_pdm_data


def save_sap_cache_debug_files(sap_cache):
    """
    Save SAP cached data to CSV files for debugging purposes.
    """
    if not SAVE_DEBUG_OUTPUTS:
        return
    
    logger.info("Saving SAP cached data to CSV files for debugging...")
    
    to_csv_dir = './test/test_outputs_v1.0/sap_cached_data'
    os.makedirs(to_csv_dir, exist_ok=True)
    
    sap_cache.get('positions_df').to_csv(os.path.join(to_csv_dir, 'cached_positions_data.csv'), index=False)
    sap_cache.get('employees_df').to_csv(os.path.join(to_csv_dir, 'cached_employees_data.csv'), index=False)
    sap_cache.get('perperson_df').to_csv(os.path.join(to_csv_dir, 'cached_perperson_data.csv'), index=False)
    sap_cache.get('perpersonal_df').to_csv(os.path.join(to_csv_dir, 'cached_perpersonal_data.csv'), index=False)
    sap_cache.get('peremail_df').to_csv(os.path.join(to_csv_dir, 'cached_peremail_data.csv'), index=False)
    
    logger.info(f"✓ SAP cached data saved to {to_csv_dir}/\n")


def extract_employee_classifications(cached_pdm_data, cached_ec_data):
    """
    Step 4: Extract and classify employees as existing , new or inactive.
    Returns:
        tuple: (existing_employees_df, new_employees_df, inactive_employees_df)
    """
    logger.info("=" * 80)
    logger.info("STEP 4: Classifying employees (existing vs new vs inactive)")
    logger.info("=" * 80)
    
    # Extract existing employees
    exist_employees_extractor = ExistEmployeesExtractor(
        pdm_users=cached_pdm_data,
        sap_users=cached_ec_data
    )
    existing_employees_df = exist_employees_extractor.extract_existing_employees()
    
    # Extract new employees
    new_employees_extractor = NewEmployeesExtractor(
        pdm_users=cached_pdm_data,
        sap_users=cached_ec_data
    )
    new_employees_df, new_employees_excluded_count = new_employees_extractor.extract_new_employees()
    
    # Extract inactive employees
    inactive_employees_extractor = InactiveEmployeesExtractor(
        pdm_users=cached_pdm_data,
        sap_users=cached_ec_data
    )
    inactive_employees_df = inactive_employees_extractor.extract_inactive_employees()
    
    logger.info(f"Found {len(existing_employees_df)} existing employees")
    logger.info(f"Found {len(new_employees_df)} new employees")
    logger.info(f"Excluded {new_employees_excluded_count} new employees not flagged as IM/SCM")
    logger.info(f"Found {len(inactive_employees_df)} inactive employees")
    logger.info("✓ Employee classification completed\n")
    
    return existing_employees_df, new_employees_df, inactive_employees_df


def validate_new_employees(new_employees_df, sap_cache):
    """
    Validate that new employees don't already exist in SAP.
    """
    if len(new_employees_df) == 0:
        logger.info("No new employees to validate")
        return
    
    logger.info("Validating new employees against live SAP cache...")
    
    cached_employees_data = sap_cache.get('employees_df')
    new_userids = new_employees_df['userid'].astype(str).str.lower().tolist()
    existing_in_sap = cached_employees_data[
        cached_employees_data['userid'].astype(str).str.lower().isin(new_userids)
    ]
    
    if len(existing_in_sap) > 0:
        logger.warning(f"⚠️  WARNING: {len(existing_in_sap)} users marked as 'new' already exist in live SAP:")
        for uid in existing_in_sap['userid'].tolist():
            logger.warning(f"   - User {uid} is in SAP but NOT in PostgreSQL staging (ec_data)")
        logger.warning("   This may cause 'hire record already exists' errors!")
    else:
        logger.info("✓ All new employees validated - no conflicts with live SAP data\n")


def prepare_new_employees_data(new_employees_df):
    """
    Step 5: Prepare new employees data (convert dates, add country fields).
    """
    if len(new_employees_df) == 0:
        logger.info("No new employees to prepare")
        return new_employees_df
    
    logger.info("=" * 80)
    logger.info("STEP 5: Preparing new employees data")
    logger.info("=" * 80)
    
    new_employees_df = convert_pdm_data(new_employees_df)
    logger.info("✓ Converted dates and added country_iso3 for new employees\n")
    
    return new_employees_df


def resolve_creation_order(new_employees_df, existing_employees_df):
    """
    Step 6: Resolve creation order for new employees based on dependencies.
    Returns:
        tuple: (batches, summary)
    """
    if len(new_employees_df) == 0:
        logger.info("No new employees - skipping creation order resolution")
        return [], {}
    
    logger.info("=" * 80)
    logger.info("STEP 6: Resolving employee creation order")
    logger.info("=" * 80)
    
    creation_order_resolver = EmployeeCreationOrderResolver(
        new_employees=new_employees_df,
        existing_employees=existing_employees_df,
    )
    
    batches = creation_order_resolver.get_ordered_batches()
    summary = creation_order_resolver.get_dependency_summary()
    
    logger.info("Employee Creation Order Summary:")
    for key, value in summary.items():
        logger.info(f"  {key}: {value}")
    logger.info("✓ Creation order resolved\n")
    
    return batches, summary


def save_creation_batches(batches):
    """
    Save creation batches to CSV files for verification.
    """
    if not SAVE_DEBUG_OUTPUTS or len(batches) == 0:
        return
    
    logger.info("Saving creation batches to CSV files...")
    
    batches_dir = './test/test_outputs_v1.0/creation_batches'
    os.makedirs(batches_dir, exist_ok=True)
    
    for i, batch_df in enumerate(batches, start=1):
        batch_file = os.path.join(batches_dir, f"batch_{i}.csv")
        try:
            batch_df.to_csv(batch_file, index=False)
            logger.info(f"✓ Saved batch {i} ({len(batch_df)} employees) to {batch_file}")
        except PermissionError:
            logger.warning(f"Could not save batch {i} - file is open in another program")


def process_new_employees(new_employees_df, batches, summary):
    """
    Step 7: Process new employee creation through CoreProcessor.
    """
    if not PROCESS_NEW_EMPLOYEES or len(new_employees_df) == 0:
        logger.info("Skipping new employee processing (disabled or no new employees)")
        return None
    
    logger.info("=" * 80)
    logger.info("STEP 7: Processing new employees creation")
    logger.info("=" * 80)
    
    core_processor = CoreProcessor(
        auth_url=auth_endpoint,
        base_url=base_url,
        auth_credentials=auth_credentials,
        ordered_batches=batches,
        batches_summary=summary
    )
    
    results = core_processor.process_batches_new_employees()
    
    logger.info(f"✓ Processed {len(results)} new employee records")
    logger.info(f"   - Successful: {sum(1 for ctx in results.values() if not ctx.has_errors)}")
    logger.info(f"   - Failed: {sum(1 for ctx in results.values() if ctx.has_errors)}\n")
    
    return results


def detect_field_changes(cached_pdm_data, cached_ec_data, existing_employees_df, run_id=None):
    """
    Step 8: Detect field changes for existing employees.
    Processes both SCM/IM users and standard users separately.
    
    Args:
        cached_pdm_data: PDM source data
        cached_ec_data: EC/SAP current data
        existing_employees_df: DataFrame of existing employees
        run_id: Optional pipeline run_id to link field change batches
        
    Returns:
        pd.DataFrame: Combined field changes dataframe
    """
    if not PROCESS_FIELD_UPDATES or len(existing_employees_df) == 0:
        logger.info("Skipping field change detection (disabled or no existing employees)")
        return None
    
    logger.info("=" * 80)
    logger.info("STEP 8: Detecting field changes for existing employees")
    logger.info("=" * 80)
    
    # Initialize database connector
    postgres_connector = Psycopg2DatabaseConnection(postgres_url)
    
    # Classify users into SCM, IM, and Standard categories
    # For now, using simple classification - you can adjust based on your logic
    existing_userids = set(existing_employees_df['userid'].astype(str).str.lower())
    
    # TODO: Implement proper logic to identify SCM and IM users
    # For now, assuming all existing users are standard users
    # You might have flags in your data like 'is_scm_user' or 'is_im_user'
    scm_users_ids = set()
    im_users_ids = set()
    
    # Check if classification columns exist
    if 'is_peoplehub_scm_manually_included' in existing_employees_df.columns:
        scm_mask = (existing_employees_df['is_peoplehub_scm_manually_included'].astype(str).str.lower()=='y') | (existing_employees_df['is_peoplehub_scm_manually_included'].astype(str).str.lower() == 'true')
        scm_users_ids = set(
            existing_employees_df[scm_mask]['userid']
            .astype(str).str.lower()
        )
        scm_df = existing_employees_df[existing_employees_df['userid'].astype(str).str.lower().isin(scm_users_ids)]
    if 'is_peoplehub_im_manually_included' in existing_employees_df.columns:
        im_mask = (existing_employees_df['is_peoplehub_im_manually_included'].astype(str).str.lower()=='y') | (existing_employees_df['is_peoplehub_im_manually_included'].astype(str).str.lower() == 'true')
        im_users_ids = set(
            existing_employees_df[im_mask]['userid']
            .astype(str).str.lower()
        )
        im_df = existing_employees_df[existing_employees_df['userid'].astype(str).str.lower().isin(im_users_ids)]
    
    # Standard users are those who are neither SCM nor IM
    standard_users_ids = existing_userids - scm_users_ids - im_users_ids
    
    
    
    all_field_changes = []
    
    # Exclude some IM/SCM users from standard processing if they match some criteria
    # 1- Exclude users based on country, company, or combined criteria
    excluded_scm_users_retriever = ExcludedUsersRetriever(
        excluded_criteria=EXLUSION_STANDARDS,
        users_df=scm_df
    )
    excluded_im_users_retriever = ExcludedUsersRetriever(
        excluded_criteria=EXLUSION_STANDARDS,
        users_df=im_df
    )
    # 2- Get cleaned SCM/IM users dataframes
    scm_users_cleaned = excluded_scm_users_retriever.get_cleaned_users_df()
    im_users_cleaned = excluded_im_users_retriever.get_cleaned_users_df()
    
    # 3- Update SCM/IM user ids after exclusion
    if scm_users_cleaned.empty:
        scm_users_ids = set()
    else:
        scm_users_ids = set(scm_users_cleaned['userid'].astype(str).str.lower())
    
    if im_users_cleaned.empty:
        im_users_ids = set()
    else:
        im_users_ids = set(im_users_cleaned['userid'].astype(str).str.lower())
    
    
    standard_users_ids = existing_userids - scm_users_ids - im_users_ids

    logger.info("User classification (after exclusions):")
    logger.info(f"  - SCM users: {len(scm_users_ids)}")
    logger.info(f"  - IM users: {len(im_users_ids)}")
    logger.info(f"  - Standard users: {len(standard_users_ids)}")
    
    all_field_changes = []
    if scm_users_ids or im_users_ids:
        logger.info("\nProcessing SCM/IM users...")
        scm_im_retriever = SCM_IM_UpdatesRetriever(
            pdm_data=cached_pdm_data,
            ec_data=cached_ec_data,
            scm_users_ids=scm_users_ids,
            im_users_ids=im_users_ids,
            table_names=regular_field_changes_tables,
            chunk_size=10000,
            postgres_connector=postgres_connector,
            run_id=run_id,
            batch_context="SCM/IM Users"
        )
        
        scm_im_retriever.persist_changes_chunked(
            change_generator=scm_im_retriever.generate_changes,
            cache_key="scm_im_field_changes_df"
        )
        
        # Retrieve SCM/IM changes
        employees_cache = EmployeesDataCache()
        scm_im_changes = employees_cache.get("scm_im_field_changes_df")
        if scm_im_changes is not None and len(scm_im_changes) > 0:
            all_field_changes.append(scm_im_changes)
            logger.info(f"✓ Detected {len(scm_im_changes)} field changes for {scm_im_changes['userid'].nunique()} SCM/IM users")
    
    # Process standard users if any
    if standard_users_ids:
        logger.info("\nProcessing standard users...")
        # Load SAP email data from cache
        sap_cache = SAPDataCache()
        sap_email_data = sap_cache.get('peremail_df')
        if sap_email_data is None:
            logger.warning("SAP email data not found in cache, email validation may fail")
            sap_email_data = pd.DataFrame()
        
        standard_retriever = StandardUsersUpdatesRetriever(
            pdm_data=cached_pdm_data,
            ec_data=cached_ec_data,
            standard_users_ids=standard_users_ids,
            postgres_connector=postgres_connector,
            table_names=regular_field_changes_tables,
            chunk_size=10000,
            sap_email_data=sap_email_data,
            run_id=run_id,
            batch_context="Standard Users"
        )
        
        standard_retriever.persist_changes_chunked(
            change_generator=standard_retriever.generate_changes,
            cache_key="standard_field_changes_df"
        )
        
        # Retrieve standard changes
        employees_cache = EmployeesDataCache()
        standard_changes = employees_cache.get("standard_field_changes_df")
        if standard_changes is not None and len(standard_changes) > 0:
            all_field_changes.append(standard_changes)
            logger.info(f"✓ Detected {len(standard_changes)} field changes for {standard_changes['userid'].nunique()} standard users")
    
    # Combine all changes
    if all_field_changes:
        field_changes_df = pd.concat(all_field_changes, ignore_index=True)
        # Store combined changes in cache
        employees_cache = EmployeesDataCache()
        employees_cache.set("field_changes_df", field_changes_df)
        
        logger.info(f"\n✓ Total: {len(field_changes_df)} field changes detected for {field_changes_df['userid'].nunique()} users\n")
    else:
        logger.info("\n✓ No field changes detected\n")
        field_changes_df = None
    
    return field_changes_df


def process_field_updates(field_changes_df):
    """
    Step 9: Process field updates for existing employees through CoreProcessor.
    """
    if not PROCESS_FIELD_UPDATES or field_changes_df is None or len(field_changes_df) == 0:
        logger.info("Skipping field updates processing (disabled or no changes detected)")
        return None
    
    logger.info("=" * 80)
    logger.info("STEP 9: Processing field updates for existing employees")
    logger.info("=" * 80)
    
    # Initialize CoreProcessor (can reuse existing instance or create new one)
    core_processor = CoreProcessor(
        auth_url=auth_endpoint,
        base_url=base_url,
        auth_credentials=auth_credentials,
        ordered_batches=[],
        batches_summary={}
    )
    
    # Process field updates
    update_results = core_processor.process_field_updates(field_changes_df)
    
    logger.info(f"✓ Processed updates for {len(update_results)} users")
    logger.info(f"   - Successful: {sum(1 for ctx in update_results.values() if not ctx.has_errors)}")
    logger.info(f"   - Failed: {sum(1 for ctx in update_results.values() if ctx.has_errors)}\n")
    
    return update_results


def process_inactive_users(inactive_employees_df, cached_pdm_data, cached_ec_data):
    """
    Step 10: Process inactive users (employment termination and account deactivation).
    """
    if not PROCESS_INACTIVE_USERS or inactive_employees_df is None or len(inactive_employees_df) == 0:
        logger.info("Skipping inactive users processing (disabled or no inactive users)")
        return None
    
    logger.info("=" * 80)
    logger.info("STEP 10: Processing inactive users (terminate & disable)")
    logger.info("=" * 80)
    
    # Retrieve inactive users with termination details using InactiveUsersRetriever
    logger.info("Retrieving inactive users data with termination details...")
    
    inactive_retriever = InactiveUsersRetriever(
        inactive_users=inactive_employees_df,
        oracle_dsn=oracle_dsn,
        extract_pdm_inactive_records_query=extract_pdm_inactive_records_query,
        cache_key='inactive_users'
    )
    
    # Store inactive users details in cache (returns boolean)
    success = inactive_retriever.store_inactive_users_details()
    
    if not success:
        logger.warning("Failed to store inactive users details")
        return None
    
    # Retrieve the cached data
    employees_cache = EmployeesDataCache()
    inactive_users_with_details = employees_cache.get('inactive_users_df')
    
    if inactive_users_with_details is None or len(inactive_users_with_details) == 0:
        logger.warning("No inactive users data retrieved from cache")
        return None
    
    logger.info(f"Retrieved termination details for {len(inactive_users_with_details)} inactive users")
    
    # Initialize DisableUsersProcessor
    disable_processor = DisableUsersProcessor(
        inactive_user_df=inactive_users_with_details,
        auth_url=auth_endpoint,
        auth_credentials=auth_credentials,
        base_url=base_url
    )
    
    # Process user deactivation
    disable_results = disable_processor.process_disable_users()
    
    logger.info(f"✓ Processed {len(disable_results)} inactive users")
    logger.info(f"   - Successful: {sum(1 for ctx in disable_results.values() if not ctx.has_errors)}")
    logger.info(f"   - Failed: {sum(1 for ctx in disable_results.values() if ctx.has_errors)}\n")
    
    return disable_results


def save_final_outputs(existing_employees_df, new_employees_df, inactive_employees_df, field_changes_df):
    """
    Save final outputs to CSV files for verification.
    """
    if not SAVE_DEBUG_OUTPUTS:
        return
    
    logger.info("=" * 80)
    logger.info("Saving final outputs")
    logger.info("=" * 80)
    
    output_dir = './test/test_outputs_v1.0'
    os.makedirs(output_dir, exist_ok=True)
    
    existing_employees_df.to_csv(os.path.join(output_dir, 'existing_employees.csv'), index=False)
    logger.info(f"✓ Saved existing employees to {output_dir}/existing_employees.csv")
    
    new_employees_df.to_csv(os.path.join(output_dir, 'new_employees.csv'), index=False)
    logger.info(f"✓ Saved new employees to {output_dir}/new_employees.csv")
    
    if field_changes_df is not None and len(field_changes_df) > 0:
        field_changes_df.to_csv(os.path.join(output_dir, 'field_changes.csv'), index=False)
        logger.info(f"✓ Saved field changes to {output_dir}/field_changes.csv")
    
    logger.info(f"\n✓ All outputs saved to {output_dir}/\n")


def print_final_summary(existing_employees_df, new_employees_df, inactive_employees_df, field_changes_df):
    """
    Print final pipeline summary.
    """
    logger.info("=" * 80)
    logger.info("PIPELINE SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Existing employees: {len(existing_employees_df)}")
    logger.info(f"New employees: {len(new_employees_df)}")
    logger.info(f"Inactive employees: {len(inactive_employees_df) if inactive_employees_df is not None else 0}")
    
    if field_changes_df is not None and len(field_changes_df) > 0:
        logger.info(f"Field changes detected: {len(field_changes_df)} changes across {field_changes_df['userid'].nunique()} users")
    else:
        logger.info("Field changes detected: 0")
    
    logger.info("=" * 80)
    logger.info("✓ Pipeline completed successfully!")
    logger.info("=" * 80)

def send_notification_email(run_id):
    """
    Step 11: Send notification email with pipeline summary.
    1. Prepare email body and save HTML preview to file.
    2. Send email using NotificationHandler.
    3. Log completion.
    """
    if not PROCESS_NOTIFICATIONS:
        logger.info("Skipping notification email (disabled by config)")
        return
    
    logger.info("=" * 80)
    logger.info("STEP 11: Sending notification email")
    logger.info("=" * 80)
    
    postgres_connector = Psycopg2DatabaseConnection(postgres_url)
    tables_names = regular_pipeline_summary_tables | regular_field_changes_tables
    handler = NotificationHandler(
        run_id=run_id,
        postgres_connector=postgres_connector,
        table_names=tables_names
    )
    
    # Prepare email returns tuple: (body, attachments)
    html_body, attachment_files = handler._prepare_email_body()    
    
    # Save HTML preview
    output_file = f"./test/test_output/notification_preview_{run_id}.html"
    os.makedirs('./test/test_output', exist_ok=True)
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_body)
    logger.info(f"HTML preview saved to: {output_file}")
    
    if attachment_files:
        logger.info(f"Attachments prepared: {', '.join([os.path.basename(f) for f in attachment_files])}")

    # Send notification email
    handler.send_pipeline_notification()
    logger.info("✓ Notification email sent successfully\n")


if __name__ == "__main__":
    history_loader = None
    e = None  # Initialize exception variable for finally block
    try:

        start_time = datetime.now()

        logger.info("Starting PDM to EC Data Synchronization Pipeline")
        logger.info("Configuration:")
        logger.info(f"  - Extract DB Data: {EXTRACT_DATABASE_DATA}")
        logger.info(f"  - Extract SAP Data: {EXTRACT_SAP_DATA}")
        logger.info(f"  - Process New Employees: {PROCESS_NEW_EMPLOYEES}")
        logger.info(f"  - Process Field Updates: {PROCESS_FIELD_UPDATES}")
        logger.info(f"  - Process Inactive Users: {PROCESS_INACTIVE_USERS}")
        logger.info(f"  - Save Debug Outputs: {SAVE_DEBUG_OUTPUTS}")
        logger.info("")
        
        # Step 1: Extract and cache database data
        extract_and_cache_database_data()
        
        # Step 2: Extract and cache SAP data
        extract_and_cache_sap_data()
        
        # Step 3: Load cached data
        postgres_cache, oracle_cache, sap_cache, cached_ec_data, cached_pdm_data = load_cached_data()
        
        # Initialize history tracking
        postgres_connector = Psycopg2DatabaseConnection(postgres_url)
        
        history_loader = PipelineHistoryLoader(postgres_connector,table_names=regular_pipeline_summary_tables)
        
        # Calculate total records for processing
        total_records = len(cached_pdm_data) if cached_pdm_data is not None else 0
        run_id = history_loader.start_pipeline_run(total_records, start_time=start_time)
        logger.info(f"Pipeline run started with ID: {run_id}")
        
        # Clean any leftover failures from previous incomplete runs with same run_id
        # This prevents duplicate key violations when re-running after failures
        try:
            conn = postgres_connector.get_postgres_db_connection()
            cursor = conn.cursor()
            # Delete from user_sync_results (not pipeline_run_summary - that's the main history record)
            cursor.execute(f"DELETE FROM {regular_pipeline_summary_tables['user_sync_results']} WHERE run_id = %s", (run_id,))
            # Delete from employee_field_changes and batches
            cursor.execute(f"DELETE FROM {regular_field_changes_tables['employee_field_changes']} WHERE batch_id IN (SELECT batch_id FROM {regular_field_changes_tables['employee_field_changes_batches']} WHERE run_id = %s)", (run_id,))
            cursor.execute(f"DELETE FROM {regular_field_changes_tables['employee_field_changes_batches']} WHERE run_id = %s", (run_id,))
            conn.commit()
            logger.info(f"Cleaned any previous data for run_id: {run_id}")
        except Exception as e:
            logger.warning(f"Could not clean previous run data: {e}")
            if conn:
                conn.rollback()
        finally:
            if cursor:
                cursor.close()
            if conn:
                conn.close()
        
        # Save SAP cache debug files
        save_sap_cache_debug_files(sap_cache)
        
        # Step 4: Extract employee classifications
        existing_employees_df, new_employees_df, inactive_employees_df = extract_employee_classifications(cached_pdm_data, cached_ec_data)
        
        # Validate new employees
        validate_new_employees(new_employees_df, sap_cache)
        
        # Step 5: Prepare new employees data
        new_employees_df = prepare_new_employees_data(new_employees_df)
        
        # Step 6: Resolve creation order
        batches, summary = resolve_creation_order(new_employees_df, existing_employees_df)
        
        # Save creation batches
        save_creation_batches(batches)
        
        # Initialize result variables
        new_employee_results = None
        field_changes_df = None
        update_results = None
        disable_results = None
        
        # Step 7: Process new employees
        new_employee_results = process_new_employees(new_employees_df, batches, summary)
        
        # Filter out new employees from existing_employees_df to avoid double-counting
        # New employees should not be considered for field changes
        if len(new_employees_df) > 0:
            new_userids = set(new_employees_df['userid'].astype(str).str.lower())
            existing_employees_df = existing_employees_df[
                ~existing_employees_df['userid'].astype(str).str.lower().isin(new_userids)
            ]
            logger.info(f"Filtered {len(new_userids)} new employees from existing users list")
            logger.info(f"Remaining existing users for field change detection: {len(existing_employees_df)}")
        
        # Step 8: Detect field changes for existing employees (pass run_id to link batches)
        field_changes_df = detect_field_changes(cached_pdm_data, cached_ec_data, existing_employees_df, run_id)
        
        # Step 9: Process field updates
        update_results = process_field_updates(field_changes_df)
        
        # Step 10: Process inactive users
        disable_results = process_inactive_users(inactive_employees_df, cached_pdm_data, cached_ec_data)
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}", exc_info=True)
        
    finally:
        # Collect history from all processing results (even if pipeline failed)
        if history_loader is None:
            logger.warning("History loader not initialized, skipping history save")
        else:
            try:
                created_count = 0
                updated_count = 0
                terminated_count = 0
                total_failed = 0
                total_warnings = 0
                all_results = []
            
                # Initialize temp_processor for history extraction
                temp_processor = CoreProcessor(
                    auth_url=auth_endpoint,
                    base_url=base_url,
                    auth_credentials=auth_credentials,
                    ordered_batches=[],
                    batches_summary={}
                )
            
                # Extract history from new employees
                if new_employee_results:
                    new_emp_history = temp_processor.extract_history_data(new_employee_results, 'CREATE')
                    created_count = new_emp_history['success_count']
                    total_failed += new_emp_history['failed_count']
                    total_warnings += new_emp_history['warning_count']
                    all_results.extend(new_emp_history['results'])

                # Extract history from field updates
                if update_results:
                    # Debug: log direct counts from update_results before extraction
                    try:
                        logger.info(f"Pre-history extraction: update_results_len={len(update_results)}; direct_success_count={sum(1 for ctx in update_results.values() if not ctx.has_errors)}; direct_failed_count={sum(1 for ctx in update_results.values() if ctx.has_errors)}")
                    except Exception:
                        logger.info("Pre-history extraction: failed to compute direct counts for update_results")

                    update_history = temp_processor.extract_history_data(update_results, 'UPDATE')
                    updated_count = update_history['success_count']
                    total_failed += update_history['failed_count']
                    total_warnings += update_history['warning_count']
                    all_results.extend(update_history['results'])

                # Extract history from terminations
                if disable_results:
                    temp_disable_processor = DisableUsersProcessor(
                        inactive_user_df=pd.DataFrame(),
                        auth_url=auth_endpoint,
                        auth_credentials=auth_credentials,
                        base_url=base_url,
                        max_retries=5
                    )
                    terminate_history = temp_disable_processor.extract_history_data(disable_results)
                    terminated_count = terminate_history['success_count']
                    total_failed += terminate_history['failed_count']
                    total_warnings += terminate_history['warning_count']
                    all_results.extend(terminate_history['results'])
                # Save results to database
                if all_results:
                    history_loader.bulk_insert_results(all_results)

                # Complete pipeline run with final counts
                # Debug: log extracted history details before saving to DB
                try:
                    logger.info(f"History to save - created_count={created_count}, updated_count={updated_count}, terminated_count={terminated_count}, failed_count={total_failed}, warning_count={total_warnings}")
                    # If available, log the detailed history objects
                    if 'new_emp_history' in locals():
                        logger.info(f"new_emp_history: {new_emp_history}")
                    if 'update_history' in locals():
                        logger.info(f"update_history: {update_history}")
                    if 'terminate_history' in locals():
                        logger.info(f"terminate_history: {terminate_history}")
                except Exception as dbg_e:
                    logger.warning(f"Failed to log history debug info: {dbg_e}")

                history_loader.complete_pipeline_run(
                    created_count=created_count,
                    updated_count=updated_count,
                    terminated_count=terminated_count,
                    failed_count=total_failed,
                    warning_count=total_warnings,
                    error_message=str(e) if 'e' in locals() else None
                )

                logger.info(f"Pipeline run {run_id} completed with history tracking")

                # Step 11: Send notification email AFTER history is saved
                try:
                    send_notification_email(run_id)
                except Exception as notif_err:
                    logger.error(f"Failed to send notification email: {notif_err}", exc_info=True)
                    
            except Exception as hist_err:
                logger.error(f"Failed to save pipeline history: {hist_err}", exc_info=True)
        
        # Save final outputs
        if field_changes_df is not None:
            try:
                save_final_outputs(existing_employees_df, new_employees_df, inactive_employees_df, field_changes_df)
            except Exception as save_err:
                logger.error(f"Failed to save final outputs: {save_err}")
        
        # Print final summary
        if field_changes_df is not None:
            try:
                print_final_summary(existing_employees_df, new_employees_df, inactive_employees_df, field_changes_df)
            except Exception as sum_err:
                logger.error(f"Failed to print final summary: {sum_err}")