from utils.logger import get_logger
from mapper.country_mapper import get_iso3_numeric
from utils.date_converter import convert_to_unix_timestamp

logger = get_logger('pdm_data_converter')

def convert_pdm_data(pdm_data):
    """
    Convert date fields in PDM data to /Date(XXXXXX)/ format for SuccessFactors API and handle country codes.

    Args:
        pdm_data (pd.DataFrame): DataFrame containing PDM user data.
    Returns:
        pd.DataFrame: DataFrame with converted date fields and country codes.
    """
    pdm_data = pdm_data.copy()
    date_fields = [
        'date_of_birth',
        'date_of_hire',
        'date_of_position',
        'hr_position_start_date',    
        'matrix_manager_position_start_date',
        'manager_position_start_date'
    ]

    for date_field in date_fields:
        if date_field in pdm_data.columns:
            pdm_data[date_field] = pdm_data[date_field].apply(convert_to_unix_timestamp)

    # Handle country code conversion for 'country' or 'country_code' field if exists
    if 'country' in pdm_data.columns:
        pdm_data['country_iso3'] = pdm_data['country'].apply(get_iso3_numeric)
    elif 'country_code' in pdm_data.columns:
        pdm_data['country_iso3'] = pdm_data['country_code'].apply(get_iso3_numeric)

    logger.info("Converted PDM date fields and country codes.")
    return pdm_data