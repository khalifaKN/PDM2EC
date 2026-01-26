from datetime import datetime, timezone
import pandas as pd
from typing import Optional, Union
from utils.logger import get_logger

"""
Date Converter Utility:

Provides functions to convert various date formats (Oracle, PostgreSQL, datetime objects) to the /Date(XXXXXX)/ format used by SuccessFactors API.

"""

logger = get_logger('date_converter')

def convert_to_unix_timestamp(date_input: Union[str, datetime, None]) -> Optional[str]:
    """ 
    Convert a date string or datetime object to /Date(XXXXXX)/ format.
    Supports multiple input formats including Oracle (MM/DD/YYYY), PostgreSQL (YYYY-MM-DD),
    and datetime objects. Used for SuccessFactors API date fields.
    
    Args:
        date_input: Date as string, datetime object, or None
        
    Returns:
        Date in /Date(XXXXXX)/ format, or None if input is invalid
    """
    if not date_input or pd.isna(date_input):
        return None

    # If already in correct format, return as-is
    if isinstance(date_input, str) and date_input.startswith('/Date(') and date_input.endswith(')/'):
        return date_input

    # If input is already a datetime object
    if isinstance(date_input, datetime):
        unix_timestamp = int(date_input.replace(tzinfo=timezone.utc).timestamp() * 1000)
        return f"/Date({unix_timestamp})/"
    
    # If input is a pandas Timestamp
    if isinstance(date_input, pd.Timestamp):
        unix_timestamp = int(date_input.tz_localize(timezone.utc).timestamp() * 1000)
        return f"/Date({unix_timestamp})/"

    # Try parsing string with various formats
    formats = [
        "%Y-%m-%d %H:%M:%S",      # PostgreSQL datetime
        "%Y-%m-%d",                # ISO format / PostgreSQL date
        "%m/%d/%Y",                # Oracle format
        "%m-%d-%Y",                # Alternative format
        "%d/%m/%Y",                # European format
        "%Y/%m/%d"                 # Alternative ISO
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(str(date_input), fmt)
            unix_timestamp = int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)
            return f"/Date({unix_timestamp})/"
        except (ValueError, TypeError):
            continue  # Try the next format

    logger.warning(f"Unable to parse date format: {date_input}. Returning None.")
    return None