"""
Country code mapping utilities for SuccessFactors integration.
Handles ISO2 to ISO3 conversions and phone number country detection.
"""
import pycountry
import phonenumbers
from utils.logger import get_logger

logger = get_logger('country_mapper')


def get_iso3_numeric(iso2):
    """
    Convert a 2-letter ISO country code to a 3-letter code using pycountry.
    
    Args:
        iso2 (str): 2-letter ISO country code (e.g., 'US', 'GB')
    
    Returns:
        str: 3-letter ISO country code (e.g., 'USA', 'GBR') or None if not found
    
    Example:
        >>> get_iso3_numeric('US')
        'USA'
        >>> get_iso3_numeric('GB')
        'GBR'
    """
    if not iso2:
        return None
    
    try:
        country = pycountry.countries.get(alpha_2=iso2)
        return country.alpha_3 if country else None
    except Exception as e:
        logger.error(f"Error converting ISO2 code '{iso2}': {e}")
        return None


def get_iso3_from_phone(phone_number):
    """
    Extract ISO3 country code from a phone number.
    
    Args:
        phone_number (str): Phone number in any format (e.g., '+1234567890', '(123) 456-7890')
    
    Returns:
        str: 3-letter ISO country code or None if parsing fails
    
    Example:
        >>> get_iso3_from_phone('+14155552671')
        'USA'
        >>> get_iso3_from_phone('+442071234567')
        'GBR'
    """
    if not phone_number:
        return None
    
    try:
        parsed_phone = phonenumbers.parse(phone_number, None)
        region_code = phonenumbers.region_code_for_country_code(parsed_phone.country_code)
        return get_iso3_numeric(region_code)
    except Exception as e:
        logger.debug(f"Could not extract country from phone '{phone_number}': {e}")
        return None


def get_country_code_from_phone(phone_number):
    """
    Extract country calling code from a phone number (e.g., '+1' for US/Canada).
    
    Args:
        phone_number (str): Phone number in any format
    
    Returns:
        str: Country calling code with '+' prefix (e.g., '+1', '+44') or None
    
    Example:
        >>> get_country_code_from_phone('+14155552671')
        '+1'
        >>> get_country_code_from_phone('+442071234567')
        '+44'
    """
    if not phone_number:
        return None
    
    try:
        parsed_phone = phonenumbers.parse(phone_number, None)
        return f"+{parsed_phone.country_code}"
    except Exception as e:
        logger.debug(f"Could not extract country code from phone '{phone_number}': {e}")
        return None


def get_national_number(phone_number):
    """
    Extract the national number portion from a phone number (without country code).
    
    Args:
        phone_number (str): Phone number in any format
    
    Returns:
        str: National number portion or None
    
    Example:
        >>> get_national_number('+14155552671')
        '4155552671'
    """
    if not phone_number:
        return None
    
    try:
        parsed_phone = phonenumbers.parse(phone_number, None)
        return str(parsed_phone.national_number)
    except Exception as e:
        logger.debug(f"Could not extract national number from phone '{phone_number}': {e}")
        return None
