import phonenumbers
from utils.logger import get_logger
from mapper.country_mapper import get_iso3_numeric
logger = get_logger('phone_mapper')

def build_per_phone_payload(person_id, phone_number, phone_type):
    """
    Build payload for PerPhone entity (business phone/mobile) with country code logic.
    phone_type: 'BIZ_PHONE' or 'BIZ_MOBILE'
    """
    phone_type_map = {
        "BIZ_PHONE": "18258",   # Business phone
        "BIZ_MOBILE": "18257",  # Business mobile
        "PRIVATE": "20221"      # Private phone (default)
    }
    # Default values
    parsed_country_code = None
    iso3_code = None
    national_number = None
    try:
        parsed_phone = phonenumbers.parse(phone_number, None)
        parsed_country_code = f"+{parsed_phone.country_code}"
        region_code = phonenumbers.region_code_for_country_code(parsed_phone.country_code)
        iso3_code = get_iso3_numeric(region_code)
        national_number = str(parsed_phone.national_number)
    except Exception as e:
        logger.info(f"No valid phone data for {person_id} - {phone_number}: {e}")
        return None
    if parsed_country_code and iso3_code:
        payload = {
            "__metadata": {"uri": "PerPhone"},
            "personIdExternal": person_id,
            "phoneType": phone_type_map.get(phone_type, "20221"),
            "customString1": iso3_code,
            "countryCode": parsed_country_code,
            "phoneNumber": national_number,
            "isPrimary": True
        }
        return payload
    else:
        logger.info(f"No valid phone data for {person_id} - {phone_number}")
        return None