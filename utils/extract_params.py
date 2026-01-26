from utils.logger import get_logger
from urllib.parse import parse_qs, unquote

Logger= get_logger('sap_data_cache')

def extract_sap_params(params:str):
    """
    Extracts SAP connection parameters from the given str.

    Args:
        params (str): A string containing SAP connection parameters. exp "$format=json&$select=jobCode,jobTitle,code,effectiveStartDate,standardHours,location,externalName_defaultValue"

    Returns:
        dict: A dictionary with extracted SAP parameters.
    """
    sap_params = {}
    if params:
        pairs = params.split('&')
        for pair in pairs:
            if '=' in pair:
                key, value = pair.split('=', 1)
                sap_params[key] = value
    Logger.info(f"Extracted SAP parameters: {sap_params}")
    return sap_params

def extract_sap_params_safe(params: str):
    """
    Extracts SAP connection parameters from the given str safely using urllib.parse.

    Args:
        params (str): A string containing SAP connection parameters. exp "$format=json&$select=jobCode,jobTitle,code,effectiveStartDate,standardHours,location,externalName_defaultValue"

    Returns:
        dict: A dictionary with extracted SAP parameters.
    """
    sap_params = {}
    if params:
        parsed = parse_qs(params, keep_blank_values=True)
        # parse_qs returns lists for each key, take first value
        for k, v in parsed.items():
            sap_params[k] = unquote(v[0]) if v else ''
    Logger.info(f"Extracted SAP parameters safely: {sap_params}")
    return sap_params