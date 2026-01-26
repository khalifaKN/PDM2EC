"""
Field mappings between Oracle PDM query output and PostgreSQL EC table.
Two mappings are provided:
1. EC_TABLE_TO_PDM_FIELD_MAPPING_SCM_IM: For SCM and IM users, covering a wide range of fields.
2. EC_TABLE_TO_PDM_FIELD_MAPPING_STANDARD_USERS: For standard users, focusing on email fields.
"""


# Mapping: EC table column -> PDM query field
# For SCM and IM users
EC_TABLE_TO_PDM_FIELD_MAPPING_SCM_IM = {
    'firstname': 'firstname',
    'mi': 'mi',
    'lastname': 'lastname',
    'nickname': 'nickname',
    'gender': 'gender',
    'start_of_employment': 'start_of_employment',
    'date_of_position': 'date_of_position',
    'date_of_birth': 'date_of_birth',
    'hiredate': 'hiredate',
    'email': 'email',
    'email_2': 'private_email',
    'biz_phone': 'biz_phone',
    'manager': 'manager',
    'matrix_manager': 'matrix_manager',
    'hr': 'hr',
    'jobcode': 'jobcode',
    'ep_ec_role':'custom_string_8',
}
# For standard users
EC_TABLE_TO_PDM_FIELD_MAPPING_STANDARD_USERS = {
    'email': 'email',
    'email_2': 'private_email',
}



def get_pdm_column_value( ec_column: str,scm_im: bool = True):
    """
    Get the value from PDM row for a given EC table column.
    
    Args:
        pdm_row: Dictionary with PDM query field names as keys
        ec_column: EC table column name
        
    Returns:
        Value from PDM row if mapping exists, None otherwise
    """
    if scm_im:
        pdm_field = EC_TABLE_TO_PDM_FIELD_MAPPING_SCM_IM.get(ec_column)
    else:
        pdm_field = EC_TABLE_TO_PDM_FIELD_MAPPING_STANDARD_USERS.get(ec_column)
    if pdm_field is None:
        return None
    return pdm_field