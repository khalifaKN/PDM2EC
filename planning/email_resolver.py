from cache.oracle_cache import OracleDataCache
from config.excluded_users_emails import USERS_TO_BE_EXCLUDED
from cache.sap_cache import SAPDataCache
from utils.logger import get_logger
import pandas as pd
import hashlib

Logger = get_logger("email_resolver")

class EmailResolver:
    """
    This class resolves the next:
    1. Check if User has email in SAP data (self.sap_email_data)
    2. If yes, it check if email ends with @kn.com, 
        2.1 If yes then it's anonymized and shouldn't be updated
        2.2 If No then next step
    3. If Email not in SAP data or email is not anonymized we check the next:
        3.1 If user not in HR Global users and not in the hardcoded list of internal users ids USERS_TO_BE_EXCLUDED then:
            - email it has to be anonymized with putting f"user{hash_value}@kn.com"
        3.2 Else we take the email from PDM as is (it can be internal or external)
    
        Args:
        Internal instance variables:
            sap_cache (SAPDataCache): Cache to get SAP email data.
            oracle_cache (OracleDataCache): Cache for Oracle HR global users data.
            USERS_TO_BE_EXCLUDED (list): List of user IDs to be excluded from anonymization.
        Parameters:
            pdm_users_data (pd.DataFrame): DataFrame containing PDM users data.
        Returns:
            str: Dataframe which contains userid and resolved email as columns.
    """
    def __init__(self):
        self.sap_cache = SAPDataCache()
        self.oracle_cache = OracleDataCache()
        self.users_to_be_excluded = USERS_TO_BE_EXCLUDED
        
    def resolve_user_email(self, userid: str, pdm_row: pd.Series, hr_global_users:set, sap_email_data: pd.DataFrame) -> dict:
        """
        Returns safe emails for the user:
        {
            "business_email": str | None,
            "private_email": str | None
        }
        """
        user_id = userid.strip().lower()
        pdm_email = pdm_row.get("email")
        private_email = pdm_row.get("private_email")
        # Convert from list of int to list of str
        self.users_to_be_excluded = set(str(uid).strip() for uid in self.users_to_be_excluded)

        is_allowed_real_email = (
            user_id in hr_global_users
            or user_id in self.users_to_be_excluded
        )
        sap_user_emails = sap_email_data[sap_email_data['personidexternal'] == user_id]
        primary = sap_user_emails[sap_user_emails['isprimary'] == 'Y']
        if not primary.empty:
            sap_email = primary.iloc[0]['emailaddress']
            if sap_email.lower().endswith('@kn.com'):
                return {
                    "business_email": sap_email,
                    "private_email": None
                }
        def anonymize(uid):
            hash_value = hashlib.md5(uid.encode()).hexdigest()
            return f"user{hash_value}@kn.com"
        # Default: anonymize
        resolved_business = anonymize(user_id)
        resolved_private = anonymize(user_id)

        # If allowed → use PDM values
        if is_allowed_real_email:
            return {
                "business_email": pdm_email,
                "private_email": private_email
            }

        # If SAP has a non-anonymized primary email, allow it
        if sap_email_data is not None and not sap_email_data.empty:
            sap_user_emails = sap_email_data[sap_email_data['personidexternal'] == user_id]
            primary = sap_user_emails[sap_user_emails['isprimary'] == 'Y']
            if not primary.empty:
                sap_email = primary.iloc[0]['emailaddress']
                if sap_email and not sap_email.lower().endswith('@kn.com'):
                    resolved_business = sap_email.lower()
                    Logger.info(f"Using SAP email for user {userid}: {resolved_business}")
        return {
            "business_email": resolved_business,
            "private_email": resolved_private
        }