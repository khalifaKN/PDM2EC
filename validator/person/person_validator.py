from utils.logger import get_logger
import pandas as pd

Logger = get_logger("person_validator")

class PersonValidator:
    """
    A class to validate person data.
    """

    def __init__(self, record: pd.Series, required_fields: list,person_df: pd.DataFrame):
        self.record = record
        self.required_fields = required_fields
        self.person_df = person_df
    
    def validate_required_fields(self) -> bool:
        """
        Validates that all required fields are present in the record.
        Returns:
            bool: True if all required fields are present, False otherwise.
        """
        missing_fields = [field for field in self.required_fields if field not in self.record or pd.isna(self.record[field])]
        # store for callers
        self.missing_fields = missing_fields
        if missing_fields:
            Logger.error(f"Missing required fields: {', '.join(missing_fields)}")
            return False
        Logger.info("All required fields are present.")
        return True
    
    def personid_exists(self) -> bool:
        """
        Checks if the person ID exists in the person data.
        Returns:
            bool: True if the person ID exists, else False.
        """
        try:
            person_id = str(self.record.get('userid')).strip()
            if person_id == "" or pd.isna(person_id):
                Logger.info("Person ID is missing in the record.")
                return False
            
            # Check if person_df is None or empty
            if self.person_df is None or self.person_df.empty:
                Logger.info("Person data is not available for validation.")
                return False
            
            mask = self.person_df['personidexternal'].astype(str).str.lower().eq(person_id.lower())
            exists = not self.person_df[mask].empty
            if exists:
                Logger.info(f"Person ID {person_id} exists in the person data.")
            else:
                Logger.info(f"Person ID {person_id} does not exist in the person data.")
            return exists
        except Exception as e:
            Logger.error(f"Error checking person ID existence: {str(e)}")
            return False
    def check_changes(self, fields_to_check: list)->bool:
        """
        Checks if there are changes between the record and the cached person data
        based on fields_to_check list.
        Returns:
            bool: True if changes are detected, False otherwise.
        """
        try:
            # Check if person_df is available
            if self.person_df is None or self.person_df.empty:
                Logger.info("Person data is not available for comparison. Assuming changes exist for new employee.")
                return True
            
            # Already validated that person ID exists with personid_exists()
            person_id = str(self.record.get('userid')).strip()            
            mask = self.person_df['personidexternal'].astype(str).str.lower().eq(person_id.lower())
            cached_record = self.person_df[mask]
            if cached_record.empty:
                Logger.info(f"No cached data found for Person ID {person_id}. Assuming changes exist for new employee.")
                return True
            
            for field in fields_to_check:
                record_value = str(self.record.get(field)).strip() if field in self.record else ""
                cached_value = str(cached_record.iloc[0][field]).strip() if field in cached_record.columns else ""
                if record_value != cached_value:
                    Logger.info(f"Change detected for field '{field}': record value '{record_value}' != cached value '{cached_value}'")
                    return True
            Logger.info("No changes detected between record and cached data.")
            return False
        except Exception as e:
            Logger.error(f"Error checking changes for Person ID {person_id}: {str(e)}")
            return False