from utils.logger import get_logger
import pandas as pd

logger = get_logger('job_existence_validator')

class JobExistenceValidator:
    """
    Existence Validator class to check the existence of job codes in SAP system 
    from the cached job mappings.
    """

    def __init__(self, job_mappings: pd.DataFrame,job_code: str, raise_if_missing=False):
        self.job_mappings = job_mappings
        self.job_code = job_code
        self.raise_if_missing = raise_if_missing
        self.requested_fields = ['bufu_id', 'cust_geographicalscope', 'cust_subunit']

    def get_job_mapping(self) -> pd.DataFrame:
        """
        Retrieves the job mapping for the specified job code and ensures required fields are present and not NaN.
        Returns:
            pd.DataFrame: DataFrame containing the job mapping. 
            if the job code does not exist and raise_if_missing is True, raises a ValueError.
            if the job code does not exist and raise_if_missing is False, returns an empty DataFrame.
        """
        job_titles_code = self.job_mappings['jobcode'].astype(str).str.lower()
        mask = job_titles_code.eq(self.job_code.lower())
        result = self.job_mappings[mask].copy()
        
        if result.empty:
            msg = f"Job code {self.job_code} not found in job mappings."
            if self.raise_if_missing:
                logger.error(msg)
                raise ValueError(msg)
        # Check if required fields exist and have no NaN values
        missing_fields = [field for field in self.requested_fields if field not in result.columns or result[field].isna().any()]
        if missing_fields:
            msg = f"Missing or NaN required fields for job code {self.job_code}: {', '.join(missing_fields)}"
            if self.raise_if_missing:
                logger.error(msg)
                raise ValueError(msg)
        else:
            # Force all values to strings and clean .0 endings
            result = result[self.requested_fields].astype(str).replace(r'\.0$', '', regex=True)
        return result