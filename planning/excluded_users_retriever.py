from utils import logger
import pandas as pd

Logger = logger.get_logger("excluded_users_retriever")

class ExcludedUsersRetriever:
    def __init__(self, excluded_criteria:dict, users_df: pd.DataFrame):
        self.excluded_criteria = excluded_criteria
        self.users_df = users_df

    def _extract_excluded_countries(self):
       
        excluded_countries = self.excluded_criteria.get("excluded_countries", [])
        return [country.strip().lower() for country in excluded_countries if country.strip()]
    
    def _extract_excluded_companies(self) -> set:
        excluded_companies = self.excluded_criteria.get("excluded_companies", [])
        return set(company.strip().lower() for company in excluded_companies if company.strip())
    def _extract_combined_country_company_exclusions(self) -> set:
        """
        Extracts combined country-company exclusions from the criteria.
        Input format: "{
        exclusions: [{country: "country1", company: "company1"}, ...]}
        }"
        Returns:
            Set of tuples: (country, company)
        """
        combined_exclusions = self.excluded_criteria.get("combined_country_company_exclusions", [])
        result = set()
        for exclusion in combined_exclusions:
            country = exclusion.get("country", "").strip().lower()
            company = exclusion.get("company", "").strip().lower()
            if country and company:
                result.add((country, company))
        return result
    
    def _get_excluded_userids(self) -> set:
        excluded_userids = set()

        excluded_countries = self._extract_excluded_countries()
        excluded_companies = self._extract_excluded_companies()
        combined_exclusions = self._extract_combined_country_company_exclusions()

        for _, row in self.users_df.iterrows():
            userid = str(row.get("userid", "")).strip().lower()
            country = str(row.get("country", "")).strip().lower()
            company = str(row.get("company", "")).strip().lower()

            if not userid:
                continue

            # Check country exclusion
            if country in excluded_countries:
                excluded_userids.add(userid)
                continue

            # Check company exclusion
            if company in excluded_companies:
                excluded_userids.add(userid)
                continue

            # Check combined country-company exclusion
            # We should have both country and company to check, Null and empty are ignored
            if country and company and (country, company) in combined_exclusions:
                excluded_userids.add(userid)
                continue

        Logger.info(f"Identified {len(excluded_userids)} excluded user IDs based on criteria.")
        return excluded_userids
    
    def get_cleaned_users_df(self) -> pd.DataFrame:
        excluded_userids = self._get_excluded_userids()
        cleaned_df = self.users_df[~self.users_df["userid"].astype(str).str.lower().isin(excluded_userids)].copy()
        Logger.info(f"Cleaned users DataFrame: {len(cleaned_df)} users remain after exclusions.")
        return cleaned_df
    
    def get_excluded_userids(self) -> set:
        return self._get_excluded_userids()