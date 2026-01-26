from payload_builders.employment.payloads.empemploymentermination import get_emp_employment_termination_payload
from utils.logger import get_logger
from cache.postgres_cache import PostgresDataCache
Logger = get_logger("build_employment_termination_payloads")


class EmploymentTerminationPayloadBuilder:
    """
    A class to build employment termination payloads for employees.
    """

    def __init__(self,user_id: str, end_date: str, terminate_event_reason: str = "TERRTMNT"):
        self.user_id = user_id
        self.end_date = end_date
        self.terminate_event_reason = terminate_event_reason
        self.postgres_cache = PostgresDataCache()
        
    def _get_userid_personIdExternal(self, person_id):
        """Retrieve USERID from PERSON_ID_EXTERNAL using cached data if they are different."""
        different_userid_personid_df = self.postgres_cache.get(
            "different_userid_personid_data_df"
        )
        result = {
            "userId": person_id,
            "personIdExternal": person_id
        }
        if different_userid_personid_df is not None:
            match = different_userid_personid_df[
                (different_userid_personid_df["person_id_external"] == person_id) | 
                (different_userid_personid_df["userid"] == person_id)
            ]
            if not match.empty:
                result= {"userId": match["userid"].values[0], "personIdExternal": match["person_id_external"].values[0]}
        return result

    def build_emp_employment_termination_payload(self):
        try:
            user_ids = self._get_userid_personIdExternal(self.person_id_external)
            payload = get_emp_employment_termination_payload()
            payload["userId"] = user_ids["userId"]
            payload["personIdExternal"] = user_ids["personIdExternal"]
            payload["endDate"] = self.end_date
            payload["eventReason"] = self.terminate_event_reason
            return payload
        except Exception as e:
            Logger.error(
                f"Error building EmpEmploymentTermination payload for {self.user_id}: {e}"
            )
            return None