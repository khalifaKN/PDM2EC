from utils.logger import get_logger
from payload_builders.employment.payloads.empemployment import get_empemployment_payloads
from payload_builders.employment.payloads.empjob import get_emp_job
from payload_builders.employment.payloads.empjobrelationships import get_empjob_relationships_payload
from cache.postgres_cache import PostgresDataCache
from utils.date_converter import convert_to_unix_timestamp

import datetime

Logger = get_logger("build_employment_payloads")


class EmploymentPayloadBuilder:
    """
    A class to build employment payloads for employees.
    """

    def __init__(
        self,
        user_id: str,
        person_id_external: str,
        hire_date: str,
        start_of_employment: str,
        company: str,
        build_event_reason: str,
        cost_center: str,
        seq_num: str = None,
        position: str = None,
        manager_id: str = None,
        end_date: str = None,
        terminate_event_reason: str = "TERRTMNT",
        delimit: bool = False,
        rel_user_id: str = None,
        old_rel_user_id: str = None,
        start_date: str = None,
        manager_position_start_date: str = None,
    ):
        self.user_id = user_id
        self.person_id_external = person_id_external
        self.hire_date = hire_date
        self.position = position
        self.start_of_employment = start_of_employment
        self.company = company
        self.build_event_reason = build_event_reason
        self.seq_num = seq_num
        self.cost_center = cost_center
        self.manager_id = manager_id
        self.end_date = end_date
        self.terminate_event_reason = terminate_event_reason
        self.delimit = delimit
        self.rel_user_id = rel_user_id
        self.old_rel_user_id = old_rel_user_id
        self.postgres_cache = PostgresDataCache()
        self.calculated_start_date = None  # Store the calculated startDate for reuse
        self.start_date = start_date
        self.manager_position_start_date = manager_position_start_date

    def build_empemployment_payload(self):
        try:
            # Check if hire_date is after start_of_employment, the current format are "/Date(UnixTimestamp)/" like /Date(1768348800000)/
            hire_date_newest = False
            hire_date_ts = int(self.hire_date.strip("/Date()")) // 1000
            start_of_employment_ts = int(self.start_of_employment.strip("/Date()")) // 1000
            if hire_date_ts > start_of_employment_ts:
                Logger.warning(
                    f"Hire date {self.hire_date} is after start of employment {self.start_of_employment} for user {self.user_id}. Using start of employment as hire date."
                )
                hire_date_newest = True
            payload = get_empemployment_payloads()
            payload["userId"] = self.user_id
            payload["personIdExternal"] = self.person_id_external
            payload["startDate"] = self.start_of_employment if not hire_date_newest else self.hire_date
            payload["originalStartDate"] = self.hire_date if not hire_date_newest else self.start_of_employment
            payload["serviceDate"] = self.hire_date
            return payload
        except Exception as e:
            Logger.error(
                f"Error building EmpEmployment payload for {self.user_id}: {e}"
            )
            return None
        
    def _get_userid_from_personid(self, person_id):
        """Retrieve USERID from PERSON_ID_EXTERNAL using cached data if they are different."""
        different_userid_personid_df = self.postgres_cache.get(
            "different_userid_personid_data_df"
        )
        if different_userid_personid_df is not None:
            match = different_userid_personid_df[
                different_userid_personid_df["person_id_external"] == person_id
            ]
            if not match.empty:
                return match["userid"].values[0]
        return person_id
    
    def _normalize_relationship_start_date(self, candidate_date):
        """
        Ensures relationship start date is not before hire start date.
        """
        hire_start = self.start_of_employment
        if candidate_date is None:
            return hire_start

        return max(candidate_date, hire_start)
    
    def build_empjob_payload(self, migration_flag: bool = False):
        try:
            payload = get_emp_job()
            # Check if hire_date is after start_of_employment, the current format are "/Date(UnixTimestamp)/" like /Date(1768348800000)/
            manager_position_start_date_ts = None
            if self.manager_position_start_date:
                manager_position_start_date_ts = int(self.manager_position_start_date.strip("/Date()")) // 1000

            # INITLOAD: use the oldest date (hire_date)
            # DATACHG: use the newest date (start_of_employment)
            if self.build_event_reason == "INITLOAD":
                    start_date_ = self.hire_date

            else:
                    start_date_ = self.start_of_employment
                
            if manager_position_start_date_ts:
                if manager_position_start_date_ts > int(start_date_.strip("/Date()")) // 1000:
                    Logger.warning(
                        f"Manager position start date {self.manager_position_start_date} is after calculated start date {start_date_} for user {self.user_id}. Using manager position start date as start date."
                    )
                    start_date_ = convert_to_unix_timestamp(self.manager_position_start_date)

            # If we have last EmpJob startDate (for existing employees), validate against backdating
            if self.start_date:
                converted_start_date = convert_to_unix_timestamp(self.start_date)
                if converted_start_date:
                    last_empjob_ts = int(converted_start_date.strip("/Date()")) // 1000
                    base_start_date_ts = int(start_date_.strip("/Date()")) // 1000
                    
                    # If last EmpJob date is NEWER than our calculated base date, we're backdating → use current date
                    if last_empjob_ts > base_start_date_ts:
                        Logger.warning(
                            f"Last EmpJob startDate ({self.start_date}) is newer than calculated startDate for user {self.user_id}. Using current date to avoid backdating."
                        )
                        current_date_ = convert_to_unix_timestamp(datetime.datetime.now(datetime.timezone.utc).strftime("%m/%d/%Y"))
                        start_date_ = current_date_
            
            # Store calculated startDate for reuse
            self.calculated_start_date = start_date_
            payload["startDate"] = start_date_
            
            payload["userId"] = self.user_id
            payload["position"] = self.position
            payload["company"] = self.company
            # seqNumber=1 INITLOAD, else DATACHG
            if self.build_event_reason:
                payload["eventReason"] = self.build_event_reason
            payload["seqNumber"] = self.seq_num
            payload["costCenter"] = self.cost_center
            # Always set managerId, use "NO_MANAGER" if missing or invalid
            if migration_flag:
                payload["managerId"] = "NO_MANAGER"
            elif not self.manager_id or str(self.manager_id).strip().lower() in ["", "none", "no_manager"]:
                payload["managerId"] = "NO_MANAGER"
            else:
                manager_id_ = self._get_userid_from_personid(self.manager_id)
                payload["managerId"] = str(manager_id_).strip()
            # Filter out empty values (but keep "NO_MANAGER")
            payload = {k: v for k, v in payload.items() if v not in ["", None]}
            return payload
        except Exception as e:
            Logger.error(f"Error building EmpJob payload for {self.user_id}: {e}")
            return None

    def build_empjob_relationships_payload(self,relationship_type: str = None, rel_user_id: str = None, old_rel_user_id: str = None, relationship_start_date: str = None):
        try:
            # Use passed relationship_type or fall back to self.relationship_type
            rel_type = relationship_type
            rel_user_id_ = self._get_userid_from_personid(rel_user_id) if rel_user_id else None
            # Skip if relationship_type is not configured (needs SAP picklist ID)
            if not rel_type:
                Logger.warning(f"Skipping relationship for {self.user_id} - relationshipType not configured")
                return None
                
            payload = get_empjob_relationships_payload()
            # Use provided start_date or hire_date to match the hire record
            old_rel_user_id_ = self._get_userid_from_personid(old_rel_user_id) if old_rel_user_id else None
            if old_rel_user_id:
                payload["relUserId"] = old_rel_user_id_ or self.old_rel_user_id
                payload["operation"] = "DELIMIT"
            else:
                payload["relUserId"] = rel_user_id_  # Use parameter instead of self.rel_user_id
            payload["userId"] = self.user_id
            payload["startDate"] = relationship_start_date
            payload["relationshipType"] = rel_type
            return payload
        except Exception as e:
            Logger.error(
                f"Error building EmpJobRelationships payload for {self.user_id}: {e}"
            )
            return None


