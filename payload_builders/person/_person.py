from payload_builders.person.payloads.peremail import get_per_email_payload
from payload_builders.person.payloads.perperson import get_perperson_payload
from payload_builders.person.payloads.perpersonal import get_perpersonal_payload
from payload_builders.person.payloads.perphone import get_perphone_payload
from mapper.country_mapper import get_iso3_numeric
from utils.date_converter import convert_to_unix_timestamp
import phonenumbers
from utils.logger import get_logger

Logger = get_logger("build_person_payloads")


class PersonPayloadBuilder:
    """
    A class to build person payloads for employees.
    """

    def __init__(
        self,
        person_id_external: str,
        first_name: str,
        last_name: str,
        date_of_birth: str,
        start_date: str,
        email: str,
        private_email: str = None,
        middle_name: str = None,
        nickname: str = None,
        phone: str = None,
        gender: str = None,
        postgres_cache = None,
    ):
        self.postgres_cache = postgres_cache
        self.person_id_external = self._get_userid_from_personid(person_id_external)
        self.first_name = first_name
        self.last_name = last_name
        self.middle_name = middle_name
        self.nickname = nickname
        self.date_of_birth = date_of_birth
        self.start_date = start_date
        self.email = email
        self.private_email = private_email        
        self.phone = phone
        self.gender = gender
        self.last_error = None

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

    def build_perperson_payload(self):
        try:
            payload = get_perperson_payload()
            payload["personIdExternal"] = self.person_id_external
            if self.date_of_birth:
                payload["dateOfBirth"] = convert_to_unix_timestamp(self.date_of_birth)
            return payload
        except Exception as e:
            Logger.error(
                f"Error building PerPerson payload for {self.person_id_external}: {e}"
            )
            return None
    
    def build_perpersonal_payload(self):
        try:
            payload = get_perpersonal_payload()
            payload["personIdExternal"] = self.person_id_external
            payload["firstName"] = self.first_name
            payload["lastName"] = self.last_name
            if self.middle_name:
                payload["middleName"] = self.middle_name
            if self.nickname:
                payload["preferredName"] = self.nickname
            payload["startDate"] = convert_to_unix_timestamp(self.start_date)
            if self.gender:
                payload["gender"] = self.gender
            else:
                payload["gender"] = "M" # Default
            return payload
        except Exception as e:
            Logger.error(
                f"Error building PerPersonal payload for {self.person_id_external}: {e}"
            )
            return None

    def build_peremail_payload(self,is_business_email: bool = True):
        try:
            payload = get_per_email_payload()
            if is_business_email:
                payload["emailType"] = "18242"  # Business email
                payload["isPrimary"] = True
                payload["emailAddress"] = self.email
            else:
                payload["emailType"] = "18240"  # Personal email
                payload["isPrimary"] = False
                payload["emailAddress"] = self.private_email if self.private_email else self.email
            payload["personIdExternal"] = self.person_id_external
            return payload
        except Exception as e:
            self.last_error = str(e)
            Logger.error(
                f"Error building PerEmail payload for {self.person_id_external}: {e}"
            )
            return None
    def build_peremail_payload_action(
        self,
        email: str,
        email_type: int,
        is_primary: bool = False,
        action: str = "INSERT"  # INSERT, UPDATE, DELETE
    ):
        """
        Build PerEmail payload for a specific action.
        Args:
            email: email address
            email_type: 18242 (business) or 18240 (private)
            is_primary: whether this email should be primary
            action: 'INSERT', 'UPDATE', 'DELETE'
        Returns:
            dict payload for peremail
        """
        try:
            payload = get_per_email_payload()
            payload["personIdExternal"] = self.person_id_external
            payload["emailAddress"] = email
            payload["emailType"] = str(email_type)
            payload["isPrimary"] = is_primary
            if action.upper() == "DELETE":
                payload["operation"] = "DELETE"
            return payload
        except Exception as e:
            Logger.error(
                f"Error building PerEmail payload ({action}) for {self.person_id_external} - {email}: {e}"
            )
            return None

    def build_perphone_payload(self):
        try:
            payload = get_perphone_payload()
            if not self.phone:
                return None
            parsed_country_code = None
            iso3_code = None
            national_number = None
            try:
                parsed_phone = phonenumbers.parse(self.phone, None)
                parsed_country_code = f"+{parsed_phone.country_code}"
                region_code = phonenumbers.region_code_for_country_code(
                    parsed_phone.country_code
                )
                iso3_code = get_iso3_numeric(region_code)
                national_number = str(parsed_phone.national_number)
            except Exception as e:
                Logger.info(
                    f"No valid phone data for {self.person_id_external} - {self.phone}: {e}"
                )
                return None
            if parsed_country_code and iso3_code:
                payload["personIdExternal"] = self.person_id_external
                payload["phoneType"] = "18258"  # Business phone
                payload["customString1"] = iso3_code
                payload["countryCode"] = parsed_country_code
                payload["phoneNumber"] = national_number
                payload["isPrimary"] = True
                return payload
            else:
                Logger.info(
                    f"No valid phone data for {self.person_id_external} - {self.phone}"
                )
                return None
        except Exception as e:
            Logger.error(
                f"Error building PerPhone payload for {self.person_id_external}: {e}"
            )
            return None
