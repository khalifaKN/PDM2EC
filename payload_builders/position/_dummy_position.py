
from mapper.country_mapper import get_iso3_numeric
from payload_builders.position.payloads.position import get_position_payload
from config.wh_per_country import wh_per_country

class DummyPositionPayloadBuilder:
    def __init__(self, dummy_pos_values: dict):
        self.dummy_pos_values = dummy_pos_values

    @staticmethod
    def _clean(value):
        if value is None:
            return ""
        return str(value).replace(".0", "")

    @staticmethod
    def _scalar(value):
        """
        Accept both scalars and pandas objects (Series/DataFrame cell).
        If it has `.values`, return the first element, else return as-is.
        """
        try:
            # pandas Series/ndarray-like
            if hasattr(value, "values"):
                return value.values[0]
        except Exception:
            pass
        return value

    def _build_base_dummy_position_payload(self, payload: dict) -> dict:
        """
        Build the minimal required fields for a dummy position.
        Assumes self.dummy_pos_values contains SCALARS (or pandas-compatible).
        """

        company = self._scalar(self.dummy_pos_values.get("company"))
        cost_center = self._scalar(self.dummy_pos_values.get("cost_center"))
        country_iso3 = self._scalar(self.dummy_pos_values.get("country_iso3"))
        country_code = self._scalar(self.dummy_pos_values.get("country_code"))
        bufu_id = self._scalar(self.dummy_pos_values.get("bufu_id"))
        jobcode = self._scalar(self.dummy_pos_values.get("jobcode"))
        address_code = self._scalar(self.dummy_pos_values.get("address_code"))
        cust_geographicalscope = self._scalar(self.dummy_pos_values.get("cust_geographicalscope"))
        cust_subunit = self._scalar(self.dummy_pos_values.get("cust_subunit"))

        # Country of registration: prefer provided iso3 numeric, fall back to mapper
        country_of_reg = country_iso3 or get_iso3_numeric(country_code)

        # Standard hours: from table, default 40
        standard_hours = wh_per_country.get(country_code, 40)

        fields = {
            "company": self._clean(company),
            "costCenter": self._clean(cost_center),
            "cust_Country_Of_Registration": self._clean(country_of_reg),
            "division": self._clean(bufu_id),  # KN naming: bufu_id → division
            "jobCode": self._clean(jobcode),
            "location": self._clean(address_code),
            "cust_geographicalScope": self._clean(cust_geographicalscope),
            "cust_subUnit": self._clean(cust_subunit),
            "standardHours": self._clean(standard_hours),
        }

        # Keep only non-empty fields
        payload.update({k: v for k, v in fields.items() if v != ""})
        return payload

    def build_dummy_position_payload(self) -> dict:
        """
        Build a dummy position payload using the values passed at construction.
        """
        try:
            payload = get_position_payload()  # base skeleton (effectiveStartDate, code placeholder, etc.)
            payload = self._build_base_dummy_position_payload(payload)
            return payload
        except Exception as e:
            raise ValueError(f"Error building dummy position payload: {e}")
