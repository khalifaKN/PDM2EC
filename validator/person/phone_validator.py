from utils.logger import get_logger
import pandas as pd

Logger = get_logger("phone_validator")
class PhoneValidator:
    BUSINESS_TYPE = 18258
    PRIVATE_TYPE = 18257

    def __init__(self, record: pd.Series, email_data: pd.DataFrame, userid: str):
        self.record = record
        self.email_data = email_data if email_data is not None else pd.DataFrame()
        self.userid = userid.strip().lower()
        self.is_private_only = str(record.get("is_private_phone", "false")).lower() == "true"

        self.incoming = self._extract_incoming()
        self.existing = self._extract_existing()
        self.primary = self._existing_primary()

    def _extract_incoming(self) -> dict[int, str]:
        """Returns dict {type: phone} for incoming phones"""
        incoming = {}
        business = self.record.get("biz_phone")
        private = self.record.get("biz_mobile")

        if private:
            incoming[self.PRIVATE_TYPE] = private.lower()
        
        # Only add business phone if it's different from private phone
        # When phone == private_phone, it means there's no separate business phone
        if not self.is_private_only and business:
            business_lower = business.lower()
            private_lower = private.lower() if private else None
            if business_lower != private_lower:
                incoming[self.BUSINESS_TYPE] = business_lower
        return incoming

    def _extract_existing(self) -> list[dict]:
        """Returns list of dicts: {'phone', 'type', 'is_primary'}"""
        if self.email_data.empty:
            return []
        
        # Handle both 'userid' and 'personidexternal' column names
        id_col = 'personidexternal' if 'personidexternal' in self.email_data.columns else 'userid'
        df = self.email_data[self.email_data[id_col].str.lower() == self.userid]
        return [
            {
                "phone": row["phoneaddress"].lower(),
                "type": int(row["phonetype"]),
                "is_primary": str(row.get("isprimary", "false")).lower() == "true",
            }
            for _, row in df.iterrows()
        ]

    def _existing_primary(self):
        for e in self.existing:
            if e["is_primary"]:
                return e
        return None

    def decide(self) -> dict:
        """Return structured actions: insert, delete, update_type, primary"""
        actions = {
            "insert": [],
            "delete": [],
            "update_type": [],
            "primary": {"promote": None, "demote": None},
        }

        for typ, phone in self.incoming.items():
            # Check existing by phone
            existing_phone = next((e for e in self.existing if e["phone"] == phone), None)
            # Check existing by type
            existing_type = next((e for e in self.existing if e["type"] == typ), None)

            # If phone exists but wrong type → update type
            if existing_phone and existing_phone["type"] != typ:
                actions["update_type"].append({"phone": phone, "old_type": existing_phone["type"], "new_type": typ})

            # If type exists but different phone → delete old
            if existing_type and existing_type["phone"] != phone:
                actions["delete"].append({"phone": existing_type["phone"], "type": existing_type["type"]})

            # If not exists with correct type → insert
            if not existing_phone or existing_phone["type"] != typ:
                actions["insert"].append({"phone": phone, "type": typ})

            # Determine primary
            if typ == self.BUSINESS_TYPE:
                # Promote business phone to primary
                if not existing_phone or not existing_phone.get("is_primary", False):
                    actions["primary"]["promote"] = phone
                    if self.primary and self.primary["phone"] != phone:
                        actions["primary"]["demote"] = self.primary["phone"]
            elif typ == self.PRIVATE_TYPE:
                # Promote private only if no business primary exists
                if not any(e["type"] == self.BUSINESS_TYPE for e in self.existing):
                    if not existing_phone or not existing_phone.get("is_primary", False):
                        actions["primary"]["promote"] = phone
                        if self.primary and self.primary["phone"] != phone:
                            actions["primary"]["demote"] = self.primary["phone"]

        return actions
