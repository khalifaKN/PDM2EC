from utils.logger import get_logger
import pandas as pd

Logger = get_logger("email_validator")
class EmailValidator:
    BUSINESS_TYPE = 18242
    PRIVATE_TYPE = 18240

    def __init__(self, record: pd.Series, email_data: pd.DataFrame, userid: str):
        self.record = record
        self.email_data = email_data if email_data is not None else pd.DataFrame()
        self.userid = userid.strip().lower()
        self.is_private_only = str(record.get("is_private_email", "false")).lower() == "true"

        self.incoming = self._extract_incoming()
        self.existing = self._extract_existing()
        self.primary = self._existing_primary()

    def _extract_incoming(self) -> dict[int, str]:
        """Returns dict {type: email} for incoming emails"""
        incoming = {}
        private = self.record.get("private_email")
        business = self.record.get("email")

        if private:
            incoming[self.PRIVATE_TYPE] = private.lower()
        
        # Only add business email if it's different from private email
        # When email == private_email, it means there's no separate business email
        if not self.is_private_only and business:
            business_lower = business.lower()
            private_lower = private.lower() if private else None
            if business_lower != private_lower:
                incoming[self.BUSINESS_TYPE] = business_lower
        return incoming

    def _extract_existing(self) -> list[dict]:
        """Returns list of dicts: {'email', 'type', 'is_primary'}"""
        if self.email_data.empty:
            return []
        
        # Handle both 'userid' and 'personidexternal' column names
        id_col = 'personidexternal' if 'personidexternal' in self.email_data.columns else 'userid'
        df = self.email_data[self.email_data[id_col].str.lower() == self.userid]
        return [
            {
                "email": row["emailaddress"].lower(),
                "type": int(row["emailtype"]),
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

        for typ, email in self.incoming.items():
            # Check existing by email
            existing_email = next((e for e in self.existing if e["email"] == email), None)
            # Check existing by type
            existing_type = next((e for e in self.existing if e["type"] == typ), None)

            # If email exists but wrong type → update type
            if existing_email and existing_email["type"] != typ:
                actions["update_type"].append({"email": email, "old_type": existing_email["type"], "new_type": typ})

            # If type exists but different email → delete old
            if existing_type and existing_type["email"] != email:
                actions["delete"].append({"email": existing_type["email"], "type": existing_type["type"]})

            # If not exists with correct type → insert
            if not existing_email or existing_email["type"] != typ:
                actions["insert"].append({"email": email, "type": typ})

            # Determine primary
            if typ == self.BUSINESS_TYPE:
                # Promote business email to primary
                if not existing_email or not existing_email.get("is_primary", False):
                    actions["primary"]["promote"] = email
                    if self.primary and self.primary["email"] != email:
                        actions["primary"]["demote"] = self.primary["email"]
            elif typ == self.PRIVATE_TYPE:
                # Promote private only if no business primary exists
                if not any(e["type"] == self.BUSINESS_TYPE for e in self.existing):
                    if not existing_email or not existing_email.get("is_primary", False):
                        actions["primary"]["promote"] = email
                        if self.primary and self.primary["email"] != email:
                            actions["primary"]["demote"] = self.primary["email"]

        return actions
