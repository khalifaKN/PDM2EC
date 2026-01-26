from dataclasses import dataclass
from typing import Any
@dataclass
class FieldChange:
    userid: str
    field_name: str
    ec_value: Any
    pdm_value: Any
    is_scm_user: bool = False
    is_im_user: bool = False

    def to_dict(self):
        return {
            "userid": self.userid,
            "field_name": self.field_name,
            "ec_value": self.ec_value,
            "pdm_value": self.pdm_value,
            "is_scm_user": self.is_scm_user,
            "is_im_user": self.is_im_user,
        }
