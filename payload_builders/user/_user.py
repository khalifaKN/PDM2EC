from payload_builders.user.payloads.inactive_user import get_user_inactive_payload
from payload_builders.user.payloads.user_role import get_user_role_payload
from utils.logger import get_logger

Logger = get_logger("build_user_payloads")

def build_user_inactive_payload(user_id: str):
    try:
        payload = get_user_inactive_payload()
        payload["userId"] = user_id
        payload["status"] = "inactive"
        return payload
    except Exception as e:
        Logger.error(f"Error building inactive user payload for user_id {user_id}: {e}")
        raise e

def build_user_role_payload(user_id: str, role_code: str):
    try:
        payload = get_user_role_payload()
        payload["userId"] = user_id
        payload["custom08"] = role_code
        return payload
    except Exception as e:
        Logger.error(f"Error building user role payload for user_id {user_id}, role_code {role_code}: {e}")
        raise e