import requests
from utils.logger import get_logger
from cache.token_cache import CacheRefreshToken
from utils.send_except_email import send_error_notification
import time

logger = get_logger("auth_api")


class AuthAPI:
    def __init__(
        self, auth_url, client_id, client_secret, company_id, grant_type, max_retries=3
    ):
        self.proxies = {
            "http": "http://127.0.0.1:9000",
            "https": "http://127.0.0.1:9000",
        }
        self.auth_url = auth_url
        self.client_id = client_id
        self.assertion = client_secret
        self.grant_type = grant_type
        self.company_id = company_id
        self.token_cache = CacheRefreshToken()
        self.max_retries = max_retries

    def get_token(self):
        if self.token_cache.check_token_validity("access_token"):
            return self.token_cache.get_value("access_token")

        payload = {
            "client_id": self.client_id,
            "assertion": self.assertion,
            "grant_type": self.grant_type,
            "company_id": self.company_id,
        }

        for attempt in range(1, self.max_retries + 1):
            try:
                logger.info(
                    f"Requesting new token from {self.auth_url} (Attempt {attempt}/{self.max_retries})"
                )
                logger.info(f"Auth URL: {self.auth_url}")
                logger.info(f"Auth payload: {payload}")
                response = requests.post(
                    self.auth_url, data=payload, verify=False, timeout=10, proxies=self.proxies
                )

                if 200 <= response.status_code < 300:
                    token_data = response.json()
                    access_token = token_data.get("access_token")
                    expires_in = token_data.get("expires_in", 3600)
                    if access_token:
                        self.token_cache.set("access_token", access_token, expires_in)
                        logger.info(f"New token obtained and cached: {access_token}")
                        return access_token
                    else:
                        logger.error(f"Token not found in response: {token_data}")
                        raise ValueError("No access_token in auth response")
                else:
                    logger.warning(f"Server error {response.status_code}, retrying...")
                    time.sleep(2**attempt)

            except requests.exceptions.RequestException as e:
                logger.error(f"RequestException on attempt {attempt}: {e}")
                if attempt == self.max_retries:
                    send_error_notification(
                        f"Authentication failed after {self.max_retries} attempts",
                        f"RequestException: {str(e)}"
                    )
                    raise
                time.sleep(2**attempt)

        send_error_notification(
            f"Failed to obtain authentication token after {self.max_retries} attempts",
            "Authentication failure - all retry attempts exhausted"
        )
        raise RuntimeError(f"Failed to obtain token after {self.max_retries} attempts")
