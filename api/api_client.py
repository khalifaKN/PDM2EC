import requests
from utils.logger import get_logger
from utils.send_except_email import send_error_notification
import time
import json
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
logger = get_logger('api_client')


class APIClient:
    def __init__(self, base_url: str, token = None, max_retries: int = 3):
        self.proxies = {
            "http": "http://127.0.0.1:9000",
            "https": "http://127.0.0.1:9000",
        }
        self.base_url = base_url
        self.token = token
        self.max_retries = max_retries
        self.session = requests.Session()
        if token:
            # Handle both string token and dict with 'access_token' key
            access_token = token if isinstance(token, str) else token.get('access_token')
            self.session.headers.update({'Authorization': f"Bearer {access_token}"})

    def _request_with_retry(self, method: str, url: str, **kwargs):
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(f"{method.upper()} request to {url}, attempt {attempt}/{self.max_retries}")
                response = self.session.request(method, url, **kwargs)      
                if 200 <= response.status_code < 300:
                    if response.status_code == 204: 
                        return None
                    return response.json()
                else:
                    # Log response body for debugging 400 errors
                    try:
                        error_body = response.text
                        payload = kwargs.get('json') or kwargs.get('data')
                        try:
                            pretty_payload = json.dumps(payload, indent=2) if payload else "No payload"
                        except (TypeError, ValueError):
                            pretty_payload = str(payload)

                        logger.error(
                            f"{method.upper()} request to {url} failed with {response.status_code}: "
                            f"{error_body[:500]} \nPayload: {pretty_payload[:500]}"
                        )
                    except Exception as e:
                        logger.warning(f"{method.upper()} request to {url} server error {response.status_code}. Retrying... Error: {e}")
                    
                    # Don't retry 400-level errors (client errors), only 500+ (server errors) , 429 (rate limiting) and 403 (forbidden: it can be token related)
                    if 400 <= response.status_code < 500 and response.status_code not in [429, 403]:
                        raise RuntimeError(f"{method.upper()} request to {url} failed with client error {response.status_code}")
                    
                    
                    time.sleep(2 ** attempt)

            except requests.exceptions.RequestException as e:
                logger.error(f"{method.upper()} request to {url} failed: {e}")
                if attempt == self.max_retries:
                    send_error_notification(f"{method.upper()} request to {url} failed after retries", str(e))
                    raise
                time.sleep(2 ** attempt)
        
        raise RuntimeError(f"{method.upper()} request to {url} failed after {self.max_retries} attempts")

    def get(self, endpoint: str, params: dict = None):
        url = f"{self.base_url}{endpoint}"
        return self._request_with_retry("get", url, params=params, verify=False, proxies=self.proxies)

    def post(self, endpoint: str, data: dict = None, json: dict = None, params: dict = None):
        url = f"{self.base_url}{endpoint}"
        return self._request_with_retry("post", url, data=data, json=json, params=params, verify=False, proxies=self.proxies)
    def fetch_all(self, endpoint: str, params: dict = None) -> list:
        """
        Fetch all pages by following SAP OData __next links.(pagination)
        """
        try:
            all_results = []
            next_url = f"{self.base_url}{endpoint}"

            while next_url:
                data = self._request_with_retry("get", next_url, params=params, verify=False, proxies=self.proxies)

                page_results = data.get("d", {}).get("results", [])
                all_results.extend(page_results)

                # SAP pagination
                next_url = data.get("d", {}).get("__next")
                params = None

            logger.info(f"Fetched total records: {len(all_results)}")
            return all_results
        except Exception as e:
            logger.error(f"Error fetching all pages from {endpoint}: {e}")
            return []