# api/batch_client.py
import requests
import uuid
import re
import time
from utils.logger import get_logger

logger = get_logger("batch_client")


class SAPBatchClient:
    def __init__(self, base_url: str, token: str, max_retries: int = 3):
        """
        Initializes the SAPBatchClient.

        Args:
            base_url (str): Base URL for the OData service (without /$batch)
            token (str): Bearer token for authorization
            max_retries (int): Maximum retries for network/server errors
        """
        self.proxies = {"http": "http://127.0.0.1:9000", "https": "http://127.0.0.1:9000"}
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Authorization": f"Bearer {token}"})
        self.max_retries = max_retries

    @staticmethod
    def _generate_boundaries():
        return f"batch_{uuid.uuid4()}", f"changeset_{uuid.uuid4()}"

    def _build_batch_body(
        self, changesets: list, batch_boundary: str, changeset_boundary: str
    ):
        batch_body = f"--{batch_boundary}\nContent-Type: multipart/mixed; boundary={changeset_boundary}\n\n"
        for payload in changesets:
            batch_body += f"--{changeset_boundary}\n"
            batch_body += "Content-Type: application/http\n"
            batch_body += "Content-Transfer-Encoding: binary\n\n"
            batch_body += "POST upsert HTTP/1.1\n"
            batch_body += "Content-Type: application/json\n\n"
            batch_body += f"{payload}\n\n"
        batch_body += f"--{changeset_boundary}--\n"
        batch_body += f"--{batch_boundary}--"
        return batch_body

    def _parse_batch_response(self, response_text: str, default_batch_boundary: str):
        results = []

        batch_boundary_matches = re.findall(r"--batch_[\w-]+", response_text)
        if not batch_boundary_matches:
            batch_boundary_matches = [default_batch_boundary]
        response_boundary = batch_boundary_matches[0]

        parts = response_text.split(response_boundary)
        changeset_index = 0
        for part in parts:
            if "HTTP/1.1" in part:
                http_responses = re.findall(
                    r"HTTP/1.1 (\d+) .*?\n\n(.*?)(?=(--|\Z))", part, re.DOTALL
                )
                request_index = 0
                for status, message, _ in http_responses:
                    results.append(
                        {
                            "changeset_index": changeset_index,
                            "request_index": request_index,
                            "status": int(status),
                            "message": message.strip(),
                        }
                    )
                    request_index += 1
                changeset_index += 1
        return results

    def send_batch(self, changesets: list) -> list:
        """
        Send a batch request. Handles retries on server/network errors and failed changesets.
        """
        attempt = 0
        pending_changesets = changesets.copy()
        final_results = []

        while pending_changesets and attempt < self.max_retries:
            batch_boundary, changeset_boundary = self._generate_boundaries()
            batch_body = self._build_batch_body(
                pending_changesets, batch_boundary, changeset_boundary
            )
            headers = {"Content-Type": f"multipart/mixed; boundary={batch_boundary}"}
            url = f"{self.base_url}/$batch"

            try:
                logger.info(
                    f"Sending batch request to {url} with {len(pending_changesets)} changesets (Attempt {attempt + 1})"
                )
                response = self.session.post(url, headers=headers, data=batch_body, verify=False, proxies=self.proxies)
                response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"Batch request failed on attempt {attempt + 1}: {e}")
                attempt += 1
                time.sleep(2**attempt)
                continue

            batch_results = self._parse_batch_response(response.text, batch_boundary)
            final_results.extend(batch_results)

            # Prepare for retry: only keep changesets that failed with 5xx
            pending_changesets = [
                pending_changesets[r["request_index"]]
                for r in batch_results
                if r["status"] >= 500
            ]

            if pending_changesets:
                logger.warning(
                    f"{len(pending_changesets)} changesets failed with server errors, retrying..."
                )
                attempt += 1
                time.sleep(2**attempt)
            else:
                break

        if pending_changesets:
            logger.error(f"Some changesets failed after {self.max_retries} attempts")

        logger.info(f"Batch completed with {len(final_results)} responses")
        return final_results