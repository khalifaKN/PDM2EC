from utils.logger import get_logger
from api.api_client import APIClient

import time
import json


logger = get_logger("upsert_client")


class UpsertClient:
    MAX_CHUNK_SIZE = 800  # It has to be less than 1000 to be safe with SAP limits

    def __init__(self, api_client: APIClient, max_retries: int = 5):
        self.api_client = api_client
        self.max_retries = max_retries

    def upsert_entity_for_users(self, entity_name: str, user_payloads: dict):
        """
        user_payloads = { user_id: payload }
        Handles batching for >1000 records safely.
        """
        results = {}

        # Flatten user_payloads to lists
        payload_list = []
        user_index = []

        for user_id, payload in user_payloads.items():
            # normalize dict -> list
            if isinstance(payload, dict):
                payload = [payload]
            for p in payload:
                payload_list.append(p)
                user_index.append(user_id)

        # Split into chunks to respect MAX_CHUNK_SIZE
        for chunk_start in range(0, len(payload_list), self.MAX_CHUNK_SIZE):
            chunk_payloads = payload_list[chunk_start:chunk_start + self.MAX_CHUNK_SIZE]
            chunk_user_index = user_index[chunk_start:chunk_start + self.MAX_CHUNK_SIZE]

            # Retry logic
            for attempt in range(1, self.max_retries + 1):
                try:
                    # Use SAP SuccessFactors upsert endpoint with array payload
                    response = self.api_client.post(
                        "/odata/v2/upsert?$format=json",
                        json=chunk_payloads
                    )
                    # Check if all records have client errors (400-499) - if so, No retry
                    records = response.get("d", [])
                    if records:
                        http_codes = [r.get("httpCode", 200) for r in records]
                        all_client_errors = all(400 <= code < 500 for code in http_codes if code)
                        #logging each payload with its response
                        for r in records:
                            idx = r.get("index")
                            if idx is None or idx >= len(chunk_payloads):
                                pretty_payload = "Unknown payload"
                            else:
                                payload_item = chunk_payloads[idx]
                                try:
                                    pretty_payload = json.dumps(payload_item, indent=2)
                                except (TypeError, ValueError):
                                    pretty_payload = str(payload_item)

                            # Log payload
                            logger.info(f"Upsert payload for {entity_name}:\n{pretty_payload}")

                            # Log response
                            logger.info(
                                f"Upsert response for {entity_name}: "
                                f"Status: {r.get('status')}, "
                                f"Message: {r.get('message')}, "
                                f"Key: {r.get('key')}, "
                                f"HttpCode: {r.get('httpCode')}"
                            )
                        if all_client_errors:
                            logger.info(f"{entity_name} chunk - all records have client errors (400-499), not retrying")
                            break
                    
                    break
                except RuntimeError as e:
                    # No retry on client errors (400-499)
                    if "client error" in str(e):
                        logger.error(f"{entity_name} chunk failed with client error: {e}")
                        for user_id in set(chunk_user_index):
                            results[user_id] = {
                                "entity": entity_name,
                                "status": "FAILED",
                                "message": str(e)
                            }
                        break  # No retry
                    logger.warning(f"{entity_name} chunk failed attempt {attempt}: {e}")
                    if attempt < self.max_retries:
                        time.sleep(2 ** attempt)
                except Exception as e:
                    logger.warning(f"{entity_name} chunk failed attempt {attempt}: {e}")
                    if attempt < self.max_retries:
                        time.sleep(2 ** attempt)
            else:
                # Max retries exceeded for this chunk
                for user_id in set(chunk_user_index):
                    results[user_id] = {
                        "entity": entity_name,
                        "status": "FAILED",
                        "message": "Max retries exceeded"
                    }
                continue  # Next chunk

            # Map response to users - SAP returns status per record
            records = response.get("d", [])
            for r in records:
                idx = r.get("index")
                if idx is None or idx >= len(chunk_user_index):
                    continue  # safety
                user_id = chunk_user_index[idx]

                # Check status from SAP response
                sap_status = r.get("status", "").upper()
                if sap_status == "OK":
                    status = "SUCCESS"
                elif sap_status == "ERROR":
                    status = "FAILED"
                elif "Warning" in r.get("message", ""):
                    status = "WARNING"
                else:
                    status = "SUCCESS"  # default if no error

                results[user_id] = {
                    "entity": entity_name,
                    "status": status,
                    "message": r.get("message"),
                    "key": r.get("key"),
                    "httpCode": r.get("httpCode")
                }

        return results
    
    def upsert_entity(self, entity_name: str, payload: dict, parameters: dict = None):
        """
        Upsert a single entity record.
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.api_client.post(
                    "/odata/v2/upsert?$format=json",
                    json=payload,
                    params=parameters if parameters else None

                )
                # Log payload
                pretty_payload = json.dumps(payload, indent=2)
                logger.info(f"Upsert payload for {entity_name}:\n{pretty_payload}")

                # The response has starts with 'd' key containing list of results
                records = response.get("d", [])
                if records:
                    response_ = records[0]  # Single record upsert
                # Log response
                logger.info(
                    f"Upsert response for {entity_name}: "
                    f"Status: {response_.get('status')}, "
                    f"Message: {response_.get('message')}, "
                    f"Key: {response_.get('key')}, "
                    f"HttpCode: {response_.get('httpCode')}"
                )

                return response_
            except RuntimeError as e:
                if "client error" in str(e):
                    logger.error(f"{entity_name} upsert failed with client error: {e}")
                    raise
                logger.warning(f"{entity_name} upsert failed attempt {attempt}: {e}")
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)
            except Exception as e:
                logger.warning(f"{entity_name} upsert failed attempt {attempt}: {e}")
                if attempt < self.max_retries:
                    time.sleep(2 ** attempt)
        else:
            raise RuntimeError(f"Max retries exceeded for upserting {entity_name}")
