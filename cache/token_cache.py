import os
import json
import time
from utils.logger import get_logger

Logger = get_logger("cache_refresh_token")

CACHE_FILE = "/tmp/pdm_cache/token_cache.json"
os.makedirs("/tmp/pdm_cache", exist_ok=True)


class CacheRefreshToken:

    @staticmethod
    def set(key, value, expires_in=None):
        expiry_ts = time.time() + expires_in if expires_in else None
        data = {
            key: {
                "value": value,
                "expiry": expiry_ts
            }
        }

        # Load existing cache if file exists
        if os.path.exists(CACHE_FILE):
            with open(CACHE_FILE, "r") as f:
                existing = json.load(f)
        else:
            existing = {}

        existing.update(data)

        with open(CACHE_FILE, "w") as f:
            json.dump(existing, f)

        Logger.info(f"[TOKEN CACHE] Stored token under key: {key}")

    @staticmethod
    def get(key):
        if not os.path.exists(CACHE_FILE):
            Logger.warning("[TOKEN CACHE] No cache file found.")
            return None

        with open(CACHE_FILE, "r") as f:
            data = json.load(f)

        return data.get(key)

    @staticmethod
    def get_value(key):
        entry = CacheRefreshToken.get(key)
        return entry["value"] if entry else None

    @staticmethod
    def check_token_validity(key):
        entry = CacheRefreshToken.get(key)
        if not entry:
            Logger.warning(f"[TOKEN CACHE] No token for key={key}")
            return False

        expiry = entry.get("expiry")
        if expiry and expiry < time.time():
            Logger.warning(f"[TOKEN CACHE] Token expired for key={key}")
            CacheRefreshToken.clear_key(key)
            return False

        return True

    @staticmethod
    def clear_key(key):
        if not os.path.exists(CACHE_FILE):
            return
        with open(CACHE_FILE, "r") as f:
            data = json.load(f)

        if key in data:
            del data[key]
            with open(CACHE_FILE, "w") as f:
                json.dump(data, f)
            Logger.info(f"[TOKEN CACHE] Cleared token for key={key}")

    @staticmethod
    def clear():
        if os.path.exists(CACHE_FILE):
            os.remove(CACHE_FILE)
            Logger.info("[TOKEN CACHE] Cleared token cache file.")