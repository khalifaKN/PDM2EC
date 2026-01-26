from utils.logger import get_logger
import time

Logger = get_logger("cache_refresh_token")


class CacheRefreshToken:
    """
    Shared in-memory cache for refresh tokens (class-level).
    Valid for single-process applications.
    """
    _cache = {}

    @classmethod
    def set(cls, key, value, expires_in=None):
        expiry_time = time.time() + expires_in if expires_in else None
        cls._cache[key] = {
            "value": value,
            "expiry": expiry_time
        }
        Logger.info(f"Token cached under key: {key} (expires in {expires_in}s)")

    @classmethod
    def get(cls, key):
        return cls._cache.get(key)

    @classmethod
    def get_value(cls, key):
        entry = cls._cache.get(key)
        return entry["value"] if entry else None

    @classmethod
    def check_token_validity(cls, key):
        entry = cls._cache.get(key)
        if not entry:
            Logger.warning(f"No token found for key: {key}")
            return False

        expiry = entry.get("expiry")
        if expiry and expiry < time.time():
            Logger.warning(f"Token expired for key: {key}")
            cls._cache.pop(key, None)  # cleanup
            return False

        return True

    @classmethod
    def clear_key(cls, key):
        if key in cls._cache:
            cls._cache.pop(key, None)
            Logger.info(f"Cache cleared for key: {key}")
        else:
            Logger.warning(f"Key not found in cache: {key}")

    @classmethod
    def clear(cls):
        cls._cache.clear()
        Logger.info("Token cache cleared.")
