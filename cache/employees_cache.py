from utils.logger import get_logger

Logger= get_logger('employees_data_cache')

class EmployeesDataCache:
    """
    A simple in-memory cache for employees data.
    """
    # Class-level cache shared across all instances
    _cache = {}

    def __init__(self):
        pass
    @classmethod
    def get(cls, key):
        """
        Retrieves data from the cache.

        Args:
            key (str): The key to look up in the cache. 
        Returns:
            The cached data if present, else None.
        """
        return cls._cache.get(key, None)
    
    @classmethod
    def set(cls, key, value):
        """
        Stores data in the cache.

        Args:
            key (str): The key under which to store the data.
            value: The data to store in the cache.
        """
        cls._cache[key] = value
        Logger.info(f"Data cached under key: {key}")
    @classmethod
    def clear(cls):
        """ Clears the entire cache. """
        cls._cache.clear()
        Logger.info("Employees data cache cleared.")

    @classmethod
    def clear_key(cls, key):
        """
        Clears a specific key from the cache.

        Args:
            key (str): The key to remove from the cache.
        """
        if key in cls._cache:
            del cls._cache[key]
            Logger.info(f"Cache cleared for key: {key}")
        else:
            Logger.warning(f"Key: {key} not found in cache.")