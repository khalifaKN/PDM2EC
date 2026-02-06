import os
import pandas as pd
from threading import Lock
from utils.logger import get_logger

Logger = get_logger("sap_cache")

class SAPDataCache:
    """
    Singleton cache for SAP data.
    Loads parquet files once per process/DAG task and keeps DataFrames in memory.
    """
    _instance = None
    _lock = Lock()
    _data = {}
    _initialized = False
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                # Double-check locking pattern
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        # Only initialize once
        if not SAPDataCache._initialized:
            with SAPDataCache._lock:
                if not SAPDataCache._initialized:
                    Logger.info("Initializing SAPDataCache singleton")
                    self._cache_dir = "./cache/sap_data"
                    SAPDataCache._initialized = True
    
    def get(self, key: str) -> pd.DataFrame:
        """
        Get DataFrame from cache. Load from parquet if not in memory.
        
        Args:
            key: Cache key (e.g., 'positions_df', 'employees_df')
            
        Returns:
            DataFrame or None if not found
        """
        # Check if already loaded in memory
        if key in SAPDataCache._data:
            Logger.debug(f"Cache HIT for {key} (using in-memory data)")
            return SAPDataCache._data[key]
        
        # Load from parquet (thread-safe)
        with SAPDataCache._lock:
            # Double-check after acquiring lock
            if key in SAPDataCache._data:
                return SAPDataCache._data[key]
            
            Logger.info(f"Cache MISS for {key} - loading from parquet")
            df = self._load_from_parquet(key)
            
            if df is not None:
                SAPDataCache._data[key] = df
                Logger.info(f"Loaded {key}: {len(df)} rows, {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
            
            return df
    
    def _load_from_parquet(self, key: str) -> pd.DataFrame:
        """Load DataFrame from parquet file."""
        try:
            file_path = os.path.join(self._cache_dir, f"{key}.parquet")
            
            if not os.path.exists(file_path):
                Logger.warning(f"Parquet file not found: {file_path}")
                return None
            
            df = pd.read_parquet(file_path)
            return df
            
        except Exception as e:
            Logger.error(f"Error loading {key} from parquet: {e}")
            return None
    
    def set(self, key: str, df: pd.DataFrame):
        """
        Save DataFrame to cache (both memory and parquet).
        
        Args:
            key: Cache key
            df: DataFrame to cache
        """
        with SAPDataCache._lock:
            # Save to memory
            SAPDataCache._data[key] = df
            
            # Save to parquet
            try:
                os.makedirs(self._cache_dir, exist_ok=True)
                file_path = os.path.join(self._cache_dir, f"{key}.parquet")
                df.to_parquet(file_path, index=False, compression='snappy')
                Logger.info(f"Saved {key} to cache: {len(df)} rows")
            except Exception as e:
                Logger.error(f"Error saving {key} to parquet: {e}")
    
    def clear(self):
        """Clear in-memory cache (useful for testing or memory management)."""
        with SAPDataCache._lock:
            SAPDataCache._data.clear()
            Logger.info("Cleared in-memory cache")
    
    @classmethod
    def reset_singleton(cls):
        """Reset singleton instance (useful for testing)."""
        with cls._lock:
            cls._instance = None
            cls._data.clear()
            cls._initialized = False
            Logger.info("Reset SAPDataCache singleton")
    # Reset Parquet files (use with caution - this will delete all cached data on disk)
    def clear_parquet_cache(self):
        with SAPDataCache._lock:
            try:
                for filename in os.listdir(self._cache_dir):
                    if filename.endswith(".parquet"):
                        os.remove(os.path.join(self._cache_dir, filename))
                Logger.info("Cleared parquet cache files")
            except Exception as e:
                Logger.error(f"Error clearing parquet cache: {e}")