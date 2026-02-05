import os
import pandas as pd
from threading import Lock
from utils.logger import get_logger

Logger = get_logger("oracle_cache")

class OracleDataCache:
    """Singleton cache for Oracle data."""
    _instance = None
    _lock = Lock()
    _data = {}
    _initialized = False
        
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if not OracleDataCache._initialized:
            with OracleDataCache._lock:
                if not OracleDataCache._initialized:
                    Logger.info("Initializing OracleDataCache singleton")
                    self._cache_dir = "./cache/oracle_data"
                    OracleDataCache._initialized = True
    
    def get(self, key: str) -> pd.DataFrame:
        """Get DataFrame from cache. Load from parquet if not in memory."""
        if key in OracleDataCache._data:
            Logger.debug(f"Cache HIT for {key}")
            return OracleDataCache._data[key]
        
        with OracleDataCache._lock:
            if key in OracleDataCache._data:
                return OracleDataCache._data[key]
            
            Logger.info(f"Cache MISS for {key} - loading from parquet")
            df = self._load_from_parquet(key)
            
            if df is not None:
                OracleDataCache._data[key] = df
                Logger.info(f"Loaded {key}: {len(df)} rows")
            
            return df
    
    def _load_from_parquet(self, key: str) -> pd.DataFrame:
        """Load DataFrame from parquet file."""
        try:
            file_path = os.path.join(self._cache_dir, f"{key}.parquet")
            if not os.path.exists(file_path):
                Logger.warning(f"Parquet file not found: {file_path}")
                return None
            return pd.read_parquet(file_path)
        except Exception as e:
            Logger.error(f"Error loading {key}: {e}")
            return None
    
    def set(self, key: str, df: pd.DataFrame):
        """Save DataFrame to cache (memory + parquet)."""
        with OracleDataCache._lock:
            OracleDataCache._data[key] = df
            try:
                os.makedirs(self._cache_dir, exist_ok=True)
                file_path = os.path.join(self._cache_dir, f"{key}.parquet")
                df.to_parquet(file_path, index=False, compression='snappy')
                Logger.info(f"Saved {key}: {len(df)} rows")
            except Exception as e:
                Logger.error(f"Error saving {key}: {e}")