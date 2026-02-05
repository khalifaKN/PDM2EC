import os
import pandas as pd
from threading import Lock
from utils.logger import get_logger

Logger = get_logger("postgres_cache")

class PostgresDataCache:
    """Singleton cache for Postgres data."""
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
        if not PostgresDataCache._initialized:
            with PostgresDataCache._lock:
                if not PostgresDataCache._initialized:
                    Logger.info("Initializing PostgresDataCache singleton")
                    self._cache_dir = "./cache/postgres_data"
                    PostgresDataCache._initialized = True
    
    def get(self, key: str) -> pd.DataFrame:
        """Get DataFrame from cache. Load from parquet if not in memory."""
        if key in PostgresDataCache._data:
            Logger.debug(f"Cache HIT for {key}")
            return PostgresDataCache._data[key]
        
        with PostgresDataCache._lock:
            if key in PostgresDataCache._data:
                return PostgresDataCache._data[key]
            
            Logger.info(f"Cache MISS for {key} - loading from parquet")
            df = self._load_from_parquet(key)
            
            if df is not None:
                PostgresDataCache._data[key] = df
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
        with PostgresDataCache._lock:
            PostgresDataCache._data[key] = df
            try:
                os.makedirs(self._cache_dir, exist_ok=True)
                file_path = os.path.join(self._cache_dir, f"{key}.parquet")
                df.to_parquet(file_path, index=False, compression='snappy')
                Logger.info(f"Saved {key}: {len(df)} rows")
            except Exception as e:
                Logger.error(f"Error saving {key}: {e}")