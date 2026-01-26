import oracledb
from utils.logger import get_logger
from utils.send_except_email import send_error_notification
logger_ = get_logger('oracle_db_connection')


class OracleDatabaseConnection():
    """
    Establishes and returns database connections using oracledb for Oracle.
    
    Args:
        oracle_dsn (dict): Database connection parameters
        connect_timeout (int): Connection timeout in seconds (default: 20)
        call_timeout (int): Query execution timeout in milliseconds (default: 60000 = 60s)
    """

    def __init__(self, oracle_dsn: dict, connect_timeout: int = 20, call_timeout: int = 60000):
        self.oracle_dsn = oracle_dsn
        self.connect_timeout = connect_timeout
        self.call_timeout = call_timeout

    def get_oracle_db_connection(self):
        """
        Establishes and returns a connection to the Oracle database using
        credentials stored in Airflow Variables or environment variables.
        Returns:
            oracledb.Connection: A connection object to the Oracle database.
        """
        try: 
            # Set connection timeout
            oracledb.defaults.timeout = self.connect_timeout
            
            connection = oracledb.connect(
                user=self.oracle_dsn['user'],
                password=self.oracle_dsn['password'],
                dsn=f"{self.oracle_dsn['host']}:{self.oracle_dsn['port']}/{self.oracle_dsn['database']}"
            )
            
            if connection:
                # Set query execution timeout (call_timeout in milliseconds)
                connection.call_timeout = self.call_timeout
                logger_.info(f"Oracle connection established (connect_timeout={self.connect_timeout}s, call_timeout={self.call_timeout}ms)")
                return connection
            else:
                logger_.error("Failed to establish Oracle database connection.")
                send_error_notification("Oracle DB Connection Error", "Failed to establish Oracle database connection.")
                raise ConnectionError("Failed to establish Oracle database connection.")
        except Exception as e:
            logger_.error(f"Error connecting to Oracle database: {e}")
            send_error_notification("Oracle DB Connection Error", str(e))
            raise e
