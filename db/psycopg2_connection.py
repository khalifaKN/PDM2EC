import psycopg2
from utils.logger import get_logger
from utils.send_except_email import send_error_notification

logger_ = get_logger('psycopg2_db_connection')


class Psycopg2DatabaseConnection():
    """
    Establishes and returns database connections using psycopg2 for PostgreSQL.
    
    Args:
        postgres_url (dict): Database connection parameters
        connect_timeout (int): Connection timeout in seconds (default: 30)
        statement_timeout (int): Query execution timeout in milliseconds (default: 60000 = 60s)
    """

    def __init__(self, postgres_url: dict, connect_timeout: int = 30, statement_timeout: int = 60000):
        self.postgres_url = postgres_url
        self.connect_timeout = connect_timeout
        self.statement_timeout = statement_timeout

    def get_postgres_db_connection(self):    
        """
        Establishes and returns a connection to the PostgreSQL database using
        credentials stored in Airflow Variables or environment variables.
    
        Returns:
            psycopg2.extensions.connection: A connection object to the PostgreSQL database.
        """
        try:
            # Set a timeout for the connection attempt
            psycopg2.extensions.set_wait_callback(psycopg2.extras.wait_select)

            connection = psycopg2.connect(
                host=self.postgres_url['host'],
                port=self.postgres_url['port'],
                database=self.postgres_url['database'],
                user=self.postgres_url['user'],
                password=self.postgres_url['password'],
                connect_timeout=self.connect_timeout,
                options=f"-c search_path={self.postgres_url['schema']} -c statement_timeout={self.statement_timeout}"
            )
            
            if connection:
                logger_.info(f"PostgreSQL connection established (connect_timeout={self.connect_timeout}s, statement_timeout={self.statement_timeout}ms)")
                return connection
            else:
                logger_.error("Failed to establish PostgreSQL database connection.")
                send_error_notification("PostgreSQL DB Connection Error", "Failed to establish PostgreSQL database connection.")
                raise ConnectionError("Failed to establish PostgreSQL database connection.")
        except Exception as e:
            logger_.error(f"Error connecting to PostgreSQL database: {e}")
            send_error_notification("PostgreSQL DB Connection Error", str(e))
            raise e