from sqlalchemy import create_engine 
from utils.logger import get_logger
from utils.format_db_url import format_postgres_url, format_oracle_dsn
logger = get_logger('db_connection')

"""
Database connection utilities for PostgreSQL and Oracle databases. 
Provides functions to establish connections using credentials from Airflow Variables or environment variables.
"""
class SQLAlchemyDatabaseConnection():
    
    def __init__(self, postgres_url: dict, oracle_dsn: dict):
        """
        Initializes the DatabaseConnection instance with formatted PostgreSQL and Oracle DSN strings.
        
        Args:
            postgres_url (dict): Dictionary containing PostgreSQL connection parameters.
            oracle_dsn (dict): Dictionary containing Oracle connection parameters.
        """
        self.postgres_url = format_postgres_url(**postgres_url)
        self.oracle_dsn = format_oracle_dsn(**oracle_dsn)
    

    def _create_postgres_engine(self):
        """
        Creates and returns a SQLAlchemy engine for PostgreSQL database.

        Returns:
            sqlalchemy.Engine: SQLAlchemy engine instance for PostgreSQL.
        """
        try:
            engine = create_engine(self.postgres_url)
            logger.info("PostgreSQL engine created successfully.")
            return engine
        except Exception as e:
            logger.error(f"Error creating PostgreSQL engine: {e}")
            raise e

    def _create_oracle_engine(self):
        """
        Creates and returns a SQLAlchemy engine for Oracle database.
        Returns:
            sqlalchemy.Engine: SQLAlchemy engine instance for Oracle.
        """
        try:
            engine = create_engine(self.oracle_dsn)
            logger.info("Oracle engine created successfully.")
            return engine
        except Exception as e:
            logger.error(f"Error creating Oracle engine: {e}")
            raise e

    def get_postgres_db_connection(self):    
        """
        Establishes and returns a connection to the PostgreSQL database using
        credentials stored in Airflow Variables or environment variables.
    
        Returns:
            psycopg2.extensions.connection: A connection object to the PostgreSQL database.
        """
        try:
            engine = self._create_postgres_engine()
            connection = engine.connect()
            logger.info("PostgreSQL database connection established successfully.")
            return connection
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL database: {e}")
            raise e
            
        
    def get_oracle_db_connection(self):    
        """
        Establishes and returns a connection to the Oracle database using
        credentials stored in Airflow Variables or environment variables.
    
        Returns:
            oracledb.Connection: A connection object to the Oracle database.
        """
        try:
            engine = self._create_oracle_engine()
            connection = engine.connect()
            logger.info("Oracle database connection established successfully.")
            return connection
        except Exception as e:
            logger.error(f"Error connecting to Oracle database: {e}")
            raise e