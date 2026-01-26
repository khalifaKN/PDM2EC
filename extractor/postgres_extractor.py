from utils.logger import get_logger
from db.psycopg2_connection import Psycopg2DatabaseConnection

logger = get_logger('postgresdb_extractor')

class PostgresDBExtractor:
    """
    Extractor class for PostgreSQL Database.
    """
    def __init__(self, postgres_url: dict):
        self.postgres_connection = Psycopg2DatabaseConnection(postgres_url)
    def extract_data(self, query: str):
        """
        Extracts data from PostgreSQL Database based on the provided SQL query.

        Args:
            query (str): SQL query to execute.
        Returns:
            tuple: (list of tuples containing the query results, list of column names)
        """ 
        try:
            connection = self.postgres_connection.get_postgres_db_connection()
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            logger.info("Data extracted successfully from PostgreSQL Database.")
            return results, columns
        except Exception as e:
            logger.error(f"Error extracting data from PostgreSQL Database: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()