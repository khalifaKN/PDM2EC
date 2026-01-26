from utils.logger import get_logger
from db.oracle_connection import OracleDatabaseConnection

logger = get_logger('oracledb_extractor')

class OracleDBExtractor:
    """
    Extractor class for Oracle Database.
    """

    def __init__(self, oracle_dsn: dict):
        self.oracle_connection = OracleDatabaseConnection(oracle_dsn)

    def extract_data(self, query: str):
        """
        Extracts data from Oracle Database based on the provided SQL query.

        Args:
            query (str): SQL query to execute. 

        Returns:
            tuple: (list of tuples containing the query results, list of column names)
        """
        connection = None
        cursor = None
        
        try:
            connection = self.oracle_connection.get_oracle_db_connection()
            cursor = connection.cursor()
            cursor.execute(query)
            results = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            logger.info(f"Data extracted successfully from Oracle Database. Raw fetchall() returned {len(results)} rows.")
            return results, columns
        except Exception as e:
            logger.error(f"Error extracting data from Oracle Database: {e}")
            raise e
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()