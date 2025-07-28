import psycopg2
from psycopg2 import Error
from psycopg2.extensions import connection as PgConnection
from typing import Optional, Dict, Any, List


from common.config import DB_HOST, DB_NAME, DB_USER, DB_PASSWORD, setup_logging
from common.exceptions import RabbitMQConnectionError

class DBClient:
    def __init__(self, service_name: str, db_host: str = None, db_name: str = None, db_user: str = None, db_password: str = None):
        self.service_name = service_name
        self.logger = setup_logging(f"{service_name}.DBClient") # Get логгер
        self._conn: Optional[PgConnection] = None
        self.db_host = db_host if db_host else DB_HOST
        self.db_name = db_name if db_name else DB_NAME
        self.db_user = db_user if db_user else DB_USER
        self.db_password = db_password if db_password else DB_PASSWORD

        if not all([self.db_host, self.db_name, self.db_user, self.db_password]):
            raise ValueError(f"[{self.service_name}] Database connection parameters are incomplete. Check ENV variables.")

        self._connect()

    def _connect(self):
        """Establishes a connection to PostgreSQL."""
        try:
            self._conn = psycopg2.connect(
                host=self.db_host,
                database=self.db_name,
                user=self.db_user,
                password=self.db_password
            )
            self._conn.autocommit = True
            self.logger.info(f"Connected to PostgreSQL: {self.db_name}@{self.db_host}")
        except Error as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise RabbitMQConnectionError(f"[{self.service_name}] Failed to connect to PostgreSQL: {e}")
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred during DB connection.")
            raise

    def _ensure_connection(self):
        """Checks and reconnects if the connection is lost."""
        if self._conn is None or self._conn.closed:
            self.logger.warning("Reconnecting to PostgreSQL...")
            self._connect()

    def execute_query(self, query: str, params: Optional[tuple] = None, fetch_one: bool = False, fetch_all: bool = False) -> Optional[List[Dict[str, Any]]]:
        """SQL query"""
        self._ensure_connection()
        try:
            with self._conn.cursor() as cur:
                cur.execute(query, params)
                if fetch_one:
                    row = cur.fetchone()
                    return [dict(zip([col[0] for col in cur.description], row))] if row else None
                elif fetch_all:
                    rows = cur.fetchall()
                    return [dict(zip([col[0] for col in cur.description], row)) for row in rows] if rows else None
                return None
        except Error as e:
            self.logger.error(f"Database query error: {e}. Query: {query}, Params: {params}")
            self._conn.rollback()
            raise
        except Exception as e:
            self.logger.exception(f"An unexpected error occurred during query execution.")
            raise

    def create_table_if_not_exists(self, table_name: str, schema: str):
        """СCreates table is doesnt exist."""
        query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {schema}
        );
        """
        try:
            self.execute_query(query)
            self.logger.info(f"Table '{table_name}' ensured.")
        except Exception as e:
            self.logger.exception(f"Error creating table '{table_name}'.")
            raise

    def __del__(self):
        if self._conn:
            self._conn.close()
            self.logger.info("Closed PostgreSQL connection.")