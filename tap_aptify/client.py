"""SQL client handling.

This includes aptifyStream and aptifyConnector.
"""

from __future__ import annotations
import typing as t
import sqlalchemy
from sqlalchemy.engine import URL
from singer_sdk import SQLConnector, SQLStream

class aptifyConnector(SQLConnector):
    """Connector class for MSSQL (Azure SQL Database) using SQLAlchemy and pyodbc."""

    def get_sqlalchemy_url(self, config: dict) -> str:
        if 'connection_string' in config:
            return config['connection_string']
        driver = config.get('driver', "ODBC Driver 17 for SQL Server")
        host = config['host']
        port = config.get('port', 1433)
        database = config['database']
        user = config['user']
        password = config['password']

        connection_url = URL.create(
            'mssql+pyodbc',
            username=user,
            password=password,
            host=host,
            port=port,
            database=database,
            query={'driver': driver, 'Encrypt': 'yes', 'TrustServerCertificate': 'yes'},
        )
        return str(connection_url)

    def to_jsonschema_type(self, sql_type):
        """Convert SQL type to JSON Schema type.
        Args:
            sql_type: SQLAlchemy SQL type.
        Returns:
            JSON Schema type.
        """
        return super().to_jsonschema_type(sql_type)

    @staticmethod
    def to_sql_type(jsonschema_type):
        """Convert JSON Schema type to SQL type.
        Args:
            jsonschema_type: JSON Schema type.
        Returns:
            SQLAlchemy SQL type.
        """
        return SQLConnector.to_sql_type(jsonschema_type)


class aptifyStream(SQLStream):
    """Stream class for aptify streams."""
    connector_class = aptifyConnector

    def get_records(self, partition: dict | None) -> t.Iterable[dict[str, t.Any]]:
        """Return a generator of record-type dictionary objects.
        This method extracts data from the tables specified in the tap configuration.
        Args:
            partition: If provided, read specifically from this partition (not used here).
        Yields:
            One dict per record.
        """
        # Retrieve the list of tables from the tap configuration.
        tables = self.config.get("tables", [])
        if not tables:
            self.logger.error("No tables specified in the configuration.")
            return

        # Create the SQLAlchemy engine using the connector.
        engine = self.connector.create_engine()
        with engine.connect() as conn:
            for table in tables:
                query = f"SELECT * FROM {table}"
                self.logger.info(f"Extracting records from table: {table}")
                result = conn.execute(query)
                for row in result:
                    record = dict(row._mapping)
                    
                    yield record
