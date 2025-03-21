"""SQL client handling.

This includes aptifyStream and aptifyConnector.
"""

from __future__ import annotations
import typing as t
import sqlalchemy
from sqlalchemy.engine import URL, Engine
from singer_sdk import SQLConnector, SQLStream

class aptifyConnector(SQLConnector):
    """Connector class for MSSQL (Azure SQL Database) using SQLAlchemy and pyodbc."""

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Return the SQLAlchemy URL string based on the provided configuration.

        Args:
            config: A dictionary containing connection settings.

        Returns:
            A string representing the SQLAlchemy URL.
        """
        # If a full connection string is provided, use it directly
        if 'connection_string' in config:
            return config['connection_string']

        # Get dialect and driver type, with defaults suitable for MSSQL
        dialect = config.get('dialect', 'mssql')
        driver_type = config.get('driver_type', 'pyodbc')
        drivername = f"{dialect}+{driver_type}"

        # Required configuration parameters
        host = config['host']
        database = config['database']
        user = config['user']
        password = config['password']
        port = int(config['port'])  # Port is optional

        # Create the base URL
        connection_url = URL.create(
            drivername,
            username=user,
            password=password,
            host=host,
            database=database,
            port=port,
        )

        # Set default query parameters for Azure SQL Database
        driver = config.get('driver', "ODBC Driver 17 for SQL Server")
        query = {'driver': driver, 'Encrypt': 'yes', 'TrustServerCertificate': 'yes'}

        # Allow additional query parameters from config to override or extend defaults
        if 'sqlalchemy_url_query' in config:
            query.update(config['sqlalchemy_url_query'])

        # Update the URL with query parameters
        connection_url = connection_url.update_query_dict(query)
        return str(connection_url)

    def create_engine(self) -> Engine:
        """Create a SQLAlchemy engine with optional additional parameters.

        Returns:
            A SQLAlchemy Engine object.
        """
        eng_prefix = "ep."
        eng_config = {
            f"{eng_prefix}url": self.sqlalchemy_url,
            f"{eng_prefix}echo": "False"  # Disable query logging by default
        }

        # Add any additional engine parameters from config
        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config[f"{eng_prefix}{key}"] = value

        return sqlalchemy.engine_from_config(eng_config, prefix=eng_prefix)

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

        This method retrieves records from a single table, using only the selected columns
        based on the stream's schema.

        Args:
            partition: If provided, read from this partition (not used here).

        Yields:
            One dict per record.
        """
        # Get the list of selected column names from the schema
        selected_column_names = list(self.get_selected_schema()["properties"].keys())

        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )

        
        query = table.select()
        with self.connector._connect() as conn:
            result = conn.execute(query)
            for row in result:
                record = dict(row._mapping)
                yield record