from singer_sdk import SQLConnector, SQLTap, SQLStream
from singer_sdk.typing import PropertiesList, Property, StringType, BooleanType, ObjectType, DateTimeType
from .client import aptifyConnector, aptifyStream

class Tapaptify(SQLTap):
    """Singer tap for extracting data from an MSSQL (Azure SQL) database."""

    name = "tap-aptify"
    default_stream_class = aptifyStream
    default_connector_class = aptifyConnector
    _tap_connector = None  # Instance variable to store the connector

    @property
    def tap_connector(self) -> SQLConnector:
        """The connector object.

        Returns:
            The connector object.
        """
        if self._tap_connector is None:
            self._tap_connector = self.default_connector_class(dict(self.config))
        return self._tap_connector
    
    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary.

        Returns:
            The tap's catalog as a dict
        """
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        connector = self.tap_connector

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(connector.discover_catalog_entries())

        self._catalog_dict = result
        return self._catalog_dict

    config_jsonschema = PropertiesList(
        Property("dialect", StringType, default="mssql", description="The Dialect of SQLAlchemy", required=True),
        Property("driver_type", StringType, default="pyodbc", description="The Python Driver to use for SQL connection", required=True),
        Property("host", StringType, description="FQDN of the SQL server", required=True),
        Property("port", StringType, description="Port for SQL connection"),  # Changed to StringType
        Property("database", StringType, description="Database name", required=True),
        Property("user", StringType, description="User with SQL access", required=True),
        Property("password", StringType, secret=True, description="Password for the user", required=True),
        Property("sqlalchemy_eng_params", ObjectType(
            Property("fast_executemany", StringType, description="Fast Executemany Mode: True, False"),
            Property("future", StringType, description="Run the engine in 2.0 mode: True, False")
        ), description="SQLAlchemy Engine Parameters: fast_executemany, future"),
        Property("sqlalchemy_url_query", ObjectType(
            Property("driver", StringType, description="The Driver to use when connecting"),
            Property("TrustServerCertificate", StringType, description="This is a Yes No option")
        ), description="SQLAlchemy URL Query options: driver, TrustServerCertificate"),
        Property("batch_config", ObjectType(
            Property("encoding", ObjectType(
                Property("format", StringType, description="Currently the only format is jsonl"),
                Property("compression", StringType, description="Currently the only compression option is gzip")
            )),
            Property("storage", ObjectType(
                Property("root", StringType, description="Directory for batch messages, e.g., file://test/batches"),
                Property("prefix", StringType, description="Prefix for batch messages, e.g., test-batch-")
            ))
        ), description="Optional Batch Message configuration"),
        Property("start_date", DateTimeType, description="Earliest record date for incremental sync"),
        Property("hd_jsonschema_types", BooleanType, default=False, description="Turn on Higher Defined(HD) JSON Schema types to assist Targets")
    ).to_dict()

    def discover_streams(self) -> list[SQLStream]:
        """Initialize all available streams and return them as a list.

        Returns:
            List of discovered Stream objects.
        """
        result: list[SQLStream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            result.append(
                self.default_stream_class(
                    tap=self,
                    catalog_entry=catalog_entry,
                    connector=self.tap_connector
                )
            )

        return result

if __name__ == "__main__":
    Tapaptify.cli()