from singer_sdk import SQLTap, SQLStream
from singer_sdk.typing import PropertiesList, Property, StringType, IntegerType, DateTimeType
from .client import aptifyConnector, aptifyStream

class Tapaptify(SQLTap):
    """Singer tap for extracting data from an MSSQL (Azure SQL) database."""

    name = "tap-aptify"
    default_stream_class = aptifyStream
    _tap_connector = None  # Instance variable to store the connector

    @property
    def tap_connector(self) -> aptifyConnector:
        """Initialize and return the connector object.

        Returns:
            The aptifyConnector instance configured with the tap's settings.
        """
        if self._tap_connector is None:
            self._tap_connector = aptifyConnector(config=self.config)
        return self._tap_connector

    @property
    def catalog_dict(self) -> dict:
        """Generate or retrieve the catalog dictionary.

        Returns:
            A dictionary representing the tap's catalog with stream definitions.
        """
        if self._catalog_dict:  # Use cached catalog if available
            return self._catalog_dict

        if self.input_catalog:  # Use provided catalog if specified
            return self.input_catalog.to_dict()

        # Generate catalog by discovering streams from the database
        connector = self.tap_connector
        result = {"streams": connector.discover_catalog_entries()}
        self._catalog_dict = result
        return self._catalog_dict

    def discover_streams(self) -> list[SQLStream]:
        """Discover and initialize all available streams.

        Returns:
            A list of Stream objects based on the catalog entries.
        """
        streams = []
        for catalog_entry in self.catalog_dict["streams"]:
            streams.append(
                self.default_stream_class(
                    tap=self,
                    catalog_entry=catalog_entry,
                    connector=self.tap_connector
                )
            )
        return streams

    config_jsonschema = PropertiesList(
        Property("server", StringType, required=True, description="FQDN of the SQL server"),
        Property("port", IntegerType, default=1433, description="Port for SQL connection"),
        Property("database", StringType, required=True, description="Database name"),
        Property("user", StringType, required=True, description="User with SQL access"),
        Property("password", StringType, required=True, secret=True, description="Password for the user"),
        Property("driver", StringType, default="ODBC Driver 17 for SQL Server", description="ODBC driver name"),
        Property("start_date", DateTimeType, description="Earliest record date for incremental sync"),
    ).to_dict()

if __name__ == "__main__":
    Tapaptify.cli()