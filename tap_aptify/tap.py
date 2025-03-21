from singer_sdk import SQLConnector, SQLTap, SQLStream
from singer_sdk.typing import PropertiesList, Property, StringType, IntegerType, DateTimeType
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
        Property("connection_string", StringType, description="Optional full connection string"),
        Property("server", StringType, required=True, description="FQDN of the SQL server"),
        Property("port", IntegerType, default=1433, description="Port for SQL connection"),
        Property("database", StringType, required=True, description="Database name"),
        Property("user", StringType, required=True, description="User with SQL access"),
        Property("password", StringType, required=True, secret=True, description="Password for the user"),
        Property("driver", StringType, default="ODBC Driver 17 for SQL Server", description="ODBC driver name"),
        Property("start_date", DateTimeType, description="Earliest record date for incremental sync"),
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