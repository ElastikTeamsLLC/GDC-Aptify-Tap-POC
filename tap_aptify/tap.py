from singer_sdk import SQLConnector, SQLTap, SQLStream
from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.typing import PropertiesList, Property, StringType, BooleanType, ObjectType, DateTimeType, IntegerType
from .client import aptifyConnector, aptifyStream


class Tapaptify(SQLTap):
    """Singer tap for extracting data from an MSSQL (Azure SQL) database."""

    name = "tap-aptify"
    default_stream_class = aptifyStream
    default_connector_class = aptifyConnector
    _tap_connector = None

    @property
    def tap_connector(self) -> SQLConnector:
        """Return the connector object."""
        if self._tap_connector is None:
            self._tap_connector = self.default_connector_class(dict(self.config))
        return self._tap_connector

    @property
    def catalog_dict(self) -> dict:
        """Get catalog dictionary."""
        if self._catalog_dict:
            return self._catalog_dict
        if self.input_catalog:
            return self.input_catalog.to_dict()
        connector = self.tap_connector
        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(connector.discover_catalog_entries())
        self._catalog_dict = result
        return self._catalog_dict

    config_jsonschema = th.PropertiesList(
        th.Property(
            "dialect",
            th.StringType,
            description="The Dialect of SQLAlchemy",
            required=True,
            allowed_values=["mssql"],
            default="mssql"
        ),
        th.Property(
            "driver_type",
            th.StringType,
            description="The Python Driver to connect to the SQL server",
            required=True,
            allowed_values=["pyodbc"],
            default="pyodbc"
        ),
        th.Property(
            "host",
            th.StringType,
            description="The FQDN of the SQL host",
            required=True
        ),
        th.Property(
            "port",
            th.StringType,
            description="The port on which SQL awaits connection"
        ),
        th.Property(
            "user",
            th.StringType,
            description="The User account for SQL access",
            required=True
        ),
        th.Property(
            "password",
            th.StringType,
            description="The Password for the User account",
            required=True,
            secret=True
        ),
        th.Property(
            "database",
            th.StringType,
            description="The default database for connection",
            required=True
        ),
        th.Property(
            "sqlalchemy_eng_params",
            th.ObjectType(
                th.Property(
                    "fast_executemany",
                    th.StringType,
                    description="Fast Executemany mode: True or False"
                ),
                th.Property(
                    "future",
                    th.StringType,
                    description="Run engine in 2.0 mode: True or False"
                )
            ),
            description="SQLAlchemy Engine Parameters",
        ),
        th.Property(
            "sqlalchemy_url_query",
            th.ObjectType(
                th.Property(
                    "driver",
                    th.StringType,
                    description="The driver to use for the connection"
                ),
                th.Property(
                    "TrustServerCertificate",
                    th.StringType,
                    description="Yes or No"
                )
            ),
            description="SQLAlchemy URL Query options",
        ),
        th.Property(
            "batch_config",
            th.ObjectType(
                th.Property(
                    "encoding",
                    th.ObjectType(
                        th.Property(
                            "format",
                            th.StringType,
                            description="Currently only jsonl is supported",
                        ),
                        th.Property(
                            "compression",
                            th.StringType,
                            description="Currently only gzip is supported",
                        )
                    )
                ),
                th.Property(
                    "storage",
                    th.ObjectType(
                        th.Property(
                            "root",
                            th.StringType,
                            description="Directory for batch messages (e.g., file://batches)",
                        ),
                        th.Property(
                            "prefix",
                            th.StringType,
                            description="Prefix for batch messages (e.g., tap-batch-)",
                        )
                    )
                )
            ),
            description="Optional Batch Message configuration",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync"
        ),
        th.Property(
            "hd_jsonschema_types",
            th.BooleanType,
            default=False,
            description="Enable HD JSON Schema types"
        ),
    ).to_dict()

    def discover_streams(self) -> list[SQLStream]:
        """Initialize and return all available streams."""
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
