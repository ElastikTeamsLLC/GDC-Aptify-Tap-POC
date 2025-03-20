from singer_sdk import SQLTap
from singer_sdk.typing import PropertiesList, Property, StringType, IntegerType, DateTimeType
from .client import aptifyConnector, aptifyStream

class Tapaptify(SQLTap):
    """Singer tap for extracting data from an MSSQL (Azure SQL) database."""

    name = "tap-aptify"
    default_stream_class = aptifyStream

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