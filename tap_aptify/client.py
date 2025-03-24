from __future__ import annotations

import gzip
import json
import datetime

from base64 import b64encode
from decimal import Decimal
from uuid import uuid4
from typing import Any, Iterable, Iterator

import pendulum
import pyodbc
import sqlalchemy

from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.batch import BaseBatcher, lazy_chunked_generator


class aptifyConnector(SQLConnector):
    """Connects to the mssql SQL source."""

    def __init__(
            self,
            config: dict | None = None,
            sqlalchemy_url: str | None = None
         ) -> None:
        """Class Default Init

        If pyodbc is used, disable its pooling so SQLAlchemy manages it.
        """
        if config.get('driver_type') == 'pyodbc':
            pyodbc.pooling = False
        super().__init__(config, sqlalchemy_url)

    def get_sqlalchemy_url(self, config: dict) -> str:
        """Return the SQLAlchemy URL string.

        Builds a URL using the provided config and adds the required
        driver and SSL query parameters.
        """
        url_drivername = f"{config.get('dialect', 'mssql')}+{config.get('driver_type', 'pyodbc')}"
        config_url = URL.create(
            url_drivername,
            username=config.get('user'),
            password=config.get('password'),
            host=config.get('host'),
            database=config.get('database'),
        )

        if 'port' in config:
            config_url = config_url.set(port=config.get('port'))

        driver_query = {
            "driver": "ODBC Driver 17 for SQL Server",
            "TrustServerCertificate": "yes",
            "Encrypt": "yes"
        }
        if 'sqlalchemy_url_query' in config:
            driver_query.update(config.get('sqlalchemy_url_query'))
        config_url = config_url.update_query_dict(driver_query)
        return str(config_url)

    def create_engine(self) -> Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Uses engine configuration parameters to create an engine.
        """
        eng_prefix = "ep."
        eng_config = {
            f"{eng_prefix}url": self.sqlalchemy_url,
            f"{eng_prefix}echo": "False"
        }

        if self.config.get('sqlalchemy_eng_params'):
            for key, value in self.config['sqlalchemy_eng_params'].items():
                eng_config.update({f"{eng_prefix}{key}": value})

        return sqlalchemy.engine_from_config(eng_config, prefix=eng_prefix)

    def to_jsonschema_type(
            self,
            from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type.

        Delegates to HD conversion if enabled in the config.
        """
        if self.config.get('hd_jsonschema_types', False):
            return self.hd_to_jsonschema_type(from_type)
        else:
            return self.org_to_jsonschema_type(from_type)

    @staticmethod
    def org_to_jsonschema_type(
        from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type (standard conversion).

        Handles MSSQL-specific types such as NUMERIC, MONEY, and BIT.
        """
        if str(from_type).startswith("NUMERIC"):
            if str(from_type).endswith(", 0)"):
                from_type = "int"
            else:
                from_type = "number"

        if str(from_type) in ["MONEY", "SMALLMONEY"]:
            from_type = "number"

        if str(from_type) in ['BIT']:
            from_type = "bool"
        
        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def hd_to_jsonschema_type(
        from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> dict:
        """Returns a JSON Schema equivalent for the given SQL type (HD conversion).

        Provides more detailed schema definitions for MSSQL-specific types.
        """
        if isinstance(from_type, str):
            sql_type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            sql_type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(from_type, sqlalchemy.types.TypeEngine):
            sql_type_name = from_type.__name__
        else:
            raise ValueError("Expected `str` or a SQLAlchemy `TypeEngine` object or type.")

        if sql_type_name in ['CHAR', 'NCHAR', 'VARCHAR', 'NVARCHAR']:
            maxLength: int = getattr(from_type, 'length', None)
            if getattr(from_type, 'length', None):
                return {"type": ["string"], "maxLength": maxLength}

        if sql_type_name == 'TIME':
            return {"type": ["string"], "format": "time"}

        if sql_type_name == 'UNIQUEIDENTIFIER':
            return {"type": ["string"], "format": "uuid"}

        if sql_type_name == 'XML':
            return {"type": ["string"], "contentMediaType": "application/xml"}

        if sql_type_name in ['BINARY', 'IMAGE', 'VARBINARY']:
            maxLength: int = getattr(from_type, 'length', None)
            if getattr(from_type, 'length', None):
                return {"type": ["string"], "contentEncoding": "base64", "maxLength": maxLength}
            else:
                return {"type": ["string"], "contentEncoding": "base64"}

        if sql_type_name == 'BIT':
            return {"type": ["boolean"]}

        if sql_type_name == 'TINYINT':
            return {"type": ["integer"], "minimum": 0, "maximum": 255}

        if sql_type_name == 'SMALLINT':
            return {"type": ["integer"], "minimum": -32768, "maximum": 32767}

        if sql_type_name == 'INTEGER':
            return {"type": ["integer"], "minimum": -2147483648, "maximum": 2147483647}

        if sql_type_name == 'BIGINT':
            return {"type": ["integer"], "minimum": -9223372036854775808, "maximum": 9223372036854775807}

        if sql_type_name in ("NUMERIC", "DECIMAL"):
            precision: int = getattr(from_type, 'precision', None)
            scale: int = getattr(from_type, 'scale', None)
            if scale == 0:
                return {"type": ["integer"], "minimum": (-pow(10, precision)) + 1, "maximum": (pow(10, precision)) - 1}
            else:
                maximum_as_number = ""
                minimum_as_number: str = "-"
                for i in range(precision):
                    if i == (precision - scale):
                        maximum_as_number += '.'
                    maximum_as_number += '9'
                minimum_as_number += maximum_as_number

                maximum_scientific_format: str = "9."
                minimum_scientific_format: str = "-"
                for i in range(scale):
                    maximum_scientific_format += '9'
                maximum_scientific_format += f"e+{precision}"
                minimum_scientific_format += maximum_scientific_format

                if "e+" not in str(float(maximum_as_number)):
                    return {"type": ["number"], "minimum": float(minimum_as_number), "maximum": float(maximum_as_number)}
                else:
                    return {"type": ["number"], "minimum": float(minimum_scientific_format), "maximum": float(maximum_scientific_format)}

        if sql_type_name == "SMALLMONEY":
            return {"type": ["number"], "minimum": -214748.3648, "maximum": 214748.3647}

        if sql_type_name == "MONEY":
            return {"type": ["number"], "minimum": -922337203685477.5808, "maximum": 922337203685477.5807}

        if sql_type_name == "FLOAT":
            return {"type": ["number"], "minimum": -1.79e308, "maximum": 1.79e308}

        if sql_type_name == "REAL":
            return {"type": ["number"], "minimum": -3.40e38, "maximum": 3.40e38}

        return SQLConnector.to_jsonschema_type(from_type)

    @staticmethod
    def to_sql_type(jsonschema_type: dict) -> sqlalchemy.types.TypeEngine:
        """Return a SQLAlchemy type representation of the provided JSON Schema type."""
        return SQLConnector.to_sql_type(jsonschema_type)
    
    @staticmethod
    def get_fully_qualified_name(
        table_name: str | None = None,
        schema_name: str | None = None,
        db_name: str | None = None,
        delimiter: str = ".",
    ) -> str:
        """Concatenates a fully qualified name from the parts."""
        parts = []
        if table_name:
            parts.append(table_name)
        if not parts:
            raise ValueError("Could not generate fully qualified name: " + (table_name or "(unknown-table-name)"))
        return table_name


class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return pendulum.instance(obj).isoformat()
        if isinstance(obj, datetime.date):
            return obj.isoformat()
        if isinstance(obj, datetime.time):
            return obj.isoformat(timespec='seconds')
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class JSONLinesBatcher(BaseBatcher):
    encoder_class = CustomJSONEncoder

    def get_batches(
        self,
        records: Iterator[dict],
    ) -> Iterator[list[str]]:
        sync_id = f"{self.tap_name}--{self.stream_name}-{uuid4()}"
        prefix = self.batch_config.storage.prefix or ""
        for i, chunk in enumerate(lazy_chunked_generator(records, self.batch_config.batch_size), start=1):
            filename = f"{prefix}{sync_id}-{i}.json.gz"
            with self.batch_config.storage.fs(create=True) as fs:
                with fs.open(filename, "wb") as f, gzip.GzipFile(fileobj=f, mode="wb") as gz:
                    gz.writelines(
                        (json.dumps(record, cls=self.encoder_class, default=str) + "\n").encode()
                        for record in chunk
                    )
                file_url = fs.geturl(filename)
            yield [file_url]


class aptifyStream(SQLStream):
    """Stream class for mssql streams."""

    connector_class = aptifyConnector

    def post_process(
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        """Append or transform raw data to match expected structure."""
        record: dict = row
        properties: dict = self.schema.get('properties')
        for key, value in record.items():
            if value is not None:
                property_schema: dict = properties.get(key)
                if isinstance(value, datetime.date):
                    record.update({key: value.isoformat()})
                if property_schema.get('contentEncoding') == 'base64':
                    record.update({key: b64encode(value).decode()})
        return record

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Yield one dict per record from the source."""
        if context:
            raise NotImplementedError(f"Stream '{self.name}' does not support partitioning.")
        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)
            if replication_key_col.type.python_type in (datetime.datetime, datetime.date):
                start_val = self.get_starting_timestamp(context)
            else:
                start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(replication_key_col >= start_val)
        if self.ABORT_AT_RECORD_COUNT is not None:
            query = query.limit(self.ABORT_AT_RECORD_COUNT + 1)
        with self.connector._connect() as conn:
            for record in conn.execute(query):
                transformed_record = self.post_process(dict(record._mapping))
                if transformed_record is None:
                    continue
                yield transformed_record
