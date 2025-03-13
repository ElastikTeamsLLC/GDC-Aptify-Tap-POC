"""aptify tap class."""

from __future__ import annotations

from singer_sdk import SQLTap
from singer_sdk import typing as th  # JSON schema typing helpers

from tap_aptify.client import aptifyStream


class Tapaptify(SQLTap):
    """aptify tap class."""

    name = "tap-aptify"
    default_stream_class = aptifyStream

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "auth_token",
            th.StringType,
            required=True,
            secret=True,  # Flag config as protected.
            title="Auth Token",
            description="The token to authenticate against the API service",
        ),
        th.Property(
            "project_ids",
            th.ArrayType(th.StringType),
            required=True,
            title="Project IDs",
            description="Project IDs to replicate",
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
            description="The earliest record date to sync",
        ),
        th.Property(
            "api_url",
            th.StringType,
            title="API URL",
            default="https://api.mysample.com",
            description="The url for the API service",
        ),
    ).to_dict()


if __name__ == "__main__":
    Tapaptify.cli()
