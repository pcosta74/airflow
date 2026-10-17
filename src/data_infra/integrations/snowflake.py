import os

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def create_snowflake_hook(snowflake_conn_id: str, **kwargs) -> SnowflakeHook:
    if os.getenv("SNOWFLAKE_EMULATOR", "").lower() in {"1", "true", "yes"}:
        session_parameters = dict(kwargs.get("session_parameters") or {})
        session_parameters["AUTOCOMMIT"] = True
        kwargs["session_parameters"] = session_parameters

    return SnowflakeHook(snowflake_conn_id=snowflake_conn_id, **kwargs)


def table_name(database: str, schema: str, table: str) -> str:
    """Build a table name for Snowflake or the emulator's default catalog."""
    if os.getenv("SNOWFLAKE_EMULATOR", "").lower() in {"1", "true", "yes"}:
        return f"{schema}.{table}"
    return f"{database}.{schema}.{table}"
