import json
import os
from datetime import date, datetime
from zipfile import ZipFile


SNOWFLAKE_EMULATOR = os.getenv("SNOWFLAKE_EMULATOR", "").lower()

if (
    __name__ != "__main__" and 
    SNOWFLAKE_EMULATOR in { "1", "true", "yes",}
):
    from snowflake.connector.converter import SnowflakeConverter
    from snowflake.sqlalchemy.snowdialect import SnowflakeDialect

    original_date_converter = SnowflakeConverter._DATE_to_python
    original_numeric_converter = SnowflakeConverter.to_python_method

    def _convert_emulator_date(self, metadata):
        convert_snowflake_date = original_date_converter(self, metadata)

        def convert(value):
            if value.lstrip("-").isdigit():
                return convert_snowflake_date(value)
            try:
                return datetime.strptime(
                    value, "%Y-%m-%d %H:%M:%S %z UTC"
                ).date()
            except ValueError:
                return date.fromisoformat(value)

        return convert

    def _convert_emulator_numeric(self, type_name, column):
        if type_name.upper() in {"FLOAT", "DOUBLE"}:
            return original_numeric_converter(self, "REAL", column)
        if type_name.upper() in {
            "NUMBER",
            "DECIMAL",
            "NUMERIC",
            "INTEGER",
            "INT",
            "BIGINT",
        }:
            metadata = column.copy()
            if metadata.get("scale") is None:
                metadata["scale"] = 0
            return original_numeric_converter(self, "FIXED", metadata)
        return original_numeric_converter(self, type_name, column)

    def _get_emulator_server_version(self, connection):
        return (8, 0, 0)

    def _get_emulator_database_schema(self, connection):
        return None, "PUBLIC_GOLD"

    def _skip_emulator_rollback(self, dbapi_connection):
        return None

    SnowflakeConverter._DATE_to_python = _convert_emulator_date
    SnowflakeConverter.to_python_method = _convert_emulator_numeric
    SnowflakeDialect._get_server_version_info = _get_emulator_server_version
    SnowflakeDialect._current_database_schema = _get_emulator_database_schema
    SnowflakeDialect.do_rollback = _skip_emulator_rollback


def import_dashboards() -> None:
    from superset.app import create_app

    app = create_app()
    with app.app_context():
        from flask import g
        from superset import db, security_manager
        from superset.commands.dashboard.importers.dispatcher import (
            ImportDashboardsCommand,
        )
        from superset.commands.importers.v1.utils import get_contents_from_bundle

        user = security_manager.find_user(username=os.environ["ADMIN_USERNAME"])
        if user is None:
            user = next(
                (
                    candidate
                    for candidate in security_manager.get_all_users()
                    if any(
                        role.name.lower() == "admin" for role in candidate.roles
                    )
                ),
                None,
            )
        if user is None:
            raise RuntimeError(
                "Cannot import Superset dashboards: no configured or existing Admin user"
            )

        g.user = user
        with ZipFile("/tmp/superset_dashboards.zip") as bundle:
            contents = get_contents_from_bundle(bundle)
        ImportDashboardsCommand(contents, overwrite=True).run()

        if os.getenv("SNOWFLAKE_EMULATOR", "").lower() in {"1", "true", "yes"}:
            from superset.models.core import Database

            database = (
                db.session.query(Database)
                .filter_by(database_name="DWH_SNOWFLAKE")
                .one()
            )
            extra = database.get_extra()
            connect_args = extra.setdefault("engine_params", {}).setdefault(
                "connect_args", {}
            )
            connect_args.update(host="snowflake", port=443, insecure_mode=True)
            connect_args.pop("protocol", None)
            connect_args.pop("disable_ocsp_checks", None)
            session_parameters = connect_args.setdefault("session_parameters", {})
            session_parameters["AUTOCOMMIT"] = True
            database.extra = json.dumps(extra)
            db.session.commit()


if __name__ == "__main__":
    import_dashboards()
