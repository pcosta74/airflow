import re
from pathlib import Path
from zipfile import ZipFile

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DASHBOARD_BUNDLE = PROJECT_ROOT / "deploy" / "superset_dashboards.zip"
GOLD_DDL = (PROJECT_ROOT / "deploy" / "init-datawarehouse.sf.sql").read_text()


def _bundle_yaml_files(kind):
    with ZipFile(DASHBOARD_BUNDLE) as bundle:
        return {
            Path(path).stem: yaml.safe_load(bundle.read(path))
            for path in bundle.namelist()
            if f"/{kind}/" in path and path.endswith((".yaml", ".yml"))
        }


def test_dashboard_bundle_uses_native_superset_export_format():
    with ZipFile(DASHBOARD_BUNDLE) as bundle:
        files = bundle.namelist()
        metadata_path = next(path for path in files if path.endswith("/metadata.yaml"))
        metadata = yaml.safe_load(bundle.read(metadata_path))
        databases = _bundle_yaml_files("databases")
        database = next(iter(databases.values()))

    assert metadata["version"] == "1.0.0"
    assert metadata["type"] == "Dashboard"
    assert len(_bundle_yaml_files("dashboards")) == 1
    assert len(_bundle_yaml_files("charts")) == 4
    assert len(_bundle_yaml_files("datasets")) == 4
    assert len(databases) == 1
    assert database["version"] == "1.0.0"
    assert database.get("password") is None
    assert database["sqlalchemy_uri"].split("@", maxsplit=1)[0].endswith(":")


def test_deployed_datasets_match_every_gold_table_column():
    datasets = _bundle_yaml_files("datasets")
    gold_tables = re.findall(
        r"CREATE TABLE IF NOT EXISTS \$\{SNOWFLAKE_DATABASE\}\."
        r"\$\{SNOWFLAKE_SCHEMA\}_gold\.([a-z_]+)\s*\((.*?)\);",
        GOLD_DDL,
        flags=re.DOTALL,
    )

    assert {dataset["table_name"] for dataset in datasets.values()} == {
        table_name for table_name, _ in gold_tables
    }

    for table_name, table_ddl in gold_tables:
        dataset = next(
            item for item in datasets.values() if item["table_name"] == table_name
        )
        ddl_columns = re.findall(
            r"^\s+(\w+)\s+(?:DATE|BIGINT|STRING|INT|DOUBLE)\b",
            table_ddl,
            flags=re.MULTILINE,
        )
        dataset_columns = [column["column_name"] for column in dataset["columns"]]
        assert len(dataset_columns) == len(set(dataset_columns))
        assert set(dataset_columns) == set(ddl_columns)
        assert dataset["schema"] == "public_gold"
        assert dataset["main_dttm_col"] in ddl_columns


def test_dashboard_charts_reference_deployed_dataset_fields_and_metrics():
    datasets = _bundle_yaml_files("datasets")
    charts = _bundle_yaml_files("charts")
    dataset_by_uuid = {dataset["uuid"]: dataset for dataset in datasets.values()}
    chart_uuids = set()

    for chart in charts.values():
        chart_uuids.add(chart["uuid"])
        dataset = dataset_by_uuid[chart["dataset_uuid"]]
        columns = {column["column_name"] for column in dataset["columns"]}
        datetime_columns = {
            column["column_name"]
            for column in dataset["columns"]
            if column.get("is_dttm")
        }
        metrics = {metric["metric_name"] for metric in dataset["metrics"]}
        assert set(chart["params"]["groupby"]) <= columns
        assert set(chart["params"]["metrics"]) <= metrics
        assert chart["viz_type"] == "echarts_timeseries_bar"
        assert chart["params"]["x_axis"] in datetime_columns

    dashboard = next(iter(_bundle_yaml_files("dashboards").values()))
    dashboard_chart_uuids = {
        node["meta"]["uuid"]
        for node in dashboard["position"].values()
        if isinstance(node, dict)
        and node.get("type") == "CHART"
    }
    assert dashboard_chart_uuids == chart_uuids
