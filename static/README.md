# Demo PIM catalog

`product_catalog.json` is the canonical demo catalog, served read-only by the
Nginx container built from `Dockerfile.pim`. The container exposes the catalog
at `/products` and a health endpoint at `/health`; catalog data is validated
by the Airflow extractor before it is written to Parquet.
