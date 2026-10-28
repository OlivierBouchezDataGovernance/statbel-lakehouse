# Databricks notebook source
# MAGIC %md
# MAGIC # lib · bigquery — publish silver tables to BigQuery
# MAGIC
# MAGIC Included with `%run ./lib/bigquery` (it includes `common` and
# MAGIC `config`).
# MAGIC
# MAGIC **Required in the calling notebook, before the `%run`:**
# MAGIC
# MAGIC ```
# MAGIC %pip install google-cloud-bigquery "cryptography==46.0.3" pyyaml
# MAGIC %restart_python
# MAGIC ```
# MAGIC
# MAGIC `cryptography` is pinned to the version shipped with Databricks
# MAGIC serverless: without the pin, pip upgrades this core package.
# MAGIC
# MAGIC **Security.** The service account key is read from the Databricks
# MAGIC secret scope named in `environment.yml`; it is never displayed or
# MAGIC written anywhere. The account only needs *BigQuery Data Editor* on the
# MAGIC dataset and *BigQuery Job User* on the project.
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC |---|---|
# MAGIC | `get_bigquery_client()` | client authenticated with the secret key |
# MAGIC | `make_labels()` | standard labels of a table |
# MAGIC | `publish_table()` | load a silver table (Parquet), geography, descriptions, labels, primary key |
# MAGIC | `drop_bigquery_foreign_keys()` / `declare_bigquery_foreign_key()` | foreign keys, handled after all loads |
# MAGIC | `publish_run_report()` | copy the `run_report` table to BigQuery |
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

import json
import re

from google.cloud import bigquery
from google.oauth2 import service_account

ENVIRONMENT = load_environment()
PROJECT_ID = ENVIRONMENT["bigquery"]["project_id"]
DATASET = ENVIRONMENT["bigquery"]["dataset"]
LOCATION = ENVIRONMENT["bigquery"].get("location", "EU")
MAX_TABLE_SIZE_GB = float(ENVIRONMENT["bigquery"].get("max_table_size_gb",
                                                      1.0))
SECRET_SCOPE = ENVIRONMENT["secrets"]["scope"]
SECRET_KEY = ENVIRONMENT["secrets"]["bigquery_key"]
MISSING_DESCRIPTION = "Description missing in the Statbel dictionary"

# Parquet files are compressed: BigQuery stores about 4 times more bytes.
PARQUET_TO_BIGQUERY_FACTOR = 4


def get_bigquery_client():
    """Return a BigQuery client authenticated with the secret scope key."""
    key_info = json.loads(
        dbutils.secrets.get(scope=SECRET_SCOPE, key=SECRET_KEY)
    )
    credentials = service_account.Credentials.from_service_account_info(
        key_info
    )
    return bigquery.Client(
        project=PROJECT_ID, credentials=credentials, location=LOCATION
    )


def bigquery_table(name):
    """Fully qualified BigQuery name of a table of the dataset."""
    return f"{PROJECT_ID}.{DATASET}.{name}"


def make_labels(domain, period):
    """Return the standard labels of a Statbel table.

    Label values may only contain lower-case letters, digits, '-' and '_'.
    """
    period_label = re.sub(r"[^a-z0-9_-]+", "-", str(period).lower())
    return {
        "source": "statbel",
        "licence": "cc-by-4-0",
        "domain": domain,
        "period": period_label[:63],
        "layer": "silver",
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## `publish_table()`
# MAGIC
# MAGIC | Step | What is done | Why |
# MAGIC |---|---|---|
# MAGIC | 1 | export the silver table to Parquet files in the `export` volume | scales to millions of rows (no `toPandas()`) |
# MAGIC | 2 | check the estimated size against `max_table_size_gb` | stay within the free tier |
# MAGIC | 3 | load the files: first `WRITE_TRUNCATE`, then `WRITE_APPEND` | running again replaces the content |
# MAGIC | 4 | build the `GEOGRAPHY` column, if configured | BigQuery GIS functions |
# MAGIC | 5 | write the column descriptions, table description and labels | read from Unity Catalog |
# MAGIC | 6 | declare the primary key (*NOT ENFORCED*) | only if declared in Unity Catalog (checked unique) |

# COMMAND ----------

def _export_parquet(source_name):
    """Write a table as Parquet files in the export volume; return them."""
    folder = f"{EXPORT_VOLUME}/{source_name}"
    spark.table(table_name(source_name)).write.mode("overwrite").parquet(
        folder)
    return sorted(
        os.path.join(folder, name) for name in os.listdir(folder)
        if name.endswith(".parquet")
    )


def _unity_catalog_metadata(source_name):
    """Return (table description, {column: comment}, primary key columns)."""
    description = spark.sql(
        f"DESCRIBE DETAIL {table_name(source_name)}").first()["description"]
    comments = {
        row.col_name: row.comment
        for row in spark.sql(f"DESCRIBE TABLE {table_name(source_name)}")
        .collect()
        if not row.col_name.startswith("#")
    }
    primary_key = [
        row.column_name for row in spark.sql(f"""
            SELECT kcu.column_name
            FROM {CATALOG}.information_schema.table_constraints AS tc
            JOIN {CATALOG}.information_schema.key_column_usage AS kcu
              ON kcu.constraint_name = tc.constraint_name
             AND kcu.table_schema = tc.table_schema
            WHERE tc.table_schema = '{SCHEMA}'
              AND tc.table_name = '{source_name}'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position""").collect()
    ]
    return description, comments, primary_key


def _geography_sql(target, geography):
    """CREATE OR REPLACE statement adding a GEOGRAPHY column, or None."""
    if "geojson" in geography:
        column = geography["geojson"]
        return (f"CREATE OR REPLACE TABLE `{target}` AS SELECT * REPLACE "
                f"(ST_GEOGFROMGEOJSON({column}, make_valid => TRUE) "
                f"AS {column}) FROM `{target}`")
    if "point" in geography:
        point = geography["point"]
        return (f"CREATE OR REPLACE TABLE `{target}` AS SELECT *, "
                f"ST_GEOGPOINT({point['longitude']}, {point['latitude']}) "
                f"AS GEOGRAPHY FROM `{target}`")
    return None


def publish_table(source_name, target_table, labels, geography=None,
                  allow_large=False):
    """Publish a silver table of Unity Catalog into the BigQuery dataset.

    Args:
        source_name: short Unity Catalog name, e.g. "silver_ref_municipality".
        target_table: BigQuery table name, e.g. "ref_municipality".
        labels: BigQuery labels (see make_labels()).
        geography: optional geography settings of the table.
        allow_large: publish even above max_table_size_gb.

    Returns:
        The BigQuery table after loading, or None if it was skipped.
    """
    client = get_bigquery_client()
    target = bigquery_table(target_table)

    files = _export_parquet(source_name)
    size_gb = sum(os.path.getsize(f) for f in files) / 1e9
    estimate_gb = size_gb * PARQUET_TO_BIGQUERY_FACTOR
    if estimate_gb > MAX_TABLE_SIZE_GB and not allow_large:
        print(f"SKIPPED {target}: estimated {estimate_gb:.2f} GB > "
              f"{MAX_TABLE_SIZE_GB} GB (set allow_large to publish it)")
        report(target_table, "skipped_size_gb", estimate_gb)
        return None

    for index, path in enumerate(files):
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition="WRITE_TRUNCATE" if index == 0
            else "WRITE_APPEND",
        )
        with open(path, "rb") as parquet_file:
            client.load_table_from_file(
                parquet_file, target, job_config=job_config).result()

    geography_sql = _geography_sql(target, geography or {})
    if geography_sql:
        client.query(geography_sql).result()

    description, comments, primary_key = _unity_catalog_metadata(source_name)
    table = client.get_table(target)
    table.schema = [
        bigquery.SchemaField(
            field.name, field.field_type, mode=field.mode,
            description=comments.get(field.name) or (
                "Geography (WGS 84) built from the coordinates"
                if field.name == "GEOGRAPHY" else MISSING_DESCRIPTION),
            fields=field.fields,
        )
        for field in table.schema
    ]
    table.description = description
    table.labels = labels
    client.update_table(table, ["schema", "description", "labels"])

    client.query(f"ALTER TABLE `{target}` DROP PRIMARY KEY IF EXISTS").result()
    if primary_key:
        client.query(
            f"ALTER TABLE `{target}` ADD PRIMARY KEY "
            f"({', '.join(primary_key)}) NOT ENFORCED"
        ).result()

    table = client.get_table(target)
    print(f"{target}: {table.num_rows:,} rows, "
          f"{table.num_bytes / 1e6:.1f} MB, primary key {primary_key}")
    report(target_table, "rows_bigquery", table.num_rows)
    report(target_table, "bigquery_mb", table.num_bytes / 1e6)
    return table

# COMMAND ----------

# MAGIC %md
# MAGIC ## Foreign keys
# MAGIC
# MAGIC They are dropped **before** the loads and declared **after** all of
# MAGIC them: replacing a table or its primary key would otherwise remove or
# MAGIC block the foreign keys of the tables pointing to it.

# COMMAND ----------

def drop_bigquery_foreign_keys(target_table):
    """Drop every foreign key of a BigQuery table (if it exists)."""
    client = get_bigquery_client()
    try:
        table = client.get_table(bigquery_table(target_table))
    except Exception:  # noqa: BLE001 - table not published yet
        return 0
    constraints = table.table_constraints
    foreign_keys = (constraints.foreign_keys or []) if constraints else []
    for foreign_key in foreign_keys:
        client.query(
            f"ALTER TABLE `{bigquery_table(target_table)}` "
            f"DROP CONSTRAINT IF EXISTS {foreign_key.name}"
        ).result()
    return len(foreign_keys)


def declare_bigquery_foreign_key(target_table, column, referenced_table,
                                 referenced_column):
    """Declare one foreign key in BigQuery (NOT ENFORCED)."""
    client = get_bigquery_client()
    constraint = f"fk_{target_table}_{column}".lower()
    client.query(
        f"ALTER TABLE `{bigquery_table(target_table)}` "
        f"ADD CONSTRAINT {constraint} FOREIGN KEY ({column}) REFERENCES "
        f"`{bigquery_table(referenced_table)}` ({referenced_column}) "
        f"NOT ENFORCED"
    ).result()


def published_tables():
    """Names of the tables present in the BigQuery dataset."""
    client = get_bigquery_client()
    return {t.table_id for t in client.list_tables(f"{PROJECT_ID}.{DATASET}")}

# COMMAND ----------

def publish_run_report():
    """Replace the BigQuery table run_report by the Delta table run_report."""
    if not table_exists("run_report"):
        return None
    client = get_bigquery_client()
    target = bigquery_table("run_report")
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        schema=[
            bigquery.SchemaField("RUN_ID", "STRING",
                                 description="Identifier of the run"),
            bigquery.SchemaField("RUN_AT", "TIMESTAMP",
                                 description="Time of the check (UTC)"),
            bigquery.SchemaField("TABLE_NAME", "STRING",
                                 description="Table checked"),
            bigquery.SchemaField("CHECK_NAME", "STRING",
                                 description="Name of the check"),
            bigquery.SchemaField("CHECK_VALUE", "FLOAT64",
                                 description="Result of the check"),
            bigquery.SchemaField("DETAIL", "STRING",
                                 description="Details (columns, references)"),
        ],
    )
    client.load_table_from_dataframe(
        spark.table(table_name("run_report")).toPandas(), target,
        job_config=job_config,
    ).result()
    table = client.get_table(target)
    table.description = (
        "Data quality checks of the pipeline runs: row counts, duplicate "
        "keys, NULL keys, null rates, distinct codes, orphan rows. "
        + LICENCE_LINE
    )
    table.labels = {"source": "statbel", "layer": "quality"}
    client.update_table(table, ["description", "labels"])
    print(f"{target}: {table.num_rows:,} check(s)")
    return table
