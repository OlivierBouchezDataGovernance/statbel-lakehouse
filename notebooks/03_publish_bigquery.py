# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Publish to BigQuery
# MAGIC
# MAGIC ```
# MAGIC Databricks (Unity Catalog)                         Google BigQuery
# MAGIC silver_<table>  --- Parquet + service account --->  <dataset>.<table>
# MAGIC   data, descriptions, primary key                   data, descriptions,
# MAGIC                                                     labels, keys, GEOGRAPHY
# MAGIC ```
# MAGIC
# MAGIC | Step | What is done |
# MAGIC |---|---|
# MAGIC | 1 | drop the foreign keys of the configured tables (they would block the reloads) |
# MAGIC | 2 | publish each selected table (`publish_table()`) |
# MAGIC | 3 | declare the foreign keys between the published tables |
# MAGIC | 4 | copy the `run_report` table to BigQuery and list the tables |
# MAGIC
# MAGIC **Prerequisite:** `02_load_statbel` has run for the selected tables.
# MAGIC
# MAGIC **Widgets:** `tables` (`enabled`, `all` or a list) and `allow_large`
# MAGIC (publish tables above `max_table_size_gb`, see `environment.yml`).
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %pip install google-cloud-bigquery cryptography==46.0.3 pyyaml

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./lib/bigquery

# COMMAND ----------

dbutils.widgets.text("tables", "enabled", "Tables (enabled, all, or list)")
dbutils.widgets.dropdown("allow_large", "false", ["false", "true"],
                         "Publish tables above the size limit")
ALL_TABLES = load_datasets()
SELECTED = [
    t for t in select_tables(dbutils.widgets.get("tables"), ALL_TABLES)
    if table_exists(f"silver_{t['table']}")
]
ALLOW_LARGE = dbutils.widgets.get("allow_large") == "true"
print(f"Run {RUN_ID}: publishing to {PROJECT_ID}.{DATASET} ({LOCATION}):",
      ", ".join(t["table"] for t in SELECTED))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Drop the foreign keys
# MAGIC
# MAGIC Replacing a table, or its primary key, is refused or breaks the foreign
# MAGIC keys of the tables pointing to it. They are dropped first and declared
# MAGIC again in step 3.

# COMMAND ----------

dropped = sum(drop_bigquery_foreign_keys(t["table"]) for t in ALL_TABLES)
print(f"{dropped} foreign key(s) dropped")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Publish the tables

# COMMAND ----------

failures = {}
for table in SELECTED:
    print(f"\n=== {table['table']} ===")
    try:
        publish_table(
            source_name=f"silver_{table['table']}",
            target_table=table["table"],
            labels=make_labels(table["domain"], table["period"]),
            geography=table.get("geography"),
            allow_large=ALLOW_LARGE,
        )
    except Exception as error:  # noqa: BLE001 - one table must not stop all
        failures[table["table"]] = f"{type(error).__name__}: {error}"
        report(table["table"], "publish_failed", 1,
               failures[table["table"]][:900])
        print(f"FAILED {table['table']}: {failures[table['table']]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Declare the foreign keys
# MAGIC
# MAGIC For every configured table present in BigQuery, if the referenced
# MAGIC table is also present. Orphan counts are in the run report of
# MAGIC notebook 02.

# COMMAND ----------

present = published_tables()
declared = 0
for table in ALL_TABLES:
    if table["table"] not in present:
        continue
    for key in table["foreign_keys"]:
        if key["references"] == table["table"]:
            # BigQuery refuses a foreign key to the same table: the
            # hierarchy stays declared in Unity Catalog only.
            print(f"Skipped {table['table']}.{key['column']}: BigQuery "
                  "does not support self-referencing foreign keys.")
            report(table["table"], f"foreign_key_not_declared:"
                   f"{key['column']}", 1, "self-reference (BigQuery)")
            continue
        if key["references"] not in present:
            print(f"Skipped {table['table']}.{key['column']}: "
                  f"{key['references']} is not published.")
            continue
        try:
            declare_bigquery_foreign_key(table["table"], key["column"],
                                         key["references"],
                                         key["referenced_column"])
            declared += 1
        except Exception as error:  # noqa: BLE001
            print(f"Foreign key {table['table']}.{key['column']} not "
                  f"declared: {str(error).splitlines()[0]}")
print(f"{declared} foreign key(s) declared")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Run report and summary

# COMMAND ----------

save_report()
publish_run_report()

client = get_bigquery_client()
summary = []
for table_id in sorted(published_tables()):
    table = client.get_table(bigquery_table(table_id))
    constraints = table.table_constraints
    summary.append((
        table_id, table.num_rows, round(table.num_bytes / 1e6, 2),
        ", ".join(constraints.primary_key.columns)
        if constraints and constraints.primary_key else "",
        len(constraints.foreign_keys or []) if constraints else 0,
        sum(1 for f in table.schema
            if f.description and f.description != MISSING_DESCRIPTION),
        len(table.schema),
    ))
display(spark.createDataFrame(
    summary, "table STRING, rows BIGINT, size_mb DOUBLE, primary_key STRING, "
             "foreign_keys INT, described_columns INT, columns INT"))

# COMMAND ----------

if failures:
    raise RuntimeError(f"{len(failures)} table(s) failed: {failures}")
print("Done. Next: connect OpenMetadata to the BigQuery dataset.")
