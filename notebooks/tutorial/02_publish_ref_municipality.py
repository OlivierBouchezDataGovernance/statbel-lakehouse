# Databricks notebook source
# MAGIC %md
# MAGIC # Tutorial 02 · Publish `ref_municipality` to BigQuery
# MAGIC
# MAGIC **Tutorial.** This notebook builds one table **by hand**, step by step, to
# MAGIC learn the method. Its tables are prefixed `tutorial_` so they never
# MAGIC overwrite the tables of the pipeline (notebooks `02` and `03`).
# MAGIC Run `00_setup` first.
# MAGIC
# MAGIC ```
# MAGIC Databricks (Unity Catalog)                          Google BigQuery
# MAGIC tutorial_silver_...      -- service account -->  <dataset>.tutorial_ref_municipality
# MAGIC   (data, descriptions, key)                         (data, descriptions, labels, key)
# MAGIC ```
# MAGIC
# MAGIC **Prerequisite:** tutorial `01_ref_municipality` has run.
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com
# MAGIC
# MAGIC **Security:** the BigQuery key is read from the Databricks secret scope
# MAGIC by `lib/bigquery`; it is never displayed. The OpenMetadata account
# MAGIC (`openmetadata-bigquery`) is a different, read-only account.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install the BigQuery library
# MAGIC
# MAGIC `cryptography` is pinned to the Databricks version (without the pin, pip
# MAGIC upgrades this core package). Databricks keeps these libraries in the
# MAGIC **notebook environment** (button *View environment*).
# MAGIC
# MAGIC After `%pip install`, Python must be restarted: all variables are lost,
# MAGIC so the utilities are included afterwards.

# COMMAND ----------

# MAGIC %pip install google-cloud-bigquery "cryptography==46.0.3" pyyaml

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../lib/bigquery

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Check the connection
# MAGIC
# MAGIC Expected: your service account, your dataset and its location.

# COMMAND ----------

client = get_bigquery_client()
dataset = client.get_dataset(f"{PROJECT_ID}.{DATASET}")
account = list(client.query("SELECT SESSION_USER() AS account").result())[0]
print("Connected as:", account["account"])
print("Dataset:", dataset.dataset_id, "| region:", dataset.location)
tables = sorted(table.table_id for table in client.list_tables(dataset))
print("Tables:", tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Publish the table
# MAGIC
# MAGIC Expected: **621 rows**, about 50 KB, primary key `CD_REFNIS`
# MAGIC (3 regions, 10 provinces, 43 districts, 565 municipalities).

# COMMAND ----------

publish_table(
    source_name="tutorial_silver_ref_municipality",
    target_table="tutorial_ref_municipality",
    labels=make_labels(domain="ref", period="2025-01-01"),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ `tutorial_ref_municipality` in BigQuery with description, labels, column
# MAGIC descriptions and primary key.
# MAGIC
# MAGIC Check in the BigQuery console: your dataset → `tutorial_ref_municipality` → tabs
# MAGIC **Schema** and **Details**.
