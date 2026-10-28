# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Load Statbel data: landing → bronze → silver
# MAGIC
# MAGIC For every selected table of `config/datasets.yml`:
# MAGIC
# MAGIC | Step | Result |
# MAGIC |---|---|
# MAGIC | **Landing** | the Statbel files, unchanged, in `/Volumes/<catalog>/<schema>/landing/<table>/` |
# MAGIC | **Bronze** | `bronze_<table>` (one per source file): all columns as text, plus `_source_file`, `_source_url`, `_ingested_at` |
# MAGIC | **Silver** | `silver_<table>`: Statbel column names kept, typed, cleaned, `REF_PERIOD` added |
# MAGIC | **Quality** | key uniqueness, NULL keys, null rate and distinct values of the codes, unconverted values |
# MAGIC | **Documentation** | table and column descriptions, primary key (only if the checks pass) |
# MAGIC
# MAGIC Then, once all tables are loaded, the **foreign keys** are checked
# MAGIC (orphan rows are counted, never dropped) and declared.
# MAGIC Every check goes to the table `run_report`.
# MAGIC
# MAGIC **Prerequisites:** `00_setup` and `01_statbel_catalog` have run.
# MAGIC
# MAGIC **Widget `tables`:** `enabled` (default), `all`, or a list such as
# MAGIC `ref_municipality, ref_first_name`.
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %pip install pyyaml openpyxl pyproj

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./lib/transform

# COMMAND ----------

dbutils.widgets.text("tables", "enabled", "Tables (enabled, all, or list)")
ALL_TABLES = load_datasets()
SELECTED = select_tables(dbutils.widgets.get("tables"), ALL_TABLES)
print(f"Run {RUN_ID}: {len(SELECTED)} table(s):",
      ", ".join(t["table"] for t in SELECTED))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Load each table
# MAGIC
# MAGIC A table that fails does not stop the others: the error is printed,
# MAGIC reported and the notebook fails at the end.

# COMMAND ----------

def profile_codes(name, table):
    """Report the null rate and the distinct values of the CD_ columns."""
    df = spark.table(table_name(name))
    codes = [c for c in df.columns if c.startswith("CD_")]
    if not codes:
        return
    total = df.count()
    stats = df.agg(
        *[F.sum(F.col(c).isNull().cast("int")).alias(f"nulls_{c}")
          for c in codes],
        *[F.countDistinct(c).alias(f"distinct_{c}") for c in codes],
    ).first()
    for column in codes:
        null_rate = stats[f"nulls_{column}"] / total if total else 0
        report(table, f"null_rate:{column}", null_rate)
        report(table, f"distinct:{column}", stats[f"distinct_{column}"])


def load_table(table):
    """Landing, bronze, silver, quality and documentation of one table."""
    name = table["table"]
    silver_name = f"silver_{name}"
    print(f"\n=== {name} ===")
    load_bronze(table)
    rows = write_table(build_silver(table), silver_name)
    report(name, "rows_silver", rows)
    profile_codes(silver_name, name)

    primary_key = table["primary_key"]
    if primary_key:
        duplicates, null_keys = check_unique_key(silver_name, primary_key)
        if duplicates or null_keys:
            print(f"Primary key NOT declared for {name}: the candidate key "
                  f"{primary_key} is not unique or has NULL values.")
            primary_key = []
    report(name, "primary_key_declared", 1 if primary_key else 0,
           ", ".join(primary_key) or None)

    columns = spark.table(table_name(silver_name)).columns
    document_table(
        silver_name,
        description=table_description(table),
        column_comments=column_descriptions(table, columns),
        primary_key=primary_key,
    )


failures = {}
for table in SELECTED:
    try:
        load_table(table)
    except Exception as error:  # noqa: BLE001 - one table must not stop all
        failures[table["table"]] = f"{type(error).__name__}: {error}"
        report(table["table"], "load_failed", 1,
               failures[table["table"]][:900])
        print(f"FAILED {table['table']}: {failures[table['table']]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Foreign keys: orphans, then declaration
# MAGIC
# MAGIC Done for **every** configured table whose silver table exists, not
# MAGIC only the ones loaded in this run: reloading a table replaces its
# MAGIC primary key, which drops (CASCADE) the foreign keys pointing to it.
# MAGIC A foreign key is declared only if the referenced table has a primary
# MAGIC key on the referenced column.

# COMMAND ----------

for table in ALL_TABLES:
    silver_name = f"silver_{table['table']}"
    if not table_exists(silver_name):
        continue
    for key in table["foreign_keys"]:
        referenced = f"silver_{key['references']}"
        if not table_exists(referenced):
            print(f"Skipped {table['table']}.{key['column']}: "
                  f"{referenced} is not loaded.")
            continue
        count_orphans(silver_name, key["column"], referenced,
                      key["referenced_column"])
        try:
            declare_foreign_key(silver_name, key["column"], referenced,
                                key["referenced_column"])
        except Exception as error:  # noqa: BLE001 - e.g. no primary key
            print(f"Foreign key {table['table']}.{key['column']} not "
                  f"declared: {str(error).splitlines()[0]}")
            report(table["table"], f"foreign_key_not_declared:"
                   f"{key['column']}", 1, str(error).splitlines()[0][:900])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run report

# COMMAND ----------

save_report()
display(
    spark.table(table_name("run_report"))
    .filter(F.col("RUN_ID") == RUN_ID)
    .groupBy("TABLE_NAME")
    .pivot("CHECK_NAME", ["rows_bronze", "rows_silver", "duplicate_keys",
                          "null_key_rows", "primary_key_declared",
                          "columns_without_description", "load_failed"])
    .agg(F.sum("CHECK_VALUE"))
    .orderBy("TABLE_NAME")
)

# COMMAND ----------

# MAGIC %md
# MAGIC Orphan rows and unconverted values of this run (all checks are in
# MAGIC `run_report`):

# COMMAND ----------

display(
    spark.table(table_name("run_report"))
    .filter((F.col("RUN_ID") == RUN_ID)
            & (F.col("CHECK_NAME").startswith("orphans:")
               | F.col("CHECK_NAME").startswith("unparsed:"))
            & (F.col("CHECK_VALUE") > 0))
    .select("TABLE_NAME", "CHECK_NAME", "CHECK_VALUE", "DETAIL")
    .orderBy("TABLE_NAME", "CHECK_NAME")
)

# COMMAND ----------

if failures:
    raise RuntimeError(f"{len(failures)} table(s) failed: {failures}")
print("All selected tables loaded. Next: 03_publish_bigquery.")
