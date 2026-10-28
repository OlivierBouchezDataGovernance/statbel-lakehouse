# Databricks notebook source
# MAGIC %md
# MAGIC # Tutorial 01 · Reference data: municipalities (`ref_municipality`)
# MAGIC
# MAGIC **Tutorial.** This notebook builds one table **by hand**, step by step, to
# MAGIC learn the method. Its tables are prefixed `tutorial_` so they never
# MAGIC overwrite the tables of the pipeline (notebooks `02` and `03`).
# MAGIC Run `00_setup` first.
# MAGIC
# MAGIC This notebook builds, for **one** Statbel source, the whole path of the
# MAGIC project:
# MAGIC
# MAGIC | Layer | Where | Content |
# MAGIC |---|---|---|
# MAGIC | **Landing** | volume `/Volumes/workspace/statbel/landing/ref_municipality/` | the Statbel zip file, unchanged |
# MAGIC | **Bronze** | table `workspace.statbel.bronze_ref_municipality` | raw data, all columns as text, plus lineage columns |
# MAGIC | **Silver** | table `workspace.statbel.silver_ref_municipality` | typed, cleaned and documented data with a primary key |
# MAGIC
# MAGIC **Source:** Statbel REFNIS codes (regions, provinces, districts,
# MAGIC municipalities), file
# MAGIC [TU_COM_REFNIS-20250101.zip](https://statbel.fgov.be/sites/default/files/files/opendata/REFNIS%20code/TU_COM_REFNIS-20250101.zip),
# MAGIC licence CC BY 4.0.
# MAGIC
# MAGIC **Compute:** *Serverless* (button **Connect**, top right). Run the cells
# MAGIC in order (**Run all**). Publishing to BigQuery: tutorial `02`.
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %run ../lib/common

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Parameters

# COMMAND ----------

SOURCE_URL = (
    "https://statbel.fgov.be/sites/default/files/files/opendata/"
    "REFNIS%20code/TU_COM_REFNIS-20250101.zip"
)
LANDING_DIR = f"{LANDING_VOLUME}/tutorial"
BRONZE = "tutorial_bronze_ref_municipality"
SILVER = "tutorial_silver_ref_municipality"
REFERENCE_DATE = "2025-01-01"  # date of the REFNIS list in the file name
CURRENT_END_DATE = "31/12/9999"  # end date of the units still valid today

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Landing: download the file into the volume
# MAGIC
# MAGIC `download_zip()` runs two steps: `download_file()` stores the zip file
# MAGIC unchanged, then `extract_zip()` checks that it is a valid archive and
# MAGIC extracts the text file next to it.

# COMMAND ----------

text_path = download_zip(SOURCE_URL, LANDING_DIR)
print("Text file:", text_path)
display(dbutils.fs.ls(LANDING_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Bronze: store the raw data
# MAGIC
# MAGIC Bronze rules:
# MAGIC - the data **as received**: every column as text, no renaming;
# MAGIC - three **lineage** columns: `_source_file`, `_source_url`, `_ingested_at`;
# MAGIC - `overwrite`: running again **replaces** the table (idempotent).
# MAGIC
# MAGIC The file is Windows-1252 encoded: `read_statbel_text()` detects it.
# MAGIC Expected result: **2,782 rows** (current and historical units).

# COMMAND ----------

raw = read_statbel_text(text_path)
write_table(add_lineage_columns(raw, SOURCE_URL), BRONZE)
display(spark.table(table_name(BRONZE)).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check: rows per level
# MAGIC
# MAGIC `LVL_REFNIS`: 1 region, 2 province, 3 district, 4 municipality (current
# MAGIC and former municipalities).

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT LVL_REFNIS, COUNT(*) AS row_count
# MAGIC FROM workspace.statbel.tutorial_bronze_ref_municipality
# MAGIC GROUP BY LVL_REFNIS
# MAGIC ORDER BY LVL_REFNIS

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Delta history: audit and time travel
# MAGIC
# MAGIC Every write creates a new **version** of the table. Delta keeps the
# MAGIC history: who, when, which operation, from which notebook (tab
# MAGIC **History** of the table in the Catalog).
# MAGIC
# MAGIC - `DESCRIBE HISTORY` lists the versions; `operationMetrics` gives for
# MAGIC   example `numOutputRows`.
# MAGIC - `VERSION AS OF 0` reads the **first** version, even after replacements.

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY workspace.statbel.tutorial_bronze_ref_municipality

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS rows_in_version_0
# MAGIC FROM workspace.statbel.tutorial_bronze_ref_municipality VERSION AS OF 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver: typed and cleaned data
# MAGIC
# MAGIC Decision of 2026-10-28: keep **only the units valid today**
# MAGIC (`DT_VLDT_END = 31/12/9999`). `CD_REFNIS` is then unique and can be
# MAGIC referenced by the other tables.
# MAGIC
# MAGIC | Rule | Example |
# MAGIC |---|---|
# MAGIC | Statbel column names are **kept** (glossary mapping exercise) | `CD_REFNIS` |
# MAGIC | `CD_*` codes stay **text** (leading zeros matter) | `02000` |
# MAGIC | `-` or empty becomes `NULL` | `CD_SUP_REFNIS` of the regions |
# MAGIC | Text dates become **dates** | `01/01/1970` → `1970-01-01` |
# MAGIC | The level becomes a **number** | `LVL_REFNIS` → `int` |
# MAGIC | `REF_PERIOD` = reference date of the file | `2025-01-01` |

# COMMAND ----------

silver = (
    spark.table(table_name(BRONZE))
    .filter(F.col("DT_VLDT_END") == CURRENT_END_DATE)
    .select(
        blank_to_null("CD_REFNIS").alias("CD_REFNIS"),
        blank_to_null("CD_SUP_REFNIS").alias("CD_SUP_REFNIS"),
        F.col("LVL_REFNIS").cast("int").alias("LVL_REFNIS"),
        blank_to_null("TX_REFNIS_FR").alias("TX_REFNIS_FR"),
        blank_to_null("TX_REFNIS_NL").alias("TX_REFNIS_NL"),
        blank_to_null("TX_REFNIS_DE").alias("TX_REFNIS_DE"),
        F.to_date("DT_VLDT_START", "dd/MM/yyyy").alias("DT_VLDT_START"),
        F.to_date("DT_VLDT_END", "dd/MM/yyyy").alias("DT_VLDT_END"),
        F.lit(REFERENCE_DATE).alias("REF_PERIOD"),
    )
)
write_table(silver, SILVER)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Quality checks
# MAGIC
# MAGIC - The primary key `CD_REFNIS` must be unique and never NULL.
# MAGIC - The hierarchy `CD_SUP_REFNIS` → `CD_REFNIS` must point to existing
# MAGIC   units (regions have no parent: NULL is not an orphan).
# MAGIC
# MAGIC Expected: **621 rows**, of which **565 municipalities** (situation after
# MAGIC the mergers of 1 January 2025), no duplicate, no orphan.

# COMMAND ----------

check_unique_key(SILVER, ["CD_REFNIS"])
display(count_orphans(SILVER, "CD_SUP_REFNIS", SILVER, "CD_REFNIS"))
display(
    spark.table(table_name(SILVER))
    .groupBy("LVL_REFNIS").count().orderBy("LVL_REFNIS")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Document the silver table
# MAGIC
# MAGIC - The table description ends with the licence line (added by
# MAGIC   `document_table()`).
# MAGIC - The primary key is **informational** (not enforced by Databricks, as in
# MAGIC   BigQuery): the quality checks above guarantee uniqueness.
# MAGIC - No Statbel field dictionary exists for this file: the column
# MAGIC   descriptions are written from the file header.
# MAGIC
# MAGIC ⚠️ Writing the silver table again recreates it and drops its keys; this
# MAGIC cell declares them again. It also drops (CASCADE) the foreign keys of
# MAGIC other tables pointing to `CD_REFNIS`: run notebook `03` again afterwards.

# COMMAND ----------

document_table(
    SILVER,
    description=(
        f"Belgian administrative units valid on {REFERENCE_DATE} "
        "(REFNIS hierarchy: region > province > district > municipality)."
    ),
    column_comments={
        "CD_REFNIS": "REFNIS code of the administrative unit",
        "CD_SUP_REFNIS": "REFNIS code of the parent unit (NULL for regions)",
        "LVL_REFNIS": (
            "Level: 1 region, 2 province, 3 district, 4 municipality"
        ),
        "TX_REFNIS_FR": "Name of the administrative unit in French",
        "TX_REFNIS_NL": "Name of the administrative unit in Dutch",
        "TX_REFNIS_DE": "Name of the administrative unit in German",
        "DT_VLDT_START": "First day of validity of the unit",
        "DT_VLDT_END": (
            "Last day of validity of the unit (9999-12-31: still valid)"
        ),
        "REF_PERIOD": "Reference date of the REFNIS list",
    },
    primary_key=["CD_REFNIS"],
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ Landing → bronze → silver for `ref_municipality`, with quality checks
# MAGIC and documentation in Unity Catalog.
# MAGIC
# MAGIC Next: tutorial `02_publish_ref_municipality` publishes the silver table
# MAGIC to BigQuery.
