# Databricks notebook source
# MAGIC %md
# MAGIC # Tutorial 03 · Reference data: first names (`ref_first_name`)
# MAGIC
# MAGIC **Tutorial.** This notebook builds one table **by hand**, step by step, to
# MAGIC learn the method. Its tables are prefixed `tutorial_` so they never
# MAGIC overwrite the tables of the pipeline (notebooks `02` and `03`).
# MAGIC Run `00_setup` first.
# MAGIC
# MAGIC Two Statbel files (female and male first names by municipality) are
# MAGIC merged into **one** table, with a column `CD_SEX`. New in this notebook:
# MAGIC
# MAGIC - two sources merged with `unionByName`;
# MAGIC - a **composite** primary key (`CD_REFNIS`, `CD_SEX`, `TX_FST_NAME`);
# MAGIC - a **foreign key** to `ref_municipality`, with an orphan count;
# MAGIC - publishing to BigQuery with the shared function `publish_table()`.
# MAGIC
# MAGIC **Prerequisite:** tutorials `01` and `02` have run (the foreign key
# MAGIC points to its table). **Licence:** CC BY 4.0.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Libraries and utilities
# MAGIC
# MAGIC The BigQuery library is installed first: the Python restart erases all
# MAGIC variables, so it must happen before any other cell.

# COMMAND ----------

# MAGIC %pip install google-cloud-bigquery "cryptography==46.0.3" pyyaml

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ../lib/bigquery

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

BASE_URL = (
    "https://statbel.fgov.be/sites/default/files/files/opendata/"
    "Voornamen%20bevolking%20per%20gemeente"
)
SOURCES = {  # sex code -> (Statbel file, bronze table)
    "F": ("TA_POP_FEMALE_2026.zip", "tutorial_bronze_ref_first_name_female"),
    "M": ("TA_POP_MALE_2026.zip", "tutorial_bronze_ref_first_name_male"),
}
LANDING_DIR = f"{LANDING_VOLUME}/tutorial"
SILVER = "tutorial_silver_ref_first_name"
PRIMARY_KEY = ["CD_REFNIS", "CD_SEX", "TX_FST_NAME"]
REFERENCE_DATE = "2026-01-01"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Landing and bronze: one bronze table per source file
# MAGIC
# MAGIC Lesson learned: these files look like UTF-8 at the start but are
# MAGIC **Windows-1252**. Read as UTF-8, accented names were damaged
# MAGIC (`Irène` → `Ir�ne`) and created **375 false duplicate keys**.
# MAGIC `read_statbel_text()` now detects the encoding on the whole file.
# MAGIC
# MAGIC Expected: 223,988 female and 202,890 male rows.

# COMMAND ----------

for sex, (file_name, bronze_name) in SOURCES.items():
    source_url = f"{BASE_URL}/{file_name}"
    text_path = download_zip(source_url, LANDING_DIR)
    write_table(
        add_lineage_columns(read_statbel_text(text_path), source_url),
        bronze_name,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver: merge the two files
# MAGIC
# MAGIC - `CD_SEX` is a **derived** column: it does not exist in the files, it
# MAGIC   comes from the file name. Its description says so.
# MAGIC - `tx_descr_nl` and `tx_descr_fr` are lower case in the files: they are
# MAGIC   written in upper case, like the other Statbel names.
# MAGIC - `unionByName` stacks the two results by column **name**, not position.

# COMMAND ----------

def bronze_to_silver(bronze_name, sex):
    """Return the silver columns of one first-name bronze table."""
    return spark.table(table_name(bronze_name)).select(
        blank_to_null("CD_REFNIS").alias("CD_REFNIS"),
        F.lit(sex).alias("CD_SEX"),
        blank_to_null("tx_descr_nl").alias("TX_DESCR_NL"),
        blank_to_null("tx_descr_fr").alias("TX_DESCR_FR"),
        blank_to_null("TX_FST_NAME").alias("TX_FST_NAME"),
        F.col("MS_FREQUENCY").cast("int").alias("MS_FREQUENCY"),
        F.lit(REFERENCE_DATE).alias("REF_PERIOD"),
    )


female = bronze_to_silver(SOURCES["F"][1], "F")
male = bronze_to_silver(SOURCES["M"][1], "M")
write_table(female.unionByName(male), SILVER)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Quality checks
# MAGIC
# MAGIC Expected: 426,878 rows, no duplicate key, no NULL key, no orphan
# MAGIC municipality.

# COMMAND ----------

check_unique_key(SILVER, PRIMARY_KEY)
orphans = count_orphans(SILVER, "CD_REFNIS",
                        "tutorial_silver_ref_municipality", "CD_REFNIS")
display(orphans.groupBy("CD_REFNIS", "TX_DESCR_FR").count()
        .orderBy("CD_REFNIS"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Document the silver table and declare its keys
# MAGIC
# MAGIC No Statbel field dictionary exists for these files: the column
# MAGIC descriptions are written from the file header. The foreign key links
# MAGIC this table to `tutorial_silver_ref_municipality` (visible with *View
# MAGIC relationships* in the Catalog).

# COMMAND ----------

document_table(
    SILVER,
    description=(
        f"First names of the population by municipality and sex on "
        f"{REFERENCE_DATE} (female and male files merged, column CD_SEX)."
    ),
    column_comments={
        "CD_REFNIS": "REFNIS code of the municipality",
        "CD_SEX": "Sex: F female, M male (derived from the source file name)",
        "TX_DESCR_NL": "Municipality name in Dutch",
        "TX_DESCR_FR": "Municipality name in French",
        "TX_FST_NAME": "First name",
        "MS_FREQUENCY": (
            "Number of residents of the municipality with this first name "
            "and sex"
        ),
        "REF_PERIOD": "Reference date of the figures",
    },
    primary_key=PRIMARY_KEY,
)
declare_foreign_key(SILVER, "CD_REFNIS", "tutorial_silver_ref_municipality",
                    "CD_REFNIS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Publish to BigQuery
# MAGIC
# MAGIC Expected: 426,878 rows, about 24 MB, composite primary key and one
# MAGIC foreign key to `ref_municipality` (which must already be published by
# MAGIC notebook `02`).

# COMMAND ----------

publish_table(
    source_name=SILVER,
    target_table="tutorial_ref_first_name",
    labels=make_labels(domain="ref", period=REFERENCE_DATE),
)
declare_bigquery_foreign_key("tutorial_ref_first_name", "CD_REFNIS",
                             "tutorial_ref_municipality", "CD_REFNIS")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ `ref_first_name` in Databricks and BigQuery, linked to
# MAGIC `ref_municipality` by a declared foreign key, with 0 orphan.
