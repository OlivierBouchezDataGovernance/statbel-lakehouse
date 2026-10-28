# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Governance tags in Unity Catalog
# MAGIC
# MAGIC Writes **tags** on the schema, the silver tables and their columns.
# MAGIC Unity Catalog makes them searchable (Catalog Explorer, `Discover`) and
# MAGIC usable by policies; **OpenMetadata ingests them** with the Unity
# MAGIC Catalog connector (option *Include Tags*).
# MAGIC
# MAGIC | Level | Tags | Source |
# MAGIC |---|---|---|
# MAGIC | schema | `source`, `licence`, `sensitivity` | `governance.table_tags` of `config/datasets.yml` |
# MAGIC | silver table | `domain` (business domain of DG-GDL-001), `source`, `licence`, `sensitivity`, `layer`, `personal_data`, `period` | `governance` section + table settings |
# MAGIC | column | `representation`: code, label, measure, date, flag, level, coordinate, geometry, reference_period | Statbel prefix of the column name |
# MAGIC | bronze / metadata tables | `layer`, `source` | fixed |
# MAGIC
# MAGIC **What is deliberately *not* tagged: the business term of each
# MAGIC column.** Linking columns to the glossary is the exercise of the
# MAGIC training (OpenMetadata). A tag with the term would give the answer.
# MAGIC
# MAGIC **Personal data.** Statbel open data is aggregated (counts published
# MAGIC from 5 occurrences): `personal_data = none`. Addresses are tagged
# MAGIC `potential` (an address can identify a household). Discussing this
# MAGIC classification is part of the training.
# MAGIC
# MAGIC **Prerequisite:** `02_load_statbel` has run. Running again replaces
# MAGIC the tags (no duplicates).
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./lib/governance

# COMMAND ----------

GOVERNANCE = load_governance()
ALL_TABLES = load_datasets()
SCHEMA_TAGS = {key: value
               for key, value in (GOVERNANCE.get("table_tags") or {}).items()
               if key in ("source", "licence", "sensitivity")}

spark.sql(f"ALTER SCHEMA {CATALOG}.{SCHEMA} "
          f"SET TAGS ({_tag_list(SCHEMA_TAGS)})")
print(f"{CATALOG}.{SCHEMA}: {SCHEMA_TAGS}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver tables and their columns

# COMMAND ----------

summary = []
for table in ALL_TABLES:
    name = f"silver_{table['table']}"
    if not table_exists(name):
        continue
    tags = table_tags(table, GOVERNANCE)
    set_tags(name, tags)
    columns = spark.table(table_name(name)).columns
    for column in columns:
        set_tags(name, {"representation": representation(column)}, column)
    summary.append((table["table"], tags.get("domain"), tags["personal_data"],
                    len(columns)))
    print(f"{name}: {len(tags)} table tag(s), {len(columns)} column tag(s)")

display(spark.createDataFrame(
    summary, "table STRING, domain STRING, personal_data STRING, "
             "tagged_columns INT"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze, metadata and quality tables

# COMMAND ----------

other_tags = {
    "bronze_": {"layer": "bronze", "source": "statbel"},
    "meta_": {"layer": "metadata", "source": "statbel-dcat"},
    "run_report": {"layer": "quality", "source": "pipeline"},
}
for row in spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect():
    for prefix, tags in other_tags.items():
        if row.tableName.startswith(prefix):
            set_tags(row.tableName, tags)
            print(f"{row.tableName}: {tags}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check: tags as seen by Unity Catalog (and by OpenMetadata)

# COMMAND ----------

display(spark.sql(f"""
    SELECT table_name, tag_name, tag_value
    FROM {CATALOG}.information_schema.table_tags
    WHERE schema_name = '{SCHEMA}' AND table_name LIKE 'silver_%'
    ORDER BY table_name, tag_name"""))

# COMMAND ----------

display(spark.sql(f"""
    SELECT tag_value AS representation, COUNT(*) AS columns
    FROM {CATALOG}.information_schema.column_tags
    WHERE schema_name = '{SCHEMA}' AND tag_name = 'representation'
    GROUP BY tag_value ORDER BY columns DESC"""))
