# Databricks notebook source
# MAGIC %md
# MAGIC # 08 · Glossary: OpenMetadata import files
# MAGIC
# MAGIC Generates the OpenMetadata 2.0 import files of the business glossary
# MAGIC (5 glossaries, 127 terms, guideline DG-GDL-001) from **one source
# MAGIC file**, `glossary/terms.yml`:
# MAGIC
# MAGIC ```
# MAGIC glossary/terms.yml ──┐
# MAGIC                      ├─ glossary/generator.py ─→ pass1-terms/*.csv
# MAGIC meta_field (01) ─────┘                          pass2-related-terms/*.csv
# MAGIC  Statbel column names                           setup.json, glossary_master.csv
# MAGIC ```
# MAGIC
# MAGIC | Output | Content |
# MAGIC |---|---|
# MAGIC | `pass1-terms/` | one CSV per glossary, every term without related terms |
# MAGIC | `pass2-related-terms/` | the terms that have related terms (imported second) |
# MAGIC | `setup.json` | `TermType` tags, domains and glossaries to create before the import |
# MAGIC | `glossary_master.csv` | one row per term with the Statbel source columns it explains |
# MAGIC | `stats.json` | counts: terms, columns, matches |
# MAGIC
# MAGIC **How to change the glossary:** edit `glossary/terms.yml` in the Git
# MAGIC folder, run this notebook, check the differences (section 3), then
# MAGIC copy the files to the repository (section 4) and commit them.
# MAGIC
# MAGIC **Source columns.** Each term may have a `source_pattern` (regular
# MAGIC expression) matched against the Statbel column names: by default the
# MAGIC names of the table `meta_field` (notebook `01_statbel_catalog`), or
# MAGIC the snapshot `glossary/statbel_columns.json` of the repository.
# MAGIC
# MAGIC The same generator runs on a laptop:
# MAGIC `python glossary/generator.py` (see `glossary/README.md`).
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./lib/common

# COMMAND ----------

import filecmp
import shutil
import sys

GLOSSARY_DIR = os.path.join(find_project_root(), "glossary")
REPOSITORY_OUT = os.path.join(GLOSSARY_DIR, "openmetadata-import")
VOLUME_OUT = f"{EXPORT_VOLUME}/glossary"

if GLOSSARY_DIR not in sys.path:
    sys.path.insert(0, GLOSSARY_DIR)
import generator  # noqa: E402 - glossary/generator.py of the repository

dbutils.widgets.dropdown("columns", "meta_field", ["meta_field", "snapshot"],
                         "Statbel column names")
dbutils.widgets.dropdown("copy_to_repository", "false", ["false", "true"],
                         "Copy the files to the Git folder")
COLUMNS = dbutils.widgets.get("columns")
COPY_TO_REPOSITORY = dbutils.widgets.get("copy_to_repository") == "true"
print(f"Glossary: {generator.DEFAULT_TERMS}")
print(f"Columns:  {COLUMNS} | output: {VOLUME_OUT}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Statbel column names
# MAGIC
# MAGIC One set of column names per field dictionary. With `meta_field`, the
# MAGIC names are those read by notebook 01 in the current Statbel catalog;
# MAGIC with `snapshot`, those used for the files of the repository.

# COMMAND ----------

if COLUMNS == "meta_field":
    if not table_exists("meta_field"):
        raise FileNotFoundError("meta_field not found: run 01_statbel_catalog"
                                " or choose columns = snapshot")
    rows = (spark.table(table_name("meta_field"))
            .select("DICTIONARY_URL", "FIELD_NAME").collect())
    columns_by_dictionary = generator.normalise_columns(
        (row.DICTIONARY_URL, row.FIELD_NAME) for row in rows)
else:
    columns_by_dictionary = generator.load_column_snapshot()
print(f"{len(columns_by_dictionary)} dictionaries, "
      f"{len({c for s in columns_by_dictionary.values() for c in s})} "
      "distinct column names")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate the files
# MAGIC
# MAGIC `terms.yml` is checked first: unknown keys, glossaries, types or
# MAGIC references, duplicate terms, and parents or related terms that do not
# MAGIC exist stop the notebook with a clear message.

# COMMAND ----------

if os.path.exists(VOLUME_OUT):
    shutil.rmtree(VOLUME_OUT)
model, stats, written = generator.generate(columns_by_dictionary, VOLUME_OUT)

display(spark.createDataFrame(written, "pass STRING, file STRING, terms INT"))
print({key: value for key, value in stats.items()
       if not isinstance(value, dict)})

# COMMAND ----------

# MAGIC %md
# MAGIC ### Terms and their Statbel source columns

# COMMAND ----------

display(spark.createDataFrame(
    [(t["glossary_name"], t["fqn"], t["type"], t["dictionaries"],
      len(t["source_columns"]), ", ".join(t["source_columns"]))
     for t in model["terms"]],
    "glossary STRING, term STRING, type STRING, dictionaries INT, "
    "source_columns INT, columns STRING",
))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Differences with the files of the repository
# MAGIC
# MAGIC `changed` files must be imported again in OpenMetadata (same two
# MAGIC passes). With `columns = meta_field`, differences can also come from
# MAGIC a newer Statbel catalog: new columns found for a term.

# COMMAND ----------

comparison = []
for folder, file_name, _ in written:
    generated = os.path.join(VOLUME_OUT, folder, file_name)
    committed = os.path.join(REPOSITORY_OUT, folder, file_name)
    if not os.path.exists(committed):
        status = "new"
    elif filecmp.cmp(generated, committed, shallow=False):
        status = "unchanged"
    else:
        status = "changed"
    comparison.append((folder, file_name, status))
display(spark.createDataFrame(comparison,
                              "pass STRING, file STRING, status STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Copy to the repository (optional)
# MAGIC
# MAGIC With the widget `copy_to_repository = true`, the generated files
# MAGIC replace those of `glossary/openmetadata-import` in the Git folder.
# MAGIC Then open the Git dialog (branch name at the top), check the changes
# MAGIC and **Commit & Push**. Otherwise download them from *Catalog ›
# MAGIC workspace › statbel › Volumes › export › glossary*.

# COMMAND ----------

if COPY_TO_REPOSITORY:
    for folder, _ in generator.PASSES:
        target = os.path.join(REPOSITORY_OUT, folder)
        if os.path.exists(target):
            shutil.rmtree(target)
        shutil.copytree(os.path.join(VOLUME_OUT, folder), target)
    for file_name in ("setup.json", "glossary_master.csv", "stats.json"):
        shutil.copy(os.path.join(VOLUME_OUT, file_name), REPOSITORY_OUT)
    print(f"Copied to {REPOSITORY_OUT}: commit and push from the Git dialog.")
else:
    print(f"Files in {VOLUME_OUT} (repository unchanged).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC Import in OpenMetadata, glossary by glossary, starting with the
# MAGIC *Shared Business Vocabulary* (see `glossary/README.md`):
# MAGIC
# MAGIC 1. create what `setup.json` lists: classification `TermType` and its
# MAGIC    tags, domains, glossaries (no reviewers yet);
# MAGIC 2. **pass 1**: glossary › ⋮ › **Import** › file of `pass1-terms`;
# MAGIC 3. **pass 2**: same, files of `pass2-related-terms`;
# MAGIC 4. add the reviewers and run the approval workflow.
