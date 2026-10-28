# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Trainer's answer key: columns ↔ business terms
# MAGIC
# MAGIC For every **silver column**, the business term it most likely
# MAGIC represents, from the glossary of `glossary/openmetadata-import`
# MAGIC (127 terms, guideline DG-GDL-001). Each term lists, as mapping
# MAGIC evidence, the Statbel source columns it explains.
# MAGIC
# MAGIC | Match | Rule |
# MAGIC |---|---|
# MAGIC | `exact` | the column name is listed in the term's source columns |
# MAGIC | `concept` | same name once the technical noise is removed (DG-GDL-001 step 2: `CD_`/`TX_` prefixes, `_FR`/`_NL` suffixes, `_LVL_n`, `DESCR`): `TX_NATLTY_FR` → `NATLTY` |
# MAGIC | `project` | column added by the pipeline (`REF_PERIOD` → *Reference Period*) |
# MAGIC | `unmapped` | no term: a technical column, or a gap of the glossary |
# MAGIC
# MAGIC **This is the answer key of the training exercise.** It is written to
# MAGIC the volume `training` (CSV and Markdown), **not** to a table, not to
# MAGIC BigQuery and not to git: OpenMetadata cannot read it (notebook 05
# MAGIC grants no access to the volumes) and participants cannot find it.
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./lib/governance

# COMMAND ----------

import collections
import csv
import glob

GLOSSARY_DIR = os.path.join(find_project_root(), "glossary",
                            "openmetadata-import", "pass1-terms")
TRAINING_DIR = f"{VOLUME_ROOT}/training"
SOURCE_COLUMNS = re.compile(r"\*\*Source columns[^*]*\*\*\s*(.*)", re.S)
FIELD = re.compile(r"\b[A-Z][A-Z0-9]*(?:_[A-Z0-9]+)+\b")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the glossary

# COMMAND ----------

def read_terms(folder):
    """Return the terms of the pass-1 import files (one dict per term)."""
    terms = []
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        glossary = re.sub(r"^\d+-", "", os.path.basename(path)[:-4])
        glossary = glossary.replace("_", " ")
        with open(path, encoding="utf-8-sig") as csv_file:
            for row in csv.DictReader(csv_file):
                parent = row["parent"] or glossary
                tags = [t.strip() for t in row["tags"].split(";") if t]
                evidence = SOURCE_COLUMNS.search(row["description"])
                terms.append({
                    "glossary": glossary,
                    "term": f"{parent}.{row['name*']}",
                    "name": row["name*"],
                    "term_type": next((t.split(".", 1)[1] for t in tags
                                       if t.startswith("TermType.")), ""),
                    "personal_data": "PersonalData.Personal" in tags,
                    "source_columns": sorted(set(
                        FIELD.findall(evidence.group(1)) if evidence else [])),
                })
    return terms


TERMS = read_terms(GLOSSARY_DIR)
print(f"{len(TERMS)} terms, "
      f"{sum(1 for t in TERMS if t['source_columns'])} with source columns")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Match the silver columns

# COMMAND ----------

exact_index = collections.defaultdict(set)
concept_index = collections.defaultdict(set)
for term in TERMS:
    for column in term["source_columns"]:
        exact_index[column].add(term["term"])
        concept_index[concept_key(column)].add(term["term"])
by_term = {term["term"]: term for term in TERMS}

columns = spark.sql(f"""
    SELECT table_name, column_name, data_type, comment
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema = '{SCHEMA}' AND table_name LIKE 'silver_%'
    ORDER BY table_name, ordinal_position""").collect()

# REF_PERIOD is added by the pipeline (not a Statbel column): it carries
# the reference period of the data.
project_columns = {
    "REF_PERIOD": {t["term"] for t in TERMS
                   if t["name"] in ("Reference Period", "Reference Date")},
}

answer_key = []
for row in columns:
    found, match = exact_index.get(row.column_name), "exact"
    if not found and project_columns.get(row.column_name):
        found, match = project_columns[row.column_name], "project"
    if not found:
        found, match = concept_index.get(concept_key(row.column_name)), \
            "concept"
    found = sorted(found or [])
    answer_key.append({
        "table": row.table_name.removeprefix("silver_"),
        "column": row.column_name,
        "type": row.data_type,
        "description": row.comment or "",
        "suggested_terms": "; ".join(found) or "unmapped",
        "match": match if found else "unmapped",
        "term_types": "; ".join(sorted({by_term[t]["term_type"]
                                        for t in found})),
        "personal_data": any(by_term[t]["personal_data"] for t in found),
    })
print(collections.Counter(row["match"] for row in answer_key))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write the answer key and its summary (volume `training`)

# COMMAND ----------

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.training "
          "COMMENT 'Trainer material (answer key): not for participants'")
os.makedirs(TRAINING_DIR, exist_ok=True)
reference_file = f"{TRAINING_DIR}/mapping_reference.csv"
with open(reference_file, "w", encoding="utf-8", newline="") as target:
    writer = csv.DictWriter(target, fieldnames=list(answer_key[0]))
    writer.writeheader()
    writer.writerows(answer_key)

per_table = collections.defaultdict(lambda: [0, 0])
for row in answer_key:
    per_table[row["table"]][0] += 1
    per_table[row["table"]][1] += row["match"] != "unmapped"
unmapped = collections.Counter(
    row["column"] for row in answer_key if row["match"] == "unmapped")
mapped_total = sum(mapped for _, mapped in per_table.values())

lines = [
    "# Training answer key: summary", "",
    f"{len(answer_key)} silver columns, {mapped_total} with a suggested "
    f"term ({mapped_total / len(answer_key):.0%}).", "",
    "| Table | Columns | With a term | Share |", "|---|---|---|---|",
]
lines += [f"| {table} | {total} | {mapped} | {mapped / total:.0%} |"
          for table, (total, mapped) in sorted(per_table.items())]
lines += ["", "## 20 most frequent unmapped column names", ""]
lines += [f"- `{name}` ({count})" for name, count in unmapped.most_common(20)]
lines += ["", "## Foreign keys (config/datasets.yml)", ""]
lines += [f"- `{t['table']}.{k['column']}` → "
          f"`{k['references']}.{k['referenced_column']}`"
          for t in load_datasets() for k in t["foreign_keys"]]
summary_file = f"{TRAINING_DIR}/mapping_summary.md"
with open(summary_file, "w", encoding="utf-8") as target:
    target.write("\n".join(lines) + "\n")
print(f"Written: {reference_file}\n         {summary_file}")

# COMMAND ----------

display(spark.createDataFrame(
    [(t, total, mapped, round(mapped / total, 2))
     for t, (total, mapped) in sorted(per_table.items())],
    "table STRING, columns INT, with_term INT, share DOUBLE"))
