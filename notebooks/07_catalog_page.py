# Databricks notebook source
# MAGIC %md
# MAGIC # 07 · Catalog page: the Statbel catalog as one web page
# MAGIC
# MAGIC Builds a **self-contained web page** (one HTML file, no server) from
# MAGIC the metadata tables of notebook `01_statbel_catalog`:
# MAGIC
# MAGIC - the list of the Statbel datasets with their files, period and themes;
# MAGIC - a search on titles, file names **and field names** (e.g. `CD_REFNIS`);
# MAGIC - for each dataset: description, files to download, **field
# MAGIC   descriptions** (EN/FR/NL/DE) and documentation PDFs;
# MAGIC - metadata in French, Dutch, English or German.
# MAGIC
# MAGIC The page is written to the volume `catalog`: download it from
# MAGIC *Catalog › workspace › statbel › Volumes › catalog* (⋮ › Download) and
# MAGIC open it in a browser, or publish it on a website.
# MAGIC
# MAGIC **Prerequisite:** `01_statbel_catalog` has run.
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./lib/common

# COMMAND ----------

import collections
import datetime
import json
import re

ASSETS = os.path.join(find_project_root(), "notebooks", "assets")
TEMPLATE = os.path.join(ASSETS, "catalog_template.html")
THEMES = load_yaml(os.path.join(ASSETS, "eu_data_themes.yml"))
OUTPUT_DIR = f"{VOLUME_ROOT}/catalog"
LANGUAGES = ("en", "fr", "nl", "de")

for name in ("meta_dataset", "meta_distribution", "meta_document",
             "meta_field"):
    if not table_exists(name):
        raise FileNotFoundError(f"{name} not found: run 01_statbel_catalog")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read the metadata tables

# COMMAND ----------

def rows_by_dataset(name):
    """Rows of a meta table, grouped by DATASET_ID."""
    grouped = collections.defaultdict(list)
    for row in spark.table(table_name(name)).collect():
        grouped[row.DATASET_ID].append(row.asDict())
    return grouped


def licence(url):
    """https://creativecommons.org/licenses/by/4.0/ -> CC BY 4.0."""
    if not url:
        return None
    match = re.search(r"creativecommons\.org/licenses/([\w-]+)/([\d.]+)",
                      url)
    label = (f"CC {match.group(1).upper()} {match.group(2)}" if match
             else url.rstrip("/").rsplit("/", 1)[-1])
    return {"label": label, "url": url}


files = rows_by_dataset("meta_distribution")
documents = rows_by_dataset("meta_document")
datasets = []
for row in spark.table(table_name("meta_dataset")).collect():
    language_texts = {
        field: {lang: row[f"{field.upper()}_{lang.upper()}"]
                for lang in LANGUAGES
                if row[f"{field.upper()}_{lang.upper()}"]}
        for field in ("title", "description")
    }
    datasets.append({
        "id": row.DATASET_ID,
        "uri": row.DATASET_URI,
        "title": language_texts["title"],
        "alternative": {},
        "description": language_texts["description"],
        "keywords": {},
        "themes": [t for t in (row.THEMES or "").split(",") if t],
        "hvd": bool(row.HIGH_VALUE),
        "license": licence(row.LICENCE),
        "start": row.PERIOD_START,
        "end": row.PERIOD_END,
        "landing": {lang: url for lang, url in
                    (("en", row.LANDING_PAGE_EN), ("fr", row.LANDING_PAGE_FR))
                    if url},
        "files": sorted(
            ({"name": f["FILE_NAME"], "url": f["DOWNLOAD_URL"],
              "format": f["FILE_FORMAT"], "compressed": f["COMPRESSION"]}
             for f in files[row.DATASET_ID]),
            key=lambda f: (f["name"].lower(), f["format"] or "")),
        "docs": [
            {"url": d["DOCUMENT_URL"], "name": d["DOCUMENT_NAME"],
             "lang": d["LANGUAGE"], "kind": d["DOCUMENT_TYPE"]}
            for d in documents[row.DATASET_ID]
        ],
    })
datasets.sort(key=lambda d: (d["title"].get("fr")
                             or next(iter(d["title"].values()), "")).lower())
print(f"{len(datasets)} datasets, {sum(len(d['files']) for d in datasets)} "
      "files")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Field descriptions: one table per field dictionary

# COMMAND ----------

columns = [f"DESCRIPTION_{lang.upper()}" for lang in LANGUAGES + ("und",)]
fields = collections.defaultdict(list)
for row in spark.table(table_name("meta_field")).orderBy(
        "DICTIONARY_URL", "FIELD_NAME").collect():
    fields[row.DICTIONARY_URL].append(
        [row.FIELD_NAME] + [row[c] or "" for c in columns])

dictionaries = {}
for url, rows in fields.items():
    used = [i for i in range(len(columns)) if any(r[i + 1] for r in rows)]
    headers = ["Field"] + [columns[i].split("_")[1] for i in used]
    dictionaries[url] = {"sheets": [{
        "name": "Fields",
        "headers": headers,
        "rows": [[r[0]] + [r[i + 1] for i in used] for r in rows],
        "total": len(rows),
        "key": 0,
    }]}
print(f"{len(dictionaries)} field dictionaries, "
      f"{sum(len(r) for r in fields.values())} fields")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write the page

# COMMAND ----------

payload = {
    "catalog": {"title": {"en": "Statbel open data"},
                "modified": datetime.date.today().isoformat()},
    "generated": datetime.date.today().isoformat(),
    "themes": THEMES,
    "datasets": datasets,
    "docs": dictionaries,
}
# "</" is escaped so that the JSON cannot close the <script> element
data = json.dumps(payload, ensure_ascii=False,
                  separators=(",", ":")).replace("</", "<\\/")
with open(TEMPLATE, encoding="utf-8") as template_file:
    page = template_file.read().replace("__CATALOG_DATA__", data)

spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.catalog "
          "COMMENT 'Generated web page of the Statbel catalog'")
os.makedirs(OUTPUT_DIR, exist_ok=True)
output = f"{OUTPUT_DIR}/index.html"
with open(output, "w", encoding="utf-8") as page_file:
    page_file.write(page)
print(f"Written: {output} ({os.path.getsize(output) / 1e6:.1f} MB)")
