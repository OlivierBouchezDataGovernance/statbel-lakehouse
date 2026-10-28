# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Setup — configuration and Databricks objects
# MAGIC
# MAGIC Run this notebook **once** (and again whenever a setting changes).
# MAGIC It:
# MAGIC
# MAGIC 1. creates the Unity Catalog schema and its three volumes
# MAGIC    (`landing`, `config`, `export`);
# MAGIC 2. writes **your** settings to `/Volumes/<catalog>/<schema>/config/environment.yml`
# MAGIC    — outside git, so the public repository never holds personal values;
# MAGIC 3. checks that the BigQuery key is in the Databricks secret scope;
# MAGIC 4. tests the connection to BigQuery;
# MAGIC 5. lists the Statbel tables configured in `config/datasets.yml`.
# MAGIC
# MAGIC **Before running it** (see `docs/install.md`):
# MAGIC - a Google Cloud project with a BigQuery dataset (location EU);
# MAGIC - a service account with *BigQuery Data Editor* on that dataset and
# MAGIC   *BigQuery Job User* on the project;
# MAGIC - its JSON key stored in a Databricks secret scope with the
# MAGIC   Databricks CLI (never in a notebook, a file or git).
# MAGIC
# MAGIC **How:** create `config/environment.yml` in your Git folder (or fill in
# MAGIC the widgets), then **Run all** — see section 1.
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %pip install pyyaml google-cloud-bigquery cryptography==46.0.3

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./lib/config

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Your settings
# MAGIC
# MAGIC Two ways to give them (the first one found is used):
# MAGIC
# MAGIC 1. **A file `config/environment.yml` in your Git folder** — copy of
# MAGIC    `config/environment.example.yml` with your values. This file is
# MAGIC    listed in `.gitignore`: it is **never committed** to GitHub.
# MAGIC    (Workspace → your Git folder → `config` → **Create** → **File**.)
# MAGIC 2. **The widgets** at the top of the notebook (`gcp_project_id`…),
# MAGIC    when your Databricks editor shows them.
# MAGIC
# MAGIC `catalog` and `schema` (created by `lib/config`) choose the Unity
# MAGIC Catalog location. On Databricks Free Edition the catalog is
# MAGIC `workspace`.

# COMMAND ----------

dbutils.widgets.text("gcp_project_id", "", "Google Cloud project ID")
dbutils.widgets.text("bq_dataset", "StatBel", "BigQuery dataset")
dbutils.widgets.dropdown("bq_location", "EU", ["EU", "US"],
                         "BigQuery location")
dbutils.widgets.text("secret_scope", "statbel", "Databricks secret scope")
dbutils.widgets.text("secret_key", "bigquery-sa-key", "Secret key name")
dbutils.widgets.text("max_table_size_gb", "1", "Max BigQuery table size (GB)")

local_file = os.path.join(find_project_root(), "config", "environment.yml")
extra_sections = {}  # other sections of the local file, kept as they are
if os.path.exists(local_file):
    local = load_yaml(local_file)
    extra_sections = {key: local[key] for key in
                      ("openmetadata", "statbel", "licence") if key in local}
    settings = {
        "gcp_project_id": local["bigquery"]["project_id"],
        "bq_dataset": local["bigquery"].get("dataset", "StatBel"),
        "bq_location": local["bigquery"].get("location", "EU"),
        "secret_scope": local["secrets"]["scope"],
        "secret_key": local["secrets"]["bigquery_key"],
        "max_table_size_gb": local["bigquery"].get("max_table_size_gb", 1),
    }
    print(f"Settings read from {local_file}")
else:
    settings = {
        name: dbutils.widgets.get(name).strip()
        for name in ("gcp_project_id", "bq_dataset", "bq_location",
                     "secret_scope", "secret_key", "max_table_size_gb")
    }
    print("Settings read from the widgets")
settings = {name: str(value).strip() for name, value in settings.items()}
if settings["gcp_project_id"] in ("", "my-gcp-project"):
    raise ValueError(
        "No Google Cloud project ID: create config/environment.yml in your "
        "Git folder (copy of environment.example.yml with your values) or "
        "fill in the widget 'gcp_project_id', then run the notebook again."
    )
print(f"Unity Catalog: {CATALOG}.{SCHEMA}")
for name, value in settings.items():
    print(f"  {name:<18} {value}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema and volumes
# MAGIC
# MAGIC `IF NOT EXISTS`: running again creates nothing twice and deletes
# MAGIC nothing.
# MAGIC
# MAGIC | Volume | Content |
# MAGIC |---|---|
# MAGIC | `landing` | Statbel files, unchanged (zip, text, GeoJSON, dictionaries) |
# MAGIC | `config` | `environment.yml` (your settings) |
# MAGIC | `export` | Parquet files sent to BigQuery |

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA} COMMENT "
          "'Statbel open data lakehouse: landing, bronze, silver, metadata'")
for volume, comment in {
    "landing": "Statbel files stored unchanged",
    "config": "Environment settings of the pipeline (outside git)",
    "export": "Parquet files exported to BigQuery",
}.items():
    spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{volume} "
              f"COMMENT '{comment}'")
display(spark.sql(f"SHOW VOLUMES IN {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Write `environment.yml`
# MAGIC
# MAGIC The file follows `config/environment.example.yml` of the repository.
# MAGIC It holds **no secret**: only the *name* of the secret scope.

# COMMAND ----------

template = load_yaml(
    os.path.join(find_project_root(), "config", "environment.example.yml"))
environment = {
    **template,
    **extra_sections,
    "databricks": {"catalog": CATALOG, "schema": SCHEMA},
    "bigquery": {
        "project_id": settings["gcp_project_id"],
        "dataset": settings["bq_dataset"],
        "location": settings["bq_location"],
        "max_table_size_gb": float(settings["max_table_size_gb"]),
    },
    "secrets": {
        "scope": settings["secret_scope"],
        "bigquery_key": settings["secret_key"],
    },
}
with open(ENVIRONMENT_FILE, "w", encoding="utf-8") as environment_file:
    yaml.safe_dump(environment, environment_file, sort_keys=False,
                   allow_unicode=True)
print(f"Written: {ENVIRONMENT_FILE}\n")
print(open(ENVIRONMENT_FILE, encoding="utf-8").read())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Is the BigQuery key in the secret scope?
# MAGIC
# MAGIC Only the **names** of the secrets are listed, never their values.
# MAGIC If the check fails, create the scope and the secret from your own
# MAGIC terminal (`docs/install.md`, step "Store the key"):
# MAGIC
# MAGIC ```
# MAGIC databricks secrets create-scope statbel
# MAGIC databricks secrets put-secret statbel bigquery-sa-key < key.json
# MAGIC ```

# COMMAND ----------

scope, key = settings["secret_scope"], settings["secret_key"]
scopes = [s.name for s in dbutils.secrets.listScopes()]
if scope not in scopes:
    raise ValueError(f"Secret scope '{scope}' not found (scopes: {scopes}).")
keys = [s.key for s in dbutils.secrets.list(scope)]
if key not in keys:
    raise ValueError(f"Secret '{key}' not found in scope '{scope}'.")
print(f"OK: secret '{key}' found in scope '{scope}'.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Test the connection to BigQuery
# MAGIC
# MAGIC Expected: the service account e-mail, the dataset and its location.

# COMMAND ----------

# MAGIC %run ./lib/bigquery

# COMMAND ----------

client = get_bigquery_client()
dataset = client.get_dataset(f"{PROJECT_ID}.{DATASET}")
account = list(client.query("SELECT SESSION_USER() AS account").result())[0]
print("Connected as:", account["account"])
print("Dataset:     ", dataset.full_dataset_id,
      "| location:", dataset.location)
if dataset.location != LOCATION:
    print(f"WARNING: the dataset is in {dataset.location}, "
          f"not in {LOCATION}.")
print("Tables:      ", sorted(published_tables()) or "none yet")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Tables configured in `config/datasets.yml`
# MAGIC
# MAGIC Switch a table on or off with `enabled: true/false` in the file (in
# MAGIC the Git folder), or give an explicit list in the `tables` widget of
# MAGIC notebooks 02 and 03.

# COMMAND ----------

display(spark.createDataFrame([
    (t["table"], t["domain"], bool(t["enabled"]), str(t["period"]),
     len(t["sources"]), ", ".join(t["primary_key"]),
     len(t["foreign_keys"]), t["title"])
    for t in load_datasets()
], "table STRING, domain STRING, enabled BOOLEAN, period STRING, "
   "sources INT, primary_key STRING, foreign_keys INT, title STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC
# MAGIC 1. `01_statbel_catalog` — Statbel catalog and field dictionaries
# MAGIC    → metadata tables `meta_*` (used for the column descriptions).
# MAGIC 2. `02_load_statbel` — landing → bronze → silver for the enabled tables.
# MAGIC 3. `03_publish_bigquery` — silver → BigQuery, keys and run report.
