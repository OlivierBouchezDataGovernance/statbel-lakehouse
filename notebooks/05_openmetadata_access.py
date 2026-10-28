# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Read-only access for OpenMetadata
# MAGIC
# MAGIC The OpenMetadata **Unity Catalog connector** harvests the tables of
# MAGIC this project: descriptions, keys, tags, owners, table and column
# MAGIC **lineage** (landing → bronze → silver), usage and profiles.
# MAGIC See `docs/openmetadata.md`.
# MAGIC
# MAGIC This notebook gives the Databricks identity used by OpenMetadata the
# MAGIC **read-only** rights listed in the OpenMetadata 2.0 documentation:
# MAGIC
# MAGIC | Purpose | Grants |
# MAGIC |---|---|
# MAGIC | metadata, profiler | `USE CATALOG`, `USE SCHEMA`, `SELECT` on the schema |
# MAGIC | usage | `system.query.history` |
# MAGIC | lineage | `system.access.table_lineage`, `system.access.column_lineage` |
# MAGIC | tags | `information_schema` tag views of the catalog |
# MAGIC
# MAGIC **Not granted:** any write right, and `READ VOLUME`: the volumes hold
# MAGIC the source files, your settings and the trainer's answer key.
# MAGIC
# MAGIC **Identity:** `openmetadata.principal` in `environment.yml` — the
# MAGIC application ID of a service principal (recommended) or a user e-mail.
# MAGIC Some grants on `system` tables need an account or metastore admin;
# MAGIC a refused grant is reported, the others still apply.
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %pip install pyyaml

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./lib/common

# COMMAND ----------

ENVIRONMENT = load_environment()
PRINCIPAL = ((ENVIRONMENT.get("openmetadata") or {}).get("principal")
             or "").strip()
if not PRINCIPAL:
    raise ValueError(
        "Set openmetadata.principal in environment.yml (application ID of "
        "a service principal, or a user e-mail), run 00_setup, then run "
        "this notebook again. See docs/openmetadata.md."
    )
print("Read-only access for:", PRINCIPAL)

# COMMAND ----------

GRANTS = [
    ("metadata", f"GRANT USE CATALOG ON CATALOG {CATALOG}"),
    ("metadata", f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA}"),
    ("metadata", f"GRANT SELECT ON SCHEMA {CATALOG}.{SCHEMA}"),
    ("usage", "GRANT USE SCHEMA ON SCHEMA system.query"),
    ("usage", "GRANT SELECT ON TABLE system.query.history"),
    ("lineage", "GRANT USE SCHEMA ON SCHEMA system.access"),
    ("lineage", "GRANT SELECT ON TABLE system.access.table_lineage"),
    ("lineage", "GRANT SELECT ON TABLE system.access.column_lineage"),
] + [
    ("tags", f"GRANT SELECT ON TABLE {CATALOG}.information_schema.{view}")
    for view in ("catalog_tags", "schema_tags", "table_tags", "column_tags")
]

results = []
for purpose, statement in GRANTS:
    sql = f"{statement} TO `{PRINCIPAL}`"
    try:
        spark.sql(sql)
        results.append((purpose, statement, "granted", None))
    except Exception as error:  # noqa: BLE001 - report and continue
        results.append((purpose, statement, "refused",
                        str(error).splitlines()[0][:300]))
display(spark.createDataFrame(
    results, "purpose STRING, grant STRING, status STRING, reason STRING"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check: rights of the principal on the schema

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS `{PRINCIPAL}` ON SCHEMA {CATALOG}.{SCHEMA}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connection details to enter in OpenMetadata
# MAGIC
# MAGIC *Settings › Services › Databases › Add New Service › Unity Catalog*.
# MAGIC The token (or the OAuth secret) is entered **in the OpenMetadata
# MAGIC interface only**, never in a file or in git.

# COMMAND ----------

host = spark.conf.get("spark.databricks.workspaceUrl", "<workspace host>")
warehouses = []
try:
    from databricks.sdk import WorkspaceClient
    warehouses = [(w.name, w.odbc_params.path)
                  for w in WorkspaceClient().warehouses.list()]
except Exception as error:  # noqa: BLE001 - SDK or rights not available
    print("SQL warehouses could not be listed:", error)

print(f"Host and Port : {host}:443")
for name, path in warehouses:
    print(f"HTTP Path     : {path}   ({name})")
print(f"Catalog       : {CATALOG}")
print(f"Database Schema: {SCHEMA}")
