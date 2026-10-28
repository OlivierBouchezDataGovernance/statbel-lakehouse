# Databricks notebook source
# MAGIC %md
# MAGIC # lib · config — project configuration
# MAGIC
# MAGIC Included by every notebook with `%run ./lib/config` (or
# MAGIC `%run ../lib/config` from `tutorial/`).
# MAGIC
# MAGIC | Source | Where | Content |
# MAGIC |---|---|---|
# MAGIC | widgets `catalog`, `schema` | top of the notebook | Unity Catalog location of the project (default `workspace.statbel`) |
# MAGIC | `environment.yml` | volume `/Volumes/<catalog>/<schema>/config/` | Google Cloud project, BigQuery dataset, secret scope (written by `00_setup`, **never in git**) |
# MAGIC | `config/datasets.yml` | this repository | the Statbel tables to load and how |
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

import copy
import os

import yaml

dbutils.widgets.text("catalog", "workspace", "Unity Catalog catalog")
dbutils.widgets.text("schema", "statbel", "Unity Catalog schema")
CATALOG = dbutils.widgets.get("catalog").strip()
SCHEMA = dbutils.widgets.get("schema").strip()

VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}"
CONFIG_VOLUME = f"{VOLUME_ROOT}/config"
LANDING_VOLUME = f"{VOLUME_ROOT}/landing"
EXPORT_VOLUME = f"{VOLUME_ROOT}/export"
ENVIRONMENT_FILE = f"{CONFIG_VOLUME}/environment.yml"

TABLE_KEYS = {
    "table", "enabled", "domain", "title", "period", "sources", "rename",
    "types", "filter", "keep_latest", "dictionary", "dictionary_aliases",
    "column_descriptions", "primary_key", "foreign_keys", "geography",
}
SOURCE_KEYS = {"url", "format", "separator", "constants"}


def table_name(name):
    """Return the fully qualified Unity Catalog name of a project table."""
    return f"{CATALOG}.{SCHEMA}.{name}"


def find_project_root(start=None):
    """Return the repository folder (the one holding config/datasets.yml).

    Notebooks run with their own folder as working directory: the function
    walks up from there.
    """
    folder = os.path.abspath(start or os.getcwd())
    while True:
        if os.path.exists(os.path.join(folder, "config", "datasets.yml")):
            return folder
        parent = os.path.dirname(folder)
        if parent == folder:
            raise FileNotFoundError(
                "config/datasets.yml not found above " + os.getcwd()
            )
        folder = parent


def load_yaml(path):
    """Read a YAML file."""
    with open(path, encoding="utf-8") as yaml_file:
        return yaml.safe_load(yaml_file)


def load_environment():
    """Read environment.yml from the config volume (see 00_setup)."""
    if not os.path.exists(ENVIRONMENT_FILE):
        raise FileNotFoundError(
            f"{ENVIRONMENT_FILE} not found: run notebook 00_setup first."
        )
    return load_yaml(ENVIRONMENT_FILE)


def load_datasets(path=None):
    """Read config/datasets.yml and return the list of table settings.

    The `defaults` section is applied to every source. Unknown keys raise
    an error, so that a typo in the file is noticed immediately.
    """
    path = path or os.path.join(find_project_root(), "config", "datasets.yml")
    content = load_yaml(path)
    defaults = content.get("defaults", {})
    tables = []
    for entry in content["tables"]:
        unknown = set(entry) - TABLE_KEYS
        if unknown:
            raise ValueError(f"{entry.get('table')}: unknown keys {unknown}")
        table = copy.deepcopy(entry)
        table.setdefault("enabled", True)
        for key in ("rename", "types", "dictionary_aliases",
                    "column_descriptions"):
            table[key] = table.get(key) or {}
        for key in ("primary_key", "foreign_keys"):
            table[key] = table.get(key) or []
        sources = []
        for source in table["sources"]:
            unknown = set(source) - SOURCE_KEYS
            if unknown:
                raise ValueError(f"{table['table']}: unknown source keys "
                                 f"{unknown}")
            merged = {**defaults, **source}
            merged["constants"] = merged.get("constants") or {}
            sources.append(merged)
        table["sources"] = sources
        tables.append(table)
    return tables


def select_tables(selection, tables=None):
    """Return the tables to process.

    Args:
        selection: "enabled" (default), "all", or table names separated by
            commas, e.g. "ref_municipality, ref_first_name".
        tables: settings from load_datasets() (read if not given).
    """
    tables = tables if tables is not None else load_datasets()
    selection = (selection or "enabled").strip()
    if selection == "all":
        return tables
    if selection == "enabled":
        return [table for table in tables if table["enabled"]]
    names = [name.strip() for name in selection.split(",") if name.strip()]
    by_name = {table["table"]: table for table in tables}
    missing = [name for name in names if name not in by_name]
    if missing:
        raise ValueError(f"Unknown tables in datasets.yml: {missing}")
    return [by_name[name] for name in names]
