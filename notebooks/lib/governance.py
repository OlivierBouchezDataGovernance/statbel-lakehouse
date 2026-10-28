# Databricks notebook source
# MAGIC %md
# MAGIC # lib · governance — tags and business concepts of the columns
# MAGIC
# MAGIC Included with `%run ./lib/governance` (it includes `common` and
# MAGIC `config`).
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC |---|---|
# MAGIC | `representation()` | physical representation of a column, from its Statbel prefix |
# MAGIC | `concept_key()` | the column name without its technical noise (guideline DG-GDL-001, step 2) |
# MAGIC | `table_tags()` | governance tags of a silver table, from `config/datasets.yml` |
# MAGIC | `set_tags()` | write tags on a table or a column in Unity Catalog |
# MAGIC
# MAGIC **Why tags on columns are representations, not glossary terms.** In
# MAGIC DG-GDL-001 a code and its French and Dutch labels are *one* term; the
# MAGIC prefix only says how the value is written (`code`, `label`,
# MAGIC `measure`…). Linking columns to business terms is the exercise of the
# MAGIC training: it is done by people in OpenMetadata, never by this pipeline.
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

import re

REPRESENTATIONS = (
    ("CD_", "code"), ("TX_", "label"), ("MS_", "measure"), ("DT_", "date"),
    ("FL_", "flag"), ("LVL_", "level"), ("XY_", "coordinate"),
)
SPECIAL_REPRESENTATIONS = {
    "REF_PERIOD": "reference_period",
    "GEOMETRY": "geometry",
}

# Technical noise of the Statbel names (DG-GDL-001, method step 2)
NOISE_PREFIX = re.compile(r"^(CD|TX|MS|DT|FL|NR|NM|LVL|XY|T|C)_")
NOISE_SUFFIX = re.compile(
    r"(_(FR|NL|DE|EN))?(_LVL_?\d+)?(_(FR|NL|DE|EN))?(_DESCR?)?$"
)
NOISE_WORDS = {"DESCR", "DSCR", "DESC", "TX", "CD", "LVL"}


def representation(column):
    """Physical representation of a column: code, label, measure, date…"""
    if column in SPECIAL_REPRESENTATIONS:
        return SPECIAL_REPRESENTATIONS[column]
    for prefix, value in REPRESENTATIONS:
        if column.startswith(prefix):
            return value
    return "other"


def concept_key(column):
    """Column name without its technical noise.

    Examples: CD_NATLTY, TX_NATLTY_FR and TX_NATLTY_DESCR_NL -> NATLTY;
    CD_REFNIS_LVL_4 -> REFNIS; TX_SEX_DESCR_FR -> SEX;
    CD_SECTOR_2024 -> SECTOR.
    """
    name = column.upper()
    name = NOISE_PREFIX.sub("", name)
    for _ in range(2):  # suffixes may be combined: _DESCR_FR_LVL1
        name = NOISE_SUFFIX.sub("", name)
        name = re.sub(r"_(FR|NL|DE|EN)(_LVL\d+)?$", "", name)
    name = re.sub(r"_(19|20)\d\d$", "", name)  # edition year: _2024
    words = [w for w in name.split("_") if w and w not in NOISE_WORDS]
    return "_".join(words) or column.upper()


def table_tags(table, governance):
    """Governance tags of a silver table (configuration + domain)."""
    tags = dict(governance.get("table_tags") or {})
    domain = (governance.get("domains") or {}).get(table["domain"])
    if domain:
        tags["domain"] = domain
    tags["personal_data"] = table.get("personal_data", "none")
    tags["period"] = str(table["period"])
    return tags


def _tag_list(tags):
    """SQL list of tags: ('key' = 'value', ...)."""
    return ", ".join(
        f"'{_sql_text(str(key))}' = '{_sql_text(str(value))}'"
        for key, value in tags.items()
    )


def set_tags(name, tags, column=None):
    """Write tags on a project table, or on one of its columns."""
    if not tags:
        return
    target = f"ALTER COLUMN `{column}` " if column else ""
    spark.sql(f"ALTER TABLE {table_name(name)} {target}"
              f"SET TAGS ({_tag_list(tags)})")
