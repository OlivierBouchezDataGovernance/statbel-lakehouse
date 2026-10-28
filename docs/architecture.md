# Architecture

```
Statbel web ──► Landing ──► Bronze ──► Silver ──► BigQuery ──► Data catalog
                (files)     (Delta)    (Delta)    (dataset)    (OpenMetadata)
Statbel DCAT ──► meta_* tables (datasets, files, field dictionaries) ──┘ descriptions
```

| Layer | Where | Rules |
|---|---|---|
| **Landing** | volume `/Volumes/<catalog>/<schema>/landing/<table>/` | files stored **unchanged** (zip, extracted text or GeoJSON); a UTF-8 copy `*.utf8.txt` when the source is Windows-1252 |
| **Bronze** | `bronze_<table>` (one per source file) | data **as read**: every column as text, no renaming; lineage columns `_source_file`, `_source_url`, `_ingested_at` |
| **Silver** | `silver_<table>` | Statbel column names **kept** (upper case); typed by prefix; `-`/empty → NULL; filters and latest period from the configuration; `REF_PERIOD`; geometry reprojected to WGS 84; documented; primary key if tested unique |
| **Metadata** | `meta_dataset`, `meta_distribution`, `meta_document`, `meta_field_description`, `meta_field` | the Statbel DCAT catalog and its field dictionaries, as tables |
| **Quality** | `run_report` (Delta and BigQuery) | one row per check and run |
| **BigQuery** | dataset of `environment.yml` | silver tables loaded from Parquet; descriptions, labels, primary and foreign keys, `GEOGRAPHY` |

## Principles

- **Idempotent**: every step replaces its output (`overwrite`,
  `WRITE_TRUNCATE`); running again gives the same result, without duplicates.
- **Configuration-driven**: `config/datasets.yml` describes the tables; the
  code has no table-specific branch.
- **Keys after loads**: foreign keys are dropped before and declared after
  all loads, because replacing a table or its primary key breaks the keys
  pointing to it.
- **Nothing hidden**: orphans are counted, not dropped; values that cannot be
  typed are counted; missing descriptions are written as missing.
- **Least privilege and no secrets in git**: see [install.md](install.md).

## Code

| Notebook | Role |
|---|---|
| `lib/config` | widgets `catalog`/`schema`, `environment.yml`, `datasets.yml`, table selection |
| `lib/common` | download, zip, encoding, bronze, Delta writes, quality checks, Unity Catalog documentation, run report |
| `lib/dictionary` | reader of the Statbel field dictionaries (about 50 layouts) |
| `lib/transform` | bronze and silver from the configuration, typing, geometry, descriptions |
| `lib/bigquery` | Parquet export, BigQuery load, geography, metadata, keys, run report |

Libraries are notebooks included with `%run`, so that the project runs as a
plain Git folder on Databricks Free Edition, without packaging.
