# OpenMetadata: harvest Databricks and BigQuery, load the glossary

[OpenMetadata](https://open-metadata.org) holds the **business layer** of the
training: domains, glossaries, terms, owners and the approval workflow
(guideline DG-GDL-001). This project provides the **physical layer**; the
catalog tool harvests it. Tested against **OpenMetadata 2.0** (release 2.0.6
for the glossary import).

## Unity Catalog or OpenMetadata?

Both, for different jobs:

| Need (DG-GDL-001) | OpenMetadata | Unity Catalog |
|---|---|---|
| Business domain with a Data Owner (L0) | domains | no domain object (tags) |
| Glossary › subject area › term › characteristic (L1-L4) | glossaries and term hierarchy | — |
| Definitions, FR/NL synonyms, related terms, references | yes | — |
| Term types (`TermType` classification) | classifications | tags |
| Approval workflow (Draft → Approved), reviewers | yes | — |
| Column ↔ term link | glossary term on the column | only a free tag |
| Access control, column masks, row filters | described | **enforced** |
| Lineage, audit, usage | harvested | native (`system` tables) |

> The glossary says what the data **means** and who approved it; the
> platform **enforces** who may use it. OpenMetadata reads the platform
> through its connectors.

## 1. Two database services

| Service | Connector | What it shows |
|---|---|---|
| `statbel_unity_catalog` | **Unity Catalog** | bronze, silver and metadata tables; descriptions, keys, **tags**, **lineage table and column** (landing → bronze → silver), usage, profiles |
| `statbel_bigquery` | **BigQuery** | the published tables: descriptions, labels, keys, `GEOGRAPHY`, profiles |

Both use **read-only** identities, distinct from the pipeline account.

### Unity Catalog (Databricks)

1. **Identity.** Recommended: a *service principal* (Databricks:
   **Settings › Identity and access › Service principals › Add**), with an
   OAuth secret or a token. On a personal Free Edition workspace you may use
   your own *personal access token* (**Settings › Developer › Access
   tokens**, short lifetime) — it then carries your own rights, not
   read-only ones: acceptable for a demo, not in production.
2. **Rights.** Put the service principal application ID (or the user
   e-mail) in `config/environment.yml` of your Git folder:
   ```yaml
   openmetadata:
     principal: <application-id-of-the-service-principal>
   ```
   then run `00_setup` and **`05_openmetadata_access`**: it grants `USE` and
   `SELECT` on the schema and read access to the lineage, usage and tag
   views — nothing else (no write right, no access to the volumes).
3. **Service.** OpenMetadata › **Settings › Services › Databases › Add New
   Service › Unity Catalog**:
   - *Host and Port*: `dbc-xxxxxxxx-xxxx.cloud.databricks.com:443`;
   - *HTTP Path*: of the *Serverless Starter Warehouse* (printed by notebook
     05, or SQL Warehouses › Connection details);
   - *Authentication*: the token or the OAuth client ID / secret —
     **entered here only**;
   - *Catalog* `workspace`, *Database Schema* `statbel`.
4. **Agents.** *Metadata* (Include Tags: on; table filter: exclude
   `^tutorial_.*`), then *Lineage*, *Usage* and, if wanted, *Profiler*.

External runs: [`openmetadata/unity_catalog_metadata.yaml`](../openmetadata/unity_catalog_metadata.yaml)
and [`unity_catalog_lineage.yaml`](../openmetadata/unity_catalog_lineage.yaml).

### BigQuery

1. **Identity.** A service account for OpenMetadata only, with *BigQuery
   Data Viewer* and *BigQuery Metadata Viewer* on the dataset and *BigQuery
   Job User* on the project (for profiling and usage).
2. **Service.** **Add New Service › BigQuery**, credentials of that account
   (*GCP Credentials Values*), schema filter `^StatBel$`.
3. **Agents.** *Metadata*, then *Profiler* (small sample: free tier).

External runs: [`openmetadata/bigquery_metadata.yaml`](../openmetadata/bigquery_metadata.yaml)
and [`bigquery_profiler.yaml`](../openmetadata/bigquery_profiler.yaml).

### What you should see

- every silver table with its description (ending with the Statbel licence
  line) and **100 %** of its columns described;
- primary and foreign keys as table constraints (BigQuery refuses
  self-referencing keys: the hierarchies `ref_municipality` and
  `ref_nace_bel` are declared in Unity Catalog only);
- tags `domain`, `personal_data`, `representation`… (notebook 04) on the
  Unity Catalog tables, labels on the BigQuery tables;
- lineage `bronze_* → silver_*` in Databricks, column by column.

The lineage **silver (Databricks) → BigQuery** crosses two engines and is
not detected automatically: draw it in the OpenMetadata lineage editor if
you want the end-to-end view in the training.

## 2. Load the business glossary

The import files of the 5 glossaries (127 terms) are in
[`glossary/`](../glossary/README.md), in the OpenMetadata 2.0 import format
(15 columns), with the steps: classification `TermType`, 5 domains,
5 glossaries, import in two passes, reviewers after the import.

## 3. The training exercise

Participants link the **columns** (Databricks silver or BigQuery) to the
**business terms**, in OpenMetadata. The pipeline never does it: no glossary
term is written as a tag, a description or a label.

The trainer's answer key is produced by notebook `06_training_answer_key`
into a Databricks volume that OpenMetadata cannot read (it is not part of
the repository either).
