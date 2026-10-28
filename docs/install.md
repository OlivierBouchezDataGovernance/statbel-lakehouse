# Install the project in Databricks

About 30 minutes. You need:

- a **Databricks Free Edition** workspace ([how](databricks_free_edition.md));
- a **Google Cloud** project with billing enabled (BigQuery stays in its free
  tier for the enabled tables) and the **gcloud** CLI
  (https://cloud.google.com/sdk/docs/install);
- the **Databricks CLI** on your computer.

Replace `my-gcp-project` by your Google Cloud project ID everywhere below.

> **Security rules of this project.** A service account key is a password:
> it goes from your disk to the Databricks secret scope without being
> displayed, and is never written in a notebook, a file of the repository,
> a chat or a ticket. Your personal settings are written by `00_setup` in a
> Databricks volume, outside git.

---

## 1. BigQuery: dataset and service account

In a terminal where `gcloud` is installed:

```bash
gcloud auth login
gcloud config set project my-gcp-project

# Dataset in the EU (skip if it already exists)
bq --location=EU mk --dataset --description "Statbel open data (demo)" my-gcp-project:StatBel

# A dedicated service account for the pipeline
gcloud iam service-accounts create statbel-pipeline \
  --display-name="Statbel pipeline (Databricks to BigQuery)"

# Run queries and load jobs in the project
gcloud projects add-iam-policy-binding my-gcp-project \
  --member="serviceAccount:statbel-pipeline@my-gcp-project.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser" --condition=None
```

Write access **on the dataset only** (least privilege). In the BigQuery
console (or with `bq query --use_legacy_sql=false`), run:

```sql
GRANT `roles/bigquery.dataEditor`
ON SCHEMA `my-gcp-project`.StatBel
TO "serviceAccount:statbel-pipeline@my-gcp-project.iam.gserviceaccount.com";
```

> Windows PowerShell 5.1 removes the double quotes inside a command line:
> run this `GRANT` in the BigQuery console instead.

Check that the account has nothing more than `roles/bigquery.jobUser` on the
project:

```bash
gcloud projects get-iam-policy my-gcp-project \
  --flatten="bindings[].members" \
  --filter="bindings.members:statbel-pipeline@" \
  --format="table(bindings.role)"
```

## 2. Databricks CLI and login

Install the CLI (https://docs.databricks.com/dev-tools/cli/install.html),
for example `winget install Databricks.DatabricksCLI` on Windows or
`brew install databricks/tap/databricks` on macOS. Then:

```bash
databricks auth login --host https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
databricks current-user me
```

## 3. Store the key in a Databricks secret scope

Create the key, send it to Databricks, then delete the local copy:

```bash
gcloud iam service-accounts keys create statbel-pipeline-key.json \
  --iam-account=statbel-pipeline@my-gcp-project.iam.gserviceaccount.com

databricks secrets create-scope statbel
databricks secrets put-secret statbel bigquery-sa-key < statbel-pipeline-key.json
databricks secrets list-secrets statbel     # shows the key name only

rm statbel-pipeline-key.json                # Windows: Remove-Item statbel-pipeline-key.json
```

On Windows PowerShell the redirection `<` does not exist; use:

```powershell
Get-Content -Raw statbel-pipeline-key.json | databricks secrets put-secret statbel bigquery-sa-key
```

## 4. Add the repository to Databricks (Git folder)

1. In Databricks: **Workspace** → your home folder → **Create** →
   **Git folder**.
2. Git repository URL: `https://github.com/OlivierBouchezDataGovernance/statbel-lakehouse`
   (or the URL of your fork), provider **GitHub** → **Create Git folder**.
   If Databricks asks for Git credentials, add them in **Settings** →
   **Linked accounts** → **Git integration**.
3. The folder contains `config/`, `notebooks/` and `docs/`. To get a new
   version later: open the Git folder, button **(branch name)** → **Pull**.

To change the configuration (for example enable `ref_address`), use your own
**fork**: edit `config/datasets.yml` in the Git folder, then commit and push
from the same dialog.

## 5. Run the notebooks

Open them in `notebooks/` and use **Run all** (compute: **Serverless**).

| Order | Notebook | First run | What to check |
|---|---|---|---|
| 1 | `00_setup` | 1 min | your settings (see below), **Run all**: schema and volumes created, secret found, `Connected as: statbel-pipeline@…` |
| 2 | `01_statbel_catalog` | 3-5 min | about 485 datasets, 1,075 files and 4,000+ described fields |
| 3 | `02_load_statbel` | 10-20 min | one line per table in the report; keys `OK` or explained; orphan counts |
| 4 | `03_publish_bigquery` | 5-10 min | summary table: rows, size, keys and described columns per table |

**Your settings for `00_setup`.** In the Git folder, open `config`, then
**Create** → **File**, name it `environment.yml` and paste the content of
`environment.example.yml` with your values (at least `bigquery.project_id`).
This file is in `.gitignore`: it stays in your workspace and is never
committed. Alternatively, fill in the widgets of the notebook if your
Databricks editor shows them.

The `%restart_python` cell interrupts *Run all* for a second after the
libraries are installed; the run continues by itself (if not, run **Run all**
again: the installation is then immediate).

Load or publish only some tables with the widget **`tables`**, e.g.
`ref_municipality, ref_first_name`.

## 6. Optional: run it as a Databricks job

[`databricks.yml`](../databricks.yml) describes a job with three tasks
(catalog → load → publish). From a clone of the repository on your
computer:

```bash
databricks bundle validate
databricks bundle deploy
databricks bundle run statbel_pipeline
```

`00_setup` must have run once before (it writes your settings).

## 7. Check the result in BigQuery

```sql
SELECT table_name, option_value AS description
FROM `my-gcp-project.StatBel.INFORMATION_SCHEMA.TABLE_OPTIONS`
WHERE option_name = 'description';

SELECT TABLE_NAME, CHECK_NAME, CHECK_VALUE, DETAIL
FROM `my-gcp-project.StatBel.run_report`
WHERE CHECK_NAME LIKE 'orphans:%' AND CHECK_VALUE > 0;
```

## 8. Clean up

Nothing is deleted by the pipeline. To remove everything:

- BigQuery: delete the tables of the dataset (or the dataset) in the console;
- Databricks: `DROP SCHEMA workspace.statbel CASCADE` in the SQL editor
  (tables and volumes) and delete the Git folder;
- secret: `databricks secrets delete-scope statbel`;
- Google Cloud: delete the service account `statbel-pipeline`.
