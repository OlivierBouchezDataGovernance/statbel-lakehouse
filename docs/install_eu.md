# Run the pipeline in the EU: Databricks on Google Cloud, Belgium

The Free Edition workspace runs on AWS in the United States (`us-east-2`), cannot change region, and its terms
allow only personal, not-for-profit use without personal data. This guide moves the pipeline to a Databricks
workspace on **Google Cloud, region `europe-west1` (Belgium)**, next to the BigQuery dataset (EU). The Free Edition
can stay for trainings on aggregated open data only.

Decided on 31 October 2026 by Olivier Bouchez. **Done on 31 October 2026:** the Marketplace subscription created the
account *olivier bouchez data consulting* and a **serverless workspace in `europe-west3` (Frankfurt)**, Premium, with
Databricks-managed storage, kept as the EU workspace: `https://8259553561530577.7.gcp.databricks.com` (catalog
`workspace`, metastore `metastore_gcp_europe_west3`). Trial credit: 400 USD until 14 November 2026, then
pay-as-you-go on the Google Cloud bill. Governance documents that apply: Data Source and Combination Review
Procedure (DG-PRO-0045), Register of Processing Activities Standard (DG-STD-0043, finding F-01).

## What it costs (prices of 31 October 2026)

| Item | Price |
|---|---|
| Databricks plan | Premium (the Standard tier is ended on Google Cloud); 14-day trial; no fixed fee |
| Jobs serverless (europe-west1) | 0.39 USD per DBU, compute included |
| Jobs compute, classic, n2-standard-4 | 0.15 USD per DBU × 0.72 DBU/h = 0.108 USD/h, plus the VM: 0.188 EUR/h on demand, about 0.04 EUR/h as Spot (Google Cloud billing catalog, europe-west1) |
| Workspace storage | two Cloud Storage buckets in the project: cents for this volume |

A light use (a few loads of one hour a month) costs about 1 to 10 EUR a month. Databricks bills through the Google
Cloud bill of the project.

## 1. Subscribe and create the workspace (the owner, in the browser)

These steps accept the terms of Databricks and use the billing account: the owner does them.

1. Google Cloud console, project `strong-aegis-512509-r4` › **Marketplace** › search **Databricks** › listing
   *Databricks* › **Subscribe**: billing account of the project, plan pay-as-you-go (Premium trial), accept the
   terms.
2. **Manage on provider** (or *Sign up with Databricks*): sign in with the Google account of the project. The account
   console opens at `https://accounts.gcp.databricks.com`.
3. **Workspaces › Create workspace**: name `statbel-eu`, region **europe-west1**, Google Cloud project ID
   `strong-aegis-512509-r4`, network managed by Databricks. Wait until the status is *Running* (about 10 minutes).
4. Note the URL of the workspace (`https://<number>.<n>.gcp.databricks.com`).

## 2. CLI profile and secret (the owner, in a terminal)

```bash
databricks auth login --host https://<workspace URL> --profile eu
databricks secrets create-scope statbel --profile eu
databricks secrets put-secret statbel bigquery-sa-key --profile eu < path/to/key.json
```

The key goes from the disk to the secret scope without being displayed (same rule as in `install.md`, section 3).

## 3. Deploy and run (Claude Code or the owner)

```bash
databricks bundle validate -t eu
databricks bundle deploy -t eu
```

Then run `00_setup` once in the workspace, with the catalog that Unity Catalog created for the workspace (Catalog
Explorer shows its name) and the schema `statbel`, and run the job:

```bash
databricks bundle run statbel_pipeline -t eu --params catalog=<catalog>,schema=statbel,tables=enabled
```

## 4. After the first load

- Remove the name tables (`ref_first_name`, `ref_family_name`) from the Free Edition workspace: the Free Edition
  forbids personal data (finding F-01 of the register of processing activities). The owner decides the deletion.
- Point the OpenMetadata Unity Catalog connector to the new workspace if it should be harvested.
- Never load personal data (names, addresses) in the Free Edition again; the address file is loaded only after the
  approval of the DPIA DG-DOC-0048.
