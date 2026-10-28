# Create a Databricks Free Edition workspace

Databricks **Free Edition** is a free, personal workspace with serverless
compute, Unity Catalog, notebooks, SQL and jobs. It is enough for this
project. No credit card is needed.

## 1. Sign up

1. Go to **https://www.databricks.com/learn/free-edition** (or search
   "Databricks Free Edition") and choose **Sign up** / *Get started for
   free*.
2. Sign up with an e-mail address (or a Google / Microsoft account) and
   confirm the e-mail.
3. Databricks creates the workspace in a few minutes. Its address looks like
   `https://dbc-xxxxxxxx-xxxx.cloud.databricks.com/` — **write it down**: the
   command line needs it.

The logo at the top left shows *Free Edition*.

## 2. Find your way around (5 minutes)

| Menu (left) | What it is |
|---|---|
| **Workspace** | your files: notebooks, Git folders (code, not data) |
| **Catalog** | Unity Catalog: data organised as `catalog.schema.table`, plus **volumes** for files. On Free Edition the catalog is **`workspace`** |
| **Compute** | nothing to create: everything is **serverless** |
| **SQL Editor** | SQL queries, on the *Serverless Starter Warehouse* |
| **Jobs & Pipelines** | scheduled or chained runs of notebooks |

A first test in **SQL Editor** (select the *Serverless Starter Warehouse*,
top right, then **Run**):

```sql
SELECT pickup_zip, COUNT(*) AS trips
FROM samples.nyctaxi.trips
GROUP BY pickup_zip
ORDER BY trips DESC
LIMIT 10;
```

## 3. Limits worth knowing

- Compute is serverless only, with daily usage limits: enough for this
  project, not for heavy workloads.
- Outbound internet access is available from notebooks (the pipeline
  downloads the Statbel files directly). Some sites refuse clients that
  announce themselves as `Python-urllib`: the pipeline sends a browser-like
  *User-Agent*.
- Secret scopes are created with the Databricks CLI (next document).

Next: [install the project](install.md).
