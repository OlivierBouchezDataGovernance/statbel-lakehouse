# Databricks notebook source
# MAGIC %md
# MAGIC # lib · common — landing, bronze, quality checks, documentation
# MAGIC
# MAGIC Included with `%run ./lib/common` (it includes `config`).
# MAGIC
# MAGIC | Function | Layer | Purpose |
# MAGIC |---|---|---|
# MAGIC | `download_file()` | landing | download a file, unchanged, into the volume |
# MAGIC | `extract_zip()` | landing | check a zip file and extract its data file |
# MAGIC | `download_zip()` | landing | both steps at once |
# MAGIC | `detect_encoding()` / `to_utf8()` | landing | real encoding of a text file; UTF-8 copy if needed |
# MAGIC | `read_statbel_text()` | bronze | read a delimited Statbel file, all columns as text |
# MAGIC | `add_lineage_columns()` | bronze | `_source_file`, `_source_url`, `_ingested_at` |
# MAGIC | `write_table()` | all | write a Delta table (overwrite: idempotent) |
# MAGIC | `blank_to_null()` | silver | trim text; `-` and empty become NULL |
# MAGIC | `check_unique_key()` | quality | duplicate keys and NULL key values |
# MAGIC | `count_orphans()` | quality | rows whose foreign key has no match |
# MAGIC | `document_table()` | silver | description, column comments, primary key |
# MAGIC | `declare_foreign_key()` | silver | declare one foreign key in Unity Catalog |
# MAGIC | `report()` / `save_report()` | quality | collect checks into the `run_report` table |
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %run ./config

# COMMAND ----------

import codecs
import datetime
import json
import tempfile
import uuid
import zipfile
from urllib.parse import unquote

import requests
from pyspark.sql import functions as F

HTTP_TIMEOUT_SECONDS = 300
DEFAULT_USER_AGENT = "Mozilla/5.0 (statbel-lakehouse)"
DEFAULT_LICENCE_LINE = (
    "Source: Statbel open data (statbel.fgov.be/fr/open-data), CC BY 4.0. "
    "Transformed by Olivier Bouchez · Data Consulting."
)
NULL_TOKENS = ("-", "")


def _environment_value(section, key, default):
    """Value of environment.yml, or a default if the file does not exist."""
    if not os.path.exists(ENVIRONMENT_FILE):
        return default
    return (load_environment().get(section) or {}).get(key, default)


# Statbel answers with an HTML page to clients announcing "Python-urllib":
# the pipeline presents itself as a browser.
USER_AGENT = _environment_value("statbel", "user_agent", DEFAULT_USER_AGENT)
LICENCE_LINE = _environment_value("licence", "data_line",
                                  DEFAULT_LICENCE_LINE).strip()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing: download and check a file
# MAGIC
# MAGIC 1. `download_file()` stores the file **unchanged**. An HTML page
# MAGIC    returned instead of the file stops the run instead of being stored.
# MAGIC 2. `extract_zip()` checks that the file is a valid zip archive and
# MAGIC    extracts its single data file next to it.

# COMMAND ----------

def download_file(url, landing_dir, file_name=None):
    """Download a file, unchanged, into the landing volume.

    Args:
        url: download URL.
        landing_dir: folder of the volume where the file is stored.
        file_name: stored name; by default the last part of the URL.

    Returns:
        Full path of the downloaded file.
    """
    os.makedirs(landing_dir, exist_ok=True)
    if MIRROR:
        content = mirror_get(url)
    else:
        response = requests.get(
            url, headers={"User-Agent": USER_AGENT},
            timeout=HTTP_TIMEOUT_SECONDS
        )
        response.raise_for_status()
        if response.headers.get("Content-Type", "").startswith("text/html"):
            raise ValueError(
                f"The server returned a web page, not a file: {url}")
        content = response.content

    file_name = file_name or unquote(url.rsplit("/", 1)[-1])
    file_path = os.path.join(landing_dir, file_name)
    with open(file_path, "wb") as target:
        target.write(content)
    print(f"Downloaded {file_path} ({len(content):,} bytes)"
          + (f" from the mirror gs://{MIRROR}" if MIRROR else ""))
    return file_path

# COMMAND ----------

# MAGIC %md
# MAGIC ### Mirror in Google Cloud Storage
# MAGIC
# MAGIC When `statbel.mirror` (a bucket name) is set in environment.yml, the
# MAGIC files are read from that bucket instead of Statbel: the serverless
# MAGIC compute of the EU workspace reaches the Google APIs but not the public
# MAGIC internet. `scripts/mirror_statbel.py` fills the bucket, object name
# MAGIC `<host>/<decoded path>`, with the Last-Modified of Statbel in the
# MAGIC metadata `source-last-modified`. Access with the key of the BigQuery
# MAGIC service account (secret scope of environment.yml).

# COMMAND ----------

MIRROR = _environment_value("statbel", "mirror", "") or ""
_MIRROR_TOKEN = {}


def _mirror_headers():
    """Authorization header for the GCS JSON API (service account key)."""
    from google.oauth2 import service_account
    import google.auth.transport.requests

    credentials = _MIRROR_TOKEN.get("credentials")
    if credentials is None:
        secrets = load_environment()["secrets"]
        info = json.loads(dbutils.secrets.get(scope=secrets["scope"],
                                              key=secrets["bigquery_key"]))
        credentials = service_account.Credentials.from_service_account_info(
            info, scopes=["https://www.googleapis.com/auth/"
                          "devstorage.read_only"])
        _MIRROR_TOKEN["credentials"] = credentials
    if not credentials.valid:
        credentials.refresh(google.auth.transport.requests.Request())
    return {"Authorization": f"Bearer {credentials.token}"}


def _mirror_object_url(url):
    from urllib.parse import quote, urlsplit
    parts = urlsplit(url)
    name = quote(parts.netloc + unquote(parts.path), safe="")
    return f"https://storage.googleapis.com/storage/v1/b/{MIRROR}/o/{name}"


def mirror_get(url):
    """Content of a Statbel file, read from the mirror bucket."""
    response = requests.get(_mirror_object_url(url) + "?alt=media",
                            headers=_mirror_headers(),
                            timeout=HTTP_TIMEOUT_SECONDS)
    if response.status_code == 404:
        raise FileNotFoundError(
            f"Not in the mirror gs://{MIRROR}: {url} "
            "(run scripts/mirror_statbel.py)")
    response.raise_for_status()
    return response.content


def mirror_last_modified(url):
    """Last-Modified of Statbel recorded in the mirror (string), or None."""
    response = requests.get(_mirror_object_url(url),
                            headers=_mirror_headers(), timeout=60)
    if response.status_code == 404:
        return None
    response.raise_for_status()
    return (response.json().get("metadata") or {}).get(
        "source-last-modified") or None


def extract_zip(zip_path, extensions=(".txt", ".csv")):
    """Check a zip file, extract it next to itself, return its data file.

    Args:
        zip_path: full path of the zip file in the landing volume.
        extensions: extensions of the expected data file.

    Returns:
        Full path of the extracted data file (exactly one is expected).
    """
    if not zipfile.is_zipfile(zip_path):
        raise ValueError(f"Not a valid zip file: {zip_path}")

    target_dir = os.path.dirname(zip_path)
    with zipfile.ZipFile(zip_path) as archive:
        data_files = [
            name for name in archive.namelist()
            if name.lower().endswith(tuple(extensions))
        ]
        if len(data_files) != 1:
            raise ValueError(
                f"Expected one {extensions} file in {zip_path}: {data_files}"
            )
        with tempfile.TemporaryDirectory() as local_dir:
            local_path = archive.extract(data_files[0], local_dir)
            target_path = os.path.join(target_dir, data_files[0])
            upload_to_volume(local_path, target_path)
    return target_path


def upload_to_volume(local_path, target_path):
    """Copy a file of the local disk into a volume with the Files API.

    On serverless, writing a large file (above ~100 MB) into a volume through
    the file system fails with "[Errno 5] Input/output error", and
    dbutils.fs.cp refuses local paths: large files are written on the local
    disk first, then uploaded.
    """
    from databricks.sdk import WorkspaceClient
    with open(local_path, "rb") as source:
        WorkspaceClient().files.upload(target_path, source, overwrite=True)


def download_zip(url, landing_dir, extensions=(".txt", ".csv")):
    """Download a zip file into the landing volume and extract it.

    Shortcut for download_file() followed by extract_zip().

    Returns:
        Full path of the extracted data file.
    """
    zip_path = download_file(url, landing_dir)
    return extract_zip(zip_path, extensions)


def detect_encoding(path, chunk_size=16 * 1024 * 1024):
    """Return the encoding of a text file: "utf-8" or "cp1252".

    The whole file is decoded, chunk by chunk (strict UTF-8): Statbel files
    that look like UTF-8 at the start may be Windows-1252 further down, and
    the wrong choice damages accented values (Irène -> Ir�ne).
    """
    decoder = codecs.getincrementaldecoder("utf-8")()
    with open(path, "rb") as text_file:
        try:
            while chunk := text_file.read(chunk_size):
                decoder.decode(chunk)
            decoder.decode(b"", final=True)
            return "utf-8"
        except UnicodeDecodeError:
            return "cp1252"


def to_utf8(path):
    """Return a UTF-8 version of a text file (the file itself if UTF-8).

    Spark 4 only reads a few encodings (not Windows-1252): such a file is
    converted line by line into '<name>.utf8.txt', next to the original,
    which stays unchanged in the landing volume.
    """
    encoding = detect_encoding(path)
    if encoding == "utf-8":
        return path
    target = f"{path}.utf8.txt"
    with tempfile.TemporaryDirectory() as local_dir:
        local_path = os.path.join(local_dir, "converted.txt")
        with open(path, encoding=encoding, newline="") as source, \
                open(local_path, "w", encoding="utf-8",
                     newline="") as converted:
            for line in source:
                converted.write(line)
        upload_to_volume(local_path, target)
    print(f"Converted {encoding} -> UTF-8: {target}")
    return target

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze: the data as received, with lineage columns

# COMMAND ----------

def read_statbel_text(path, separator="|", multiline=False):
    """Read a delimited Statbel file: header row, all columns as text.

    The file is first converted to UTF-8 if needed (see to_utf8()).
    multiline: quoted values may hold line breaks and doubled quotes
    (RFC 4180); slower, so only for the sources that need it.
    """
    reader = (
        spark.read
        .option("header", True)
        .option("sep", separator)
        .option("encoding", "UTF-8")
    )
    if multiline:
        reader = reader.option("multiLine", True).option("escape", '"')
    return reader.csv(to_utf8(path))


def add_lineage_columns(df, source_url):
    """Add the bronze lineage columns: source file, source URL, load time."""
    source_file = unquote(source_url.rsplit("/", 1)[-1])
    return (
        df
        .withColumn("_source_file", F.lit(source_file))
        .withColumn("_source_url", F.lit(source_url))
        .withColumn("_ingested_at", F.current_timestamp())
    )


def write_table(df, name):
    """Write a Delta table, replacing it, and return its row count.

    `overwrite` makes the operation idempotent: running it again replaces
    the table instead of adding duplicates.
    """
    (df.write.mode("overwrite").option("overwriteSchema", True)
     .saveAsTable(table_name(name)))
    row_count = spark.table(table_name(name)).count()
    print(f"{table_name(name)}: {row_count:,} rows")
    return row_count


def table_exists(name):
    """True if a project table exists in Unity Catalog."""
    return spark.catalog.tableExists(table_name(name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver: cleaning helper

# COMMAND ----------

def blank_to_null(column_name):
    """Trim a text column; '-' and empty strings become NULL."""
    value = F.trim(F.col(f"`{column_name}`"))
    return F.when(value.isin(*NULL_TOKENS), None).otherwise(value)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quality checks and run report
# MAGIC
# MAGIC Keys are **informational** in Databricks and BigQuery (*NOT
# MAGIC ENFORCED*): nothing blocks a duplicate or an orphan, so they are
# MAGIC checked on every load. Orphans are **reported, never dropped**.
# MAGIC Every check is also written to the table `run_report`.

# COMMAND ----------

RUN_ID = datetime.datetime.now(datetime.timezone.utc).strftime(
    "%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:6]
REPORT_ROWS = []
REPORT_SCHEMA = (
    "RUN_ID STRING, RUN_AT TIMESTAMP, TABLE_NAME STRING, CHECK_NAME STRING, "
    "CHECK_VALUE DOUBLE, DETAIL STRING"
)


def report(table, check, value, detail=None):
    """Add one check result to the run report (saved by save_report()).

    The table is reported by its configuration name (prefix silver_
    removed), so that all checks of a table share the same name.
    """
    if table.startswith("silver_"):
        table = table[len("silver_"):]
    REPORT_ROWS.append((
        RUN_ID, datetime.datetime.now(datetime.timezone.utc), table, check,
        float(value), detail,
    ))


def save_report():
    """Append the collected checks to the Delta table run_report."""
    if not REPORT_ROWS:
        return 0
    (spark.createDataFrame(REPORT_ROWS, REPORT_SCHEMA)
     .write.mode("append").saveAsTable(table_name("run_report")))
    print(f"{table_name('run_report')}: {len(REPORT_ROWS)} check(s) "
          f"added for run {RUN_ID}")
    return len(REPORT_ROWS)


def check_unique_key(name, key_columns):
    """Print, report and return (duplicate keys, rows with a NULL key)."""
    df = spark.table(table_name(name))
    duplicates = df.groupBy(*key_columns).count().filter("count > 1").count()
    null_values = df.filter(
        " OR ".join(f"`{column}` IS NULL" for column in key_columns)
    ).count()
    status = "OK" if duplicates == 0 and null_values == 0 else "FAILED"
    print(f"Key ({', '.join(key_columns)}) of {name}: "
          f"{duplicates} duplicate(s), {null_values} row(s) with a NULL key "
          f"-> {status}")
    report(name, "duplicate_keys", duplicates, ", ".join(key_columns))
    report(name, "null_key_rows", null_values, ", ".join(key_columns))
    return duplicates, null_values


def count_orphans(name, column, referenced_name, referenced_column):
    """Print, report and return the rows whose foreign key has no match.

    NULL values are not orphans (no reference expected) and are ignored.
    """
    df = spark.table(table_name(name)).filter(F.col(column).isNotNull())
    referenced = (
        spark.table(table_name(referenced_name))
        .select(F.col(referenced_column).alias(column))
    )
    orphans = df.join(referenced, column, "left_anti")
    orphan_count = orphans.count()
    print(f"Foreign key {name}.{column} -> "
          f"{referenced_name}.{referenced_column}: "
          f"{orphan_count:,} orphan row(s)")
    report(name, f"orphans:{column}", orphan_count,
           f"{referenced_name}.{referenced_column}")
    return orphans

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation and keys in Unity Catalog
# MAGIC
# MAGIC `document_table()` writes the table description (ending with the
# MAGIC licence line), the column descriptions and the primary key.
# MAGIC Foreign keys are declared separately, **after** all tables are loaded:
# MAGIC replacing a primary key drops (CASCADE) the foreign keys pointing to it.

# COMMAND ----------

def _sql_text(text):
    """Escape a text for a single-quoted SQL literal."""
    return text.replace("\\", "\\\\").replace("'", "\\'")


def _alter(name, clause):
    """Run one ALTER TABLE statement on a project table."""
    spark.sql(f"ALTER TABLE {table_name(name)} {clause}")


def document_table(name, description, column_comments, primary_key=()):
    """Describe a table and declare its primary key in Unity Catalog.

    Args:
        name: short table name, e.g. "silver_ref_municipality".
        description: table description, without the licence line.
        column_comments: dict {column: description}.
        primary_key: columns of the primary key (empty: none declared).
    """
    full_description = _sql_text(f"{description} {LICENCE_LINE}")
    spark.sql(f"COMMENT ON TABLE {table_name(name)} IS '{full_description}'")
    for column, comment in column_comments.items():
        _alter(name, f"ALTER COLUMN `{column}` COMMENT '{_sql_text(comment)}'")

    _alter(name, "DROP PRIMARY KEY IF EXISTS CASCADE")
    if primary_key:
        for column in primary_key:
            _alter(name, f"ALTER COLUMN `{column}` SET NOT NULL")
        _alter(name, f"ADD CONSTRAINT pk_{name} "
                     f"PRIMARY KEY ({', '.join(primary_key)})")
    print(f"{table_name(name)}: description, {len(column_comments)} column "
          f"comment(s), primary key {list(primary_key)}")


def declare_foreign_key(name, column, referenced_name, referenced_column):
    """Declare (or declare again) one foreign key in Unity Catalog."""
    constraint = f"fk_{name}_{column}".lower()
    _alter(name, f"DROP CONSTRAINT IF EXISTS {constraint}")
    _alter(name, f"ADD CONSTRAINT {constraint} FOREIGN KEY (`{column}`) "
                 f"REFERENCES {table_name(referenced_name)} "
                 f"(`{referenced_column}`)")
