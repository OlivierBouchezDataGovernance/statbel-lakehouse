# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Statbel catalog: from the DCAT to metadata tables
# MAGIC
# MAGIC Statbel publishes a **DCAT catalog** (Turtle file) describing all its
# MAGIC open data: datasets, downloadable files and documentation. This notebook
# MAGIC turns it into **metadata tables** that can be queried in SQL and used to
# MAGIC **document our tables automatically**.
# MAGIC
# MAGIC | Table | One row per | Content |
# MAGIC |---|---|---|
# MAGIC | `meta_dataset` | dataset | titles and descriptions (EN/FR/NL/DE), period, themes, landing page, licence |
# MAGIC | `meta_distribution` | downloadable file | file name, format, download URL |
# MAGIC | `meta_document` | documentation file | PDF or Excel field dictionary linked to a dataset, download status |
# MAGIC | `meta_field_description` | field × language × dictionary | the raw descriptions read in the dictionaries |
# MAGIC | `meta_field` | field × dictionary | one column per language and a `DESCRIPTION` (EN, else FR, …) |
# MAGIC
# MAGIC **Important:** the DCAT itself does **not** describe the fields. It only
# MAGIC links each dataset (`foaf:page`) to PDF notes and, for most datasets, to
# MAGIC an Excel **field dictionary**. These dictionaries have about 50
# MAGIC different layouts; step 4 normalises them (`lib/dictionary`).
# MAGIC
# MAGIC **Source:** https://doc.statbel.be/publications/DCAT/DCAT_opendata_datasets.ttl
# MAGIC (CC BY 4.0).
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com
# MAGIC
# MAGIC Run it before `02_load_statbel`: the column descriptions of the
# MAGIC silver tables come from `meta_field`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Libraries and utilities
# MAGIC
# MAGIC `rdflib` reads RDF files such as the Turtle (`.ttl`) DCAT catalog.
# MAGIC `openpyxl` reads the Excel dictionaries.

# COMMAND ----------

# MAGIC %pip install rdflib openpyxl pyyaml

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

# MAGIC %run ./lib/common

# COMMAND ----------

# MAGIC %run ./lib/dictionary

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Parameters

# COMMAND ----------

import collections
import hashlib
import re
from concurrent.futures import ThreadPoolExecutor

from rdflib import RDF, Graph, Literal, Namespace
from rdflib.namespace import DCAT, DCTERMS, FOAF

DCAT_URL = load_environment()["statbel"]["dcat_url"]
LANDING_DIR = f"{LANDING_VOLUME}/dcat"
DICTIONARY_DIR = f"{LANDING_DIR}/dictionaries"
DOWNLOAD_THREADS = 8
DCATAP = Namespace("http://data.europa.eu/r5r/")

# Explicit schemas: Spark cannot guess the type of an always-empty column
DATASET_SCHEMA = (
    "DATASET_ID STRING, DATASET_URI STRING, "
    "TITLE_EN STRING, DESCRIPTION_EN STRING, "
    "TITLE_FR STRING, DESCRIPTION_FR STRING, "
    "TITLE_NL STRING, DESCRIPTION_NL STRING, "
    "TITLE_DE STRING, DESCRIPTION_DE STRING, "
    "PERIOD_START STRING, PERIOD_END STRING, THEMES STRING, "
    "HIGH_VALUE BOOLEAN, LANDING_PAGE_EN STRING, LANDING_PAGE_FR STRING, "
    "LICENCE STRING"
)
DISTRIBUTION_SCHEMA = (
    "DATASET_ID STRING, FILE_NAME STRING, FILE_FORMAT STRING, "
    "COMPRESSION STRING, DOWNLOAD_URL STRING"
)
DOCUMENT_SCHEMA = (
    "DATASET_ID STRING, DOCUMENT_URL STRING, DOCUMENT_NAME STRING, "
    "DOCUMENT_TYPE STRING, LANGUAGE STRING, STATUS STRING"
)
FIELD_DESCRIPTION_SCHEMA = (
    "DICTIONARY_URL STRING, SHEET_NAME STRING, FIELD_NAME STRING, "
    "LANGUAGE STRING, DESCRIPTION STRING"
)


def to_dataframe(rows, schema):
    """Build a Spark DataFrame from a list of dicts and a DDL schema."""
    columns = [column.split()[0] for column in schema.split(",")]
    return spark.createDataFrame(
        [tuple(row.get(column) for column in columns) for row in rows],
        schema,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Landing: download the DCAT catalog

# COMMAND ----------

dcat_path = download_file(DCAT_URL, LANDING_DIR)
graph = Graph()
graph.parse(dcat_path, format="turtle")
print(f"{len(graph):,} RDF triples read")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Datasets, files and documents
# MAGIC
# MAGIC In RDF, everything is a triple *subject – predicate – object*, for
# MAGIC example `dataset – dct:title – "Population by sector"@en`. The helper
# MAGIC functions below read the triples of one subject.

# COMMAND ----------

def last_part(uri):
    """Last part of a URI: '.../file-type/XLSX' -> 'XLSX'."""
    return str(uri).rstrip("/").rsplit("/", 1)[-1]


def texts_by_language(subject, predicate):
    """Return {language: text} of the literals of a subject."""
    texts = {}
    for value in graph.objects(subject, predicate):
        if isinstance(value, Literal):
            texts.setdefault(value.language or "und", str(value).strip())
    return texts


def url_language(url):
    """Language of a Statbel page or document: '/fr/...' or '...FR.pdf'."""
    match = re.search(r"statbel\.fgov\.be/(fr|nl|en|de)/", url) or re.search(
        r"\.(FR|NL|EN|DE)\.pdf$", url, re.IGNORECASE
    )
    return match.group(1).lower() if match else None


def dataset_id(subject):
    """Statbel node number: 'NodeID6969' -> '6969'."""
    identifier = str(graph.value(subject, DCTERMS.identifier) or subject)
    return re.sub(r"\D", "", identifier) or identifier


def read_dataset(subject):
    """One row of meta_dataset."""
    titles = texts_by_language(subject, DCTERMS.title)
    descriptions = texts_by_language(subject, DCTERMS.description)
    starts, ends = [], []
    for period in graph.objects(subject, DCTERMS.temporal):
        starts += [str(d) for d in graph.objects(period, DCAT.startDate)]
        ends += [str(d) for d in graph.objects(period, DCAT.endDate)]
    pages = {url_language(str(u)): str(u)
             for u in graph.objects(subject, DCAT.landingPage)}
    row = {"DATASET_ID": dataset_id(subject), "DATASET_URI": str(subject)}
    for language in LANGUAGES:
        row[f"TITLE_{language.upper()}"] = titles.get(language)
        row[f"DESCRIPTION_{language.upper()}"] = descriptions.get(language)
    row.update({
        # ISO dates (yyyy-mm-dd): text order is date order
        "PERIOD_START": min(starts) if starts else None,
        "PERIOD_END": max(ends) if ends else None,
        "THEMES": ",".join(sorted(
            last_part(t) for t in graph.objects(subject, DCAT.theme))),
        "HIGH_VALUE": graph.value(subject, DCATAP.hvdCategory) is not None,
        "LANDING_PAGE_EN": pages.get("en"),
        "LANDING_PAGE_FR": pages.get("fr"),
        "LICENCE": str(graph.value(subject, DCTERMS.license) or "") or None,
    })
    return row


def read_distributions(subject):
    """Rows of meta_distribution: the downloadable files of a dataset."""
    rows = []
    for distribution in graph.objects(subject, DCAT.distribution):
        url = str(graph.value(distribution, DCAT.downloadURL)
                  or graph.value(distribution, DCAT.accessURL) or "")
        file_format = graph.value(distribution, DCTERMS.format)
        compression = graph.value(distribution, DCAT.compressFormat)
        rows.append({
            "DATASET_ID": dataset_id(subject),
            "FILE_NAME": unquote(url.rsplit("/", 1)[-1]),
            "FILE_FORMAT": last_part(file_format) if file_format else None,
            "COMPRESSION": last_part(compression) if compression else None,
            "DOWNLOAD_URL": url,
        })
    return rows


def read_documents(subject):
    """Rows of meta_document: PDF notes and Excel field dictionaries."""
    rows = []
    for page in graph.objects(subject, FOAF.page):
        url = str(page)
        extension = url.rsplit(".", 1)[-1].lower()
        rows.append({
            "DATASET_ID": dataset_id(subject),
            "DOCUMENT_URL": url,
            "DOCUMENT_NAME": unquote(url.rsplit("/", 1)[-1]),
            "DOCUMENT_TYPE": (
                "field dictionary" if extension in ("xlsx", "xls")
                else extension
            ),
            "LANGUAGE": url_language(url),
        })
    return rows


datasets, distributions, documents = [], [], []
for subject in graph.subjects(RDF.type, DCAT.Dataset):
    datasets.append(read_dataset(subject))
    distributions += read_distributions(subject)
    documents += read_documents(subject)
print(f"{len(datasets)} datasets, {len(distributions)} files, "
      f"{len(documents)} documents")

# COMMAND ----------

write_table(to_dataframe(datasets, DATASET_SCHEMA), "meta_dataset")
write_table(to_dataframe(distributions, DISTRIBUTION_SCHEMA),
            "meta_distribution")
check_unique_key("meta_dataset", ["DATASET_ID"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Field dictionaries
# MAGIC
# MAGIC ### 4a. Download
# MAGIC
# MAGIC Many dictionaries have the same name (`Columns description.xlsx`): each
# MAGIC one is stored under a short hash of its URL to avoid overwriting.
# MAGIC Dictionaries already downloaded are not downloaded again. A missing
# MAGIC file (HTTP 404 at Statbel) is recorded, not fatal.

# COMMAND ----------

def dictionary_file_name(url):
    """Unique local name of a dictionary: '<hash>_<original name>'."""
    digest = hashlib.sha1(url.encode()).hexdigest()[:10]
    name = re.sub(r"[^\w.-]+", "_", unquote(url.rsplit("/", 1)[-1]))
    return f"{digest}_{name}"


def fetch_dictionary(url):
    """Download one dictionary if missing; return (url, path, status)."""
    path = os.path.join(DICTIONARY_DIR, dictionary_file_name(url))
    if os.path.exists(path):
        return url, path, "cached"
    try:
        download_file(url, DICTIONARY_DIR, dictionary_file_name(url))
        return url, path, "downloaded"
    except Exception as error:  # noqa: BLE001 - one bad file must not stop
        return url, None, f"error: {error}"


dictionary_urls = sorted({
    d["DOCUMENT_URL"] for d in documents
    if d["DOCUMENT_TYPE"] == "field dictionary"
})
os.makedirs(DICTIONARY_DIR, exist_ok=True)
with ThreadPoolExecutor(max_workers=DOWNLOAD_THREADS) as pool:
    fetched = list(pool.map(fetch_dictionary, dictionary_urls))

status = {url: result for url, _, result in fetched}
for document in documents:
    document["STATUS"] = status.get(document["DOCUMENT_URL"],
                                    "not downloaded")
write_table(to_dataframe(documents, DOCUMENT_SCHEMA), "meta_document")
print(f"{len(dictionary_urls)} dictionaries:",
      dict(collections.Counter(r.split(":")[0] for *_, r in fetched)))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4b. Read and normalise the dictionaries
# MAGIC
# MAGIC `read_dictionary()` (in `lib/dictionary`) turns every sheet into rows
# MAGIC *(field, language, description)*.
# MAGIC Layouts handled:
# MAGIC
# MAGIC | Layout | Example header | Rule |
# MAGIC |---|---|---|
# MAGIC | language suffix | `TX_VARNAME \| TX_DESCR_FR \| TX_DESCR_NL` | language from the column name |
# MAGIC | Statbel standard | `Naam/Nom \| Omschrijving \| Description \| Description \| Umschreibung` | NL, then FR, EN, DE in this order |
# MAGIC | explicit language | `Variable \| Description FR \| Omschrijving NL \| Description EN` | language word in the header |
# MAGIC | long | `Language \| Name \| Label \| Description` | one row per field and language |
# MAGIC | no header | `CD_YEAR \| Reference year` | language unknown (`und`) |
# MAGIC
# MAGIC Columns such as *Type*, *Min*, *Max* or *Format* are ignored. Sheets
# MAGIC without a column of field names (code lists, lists of municipalities)
# MAGIC are skipped. Result on the 2026 catalog: 211 of 214 dictionaries read.

# COMMAND ----------

field_descriptions, unread = [], []
for url, path, result in fetched:
    rows = read_dictionary(url, path) if path else []
    field_descriptions += rows
    if not rows:
        unread.append((url, result))

write_table(to_dataframe(field_descriptions, FIELD_DESCRIPTION_SCHEMA),
            "meta_field_description")
print(f"Dictionaries without fields: {len(unread)}")
for url, result in unread:
    print("  ", unquote(url.rsplit("/", 1)[-1]), "|", result)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4c. One row per field: `meta_field`
# MAGIC
# MAGIC The descriptions are pivoted to one column per language. When a
# MAGIC dictionary describes a field twice (several sheets), the first text is
# MAGIC kept. `DESCRIPTION` follows the rule of the project brief: **English,
# MAGIC else French**, else the other languages.

# COMMAND ----------

pivot = (
    spark.table(table_name("meta_field_description"))
    .groupBy("DICTIONARY_URL", "FIELD_NAME")
    .pivot("LANGUAGE", list(LANGUAGES) + ["und"])
    .agg(F.first("DESCRIPTION"))
)
meta_field = pivot.select(
    "DICTIONARY_URL",
    "FIELD_NAME",
    *[F.col(language).alias(f"DESCRIPTION_{language.upper()}")
      for language in list(LANGUAGES) + ["und"]],
    F.coalesce("en", "fr", "und", "nl", "de").alias("DESCRIPTION"),
)
write_table(meta_field, "meta_field")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Document the metadata tables themselves

# COMMAND ----------

document_table(
    "meta_dataset",
    description="Statbel open data datasets, read from the DCAT catalog.",
    column_comments={
        "DATASET_ID": "Statbel node number of the dataset",
        "DATASET_URI": "URI of the dataset in the DCAT catalog",
        "PERIOD_START": "First day of the period covered",
        "PERIOD_END": "Last day of the period covered",
        "THEMES": "EU data themes (comma separated codes)",
        "HIGH_VALUE": "True if the dataset is an EU High Value Dataset",
        "LICENCE": "URI of the licence",
    },
    primary_key=["DATASET_ID"],
)
document_table(
    "meta_distribution",
    description="Downloadable files of the Statbel datasets (DCAT).",
    column_comments={
        "DATASET_ID": "Statbel node number of the dataset",
        "FILE_NAME": "Name of the downloadable file",
        "FILE_FORMAT": "File format (EU file-type code)",
        "COMPRESSION": "Compression format, if the file is compressed",
        "DOWNLOAD_URL": "Download URL of the file",
    },
)
declare_foreign_key("meta_distribution", "DATASET_ID", "meta_dataset",
                    "DATASET_ID")
document_table(
    "meta_field",
    description=(
        "Field descriptions read in the Statbel field dictionaries "
        "(one row per field and dictionary)."
    ),
    column_comments={
        "DICTIONARY_URL": "URL of the Excel field dictionary",
        "FIELD_NAME": "Field (column) name as written in the dictionary",
        "DESCRIPTION": "Best description: English, else French, else other",
        "DESCRIPTION_UND": "Description of unknown language (no header)",
    },
    primary_key=["DICTIONARY_URL", "FIELD_NAME"],
)
document_table(
    "meta_document",
    description="Documents linked to the Statbel datasets: PDF notes and "
                "Excel field dictionaries (DCAT foaf:page).",
    column_comments={
        "DATASET_ID": "Statbel node number of the dataset",
        "DOCUMENT_URL": "URL of the document",
        "DOCUMENT_TYPE": "pdf, field dictionary, ...",
        "LANGUAGE": "Language of the document, if known",
        "STATUS": "Download status of a field dictionary",
    },
)
document_table(
    "meta_field_description",
    description="Raw field descriptions read in the Statbel field "
                "dictionaries (one row per field, language and sheet).",
    column_comments={
        "DICTIONARY_URL": "URL of the Excel field dictionary",
        "SHEET_NAME": "Sheet of the dictionary",
        "FIELD_NAME": "Field (column) name as written in the dictionary",
        "LANGUAGE": "en, fr, nl, de, or und (unknown)",
        "DESCRIPTION": "Description text",
    },
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Use the metadata
# MAGIC
# MAGIC ### Which fields does a downloadable file have?
# MAGIC
# MAGIC The DCAT links a **file** to a **dataset**, and a dataset to its
# MAGIC **dictionary**. Example: the NUTS / LAU correspondence of 2025.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT dist.FILE_NAME, f.FIELD_NAME, f.DESCRIPTION
# MAGIC FROM workspace.statbel.meta_distribution AS dist
# MAGIC JOIN workspace.statbel.meta_document AS doc
# MAGIC   ON doc.DATASET_ID = dist.DATASET_ID
# MAGIC  AND doc.DOCUMENT_TYPE = 'field dictionary'
# MAGIC JOIN workspace.statbel.meta_field AS f
# MAGIC   ON f.DICTIONARY_URL = doc.DOCUMENT_URL
# MAGIC WHERE dist.FILE_NAME = 'TU_COM_NUTS_LAU-20250101.zip'
# MAGIC ORDER BY f.FIELD_NAME

# COMMAND ----------

# MAGIC %md
# MAGIC ### Automatic documentation: suggestions for our silver tables
# MAGIC
# MAGIC `information_schema.columns` lists the columns of our tables. For each
# MAGIC column, the most frequent description found in **all** Statbel
# MAGIC dictionaries is suggested. This is a suggestion only: the description
# MAGIC of a field may differ from one dataset to another.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT c.table_name,
# MAGIC        c.column_name,
# MAGIC        c.comment                       AS current_comment,
# MAGIC        COUNT(f.FIELD_NAME)             AS dictionaries,
# MAGIC        mode(f.DESCRIPTION)             AS suggested_description
# MAGIC FROM workspace.information_schema.columns AS c
# MAGIC LEFT JOIN workspace.statbel.meta_field AS f
# MAGIC   ON upper(f.FIELD_NAME) = upper(c.column_name)
# MAGIC WHERE c.table_schema = 'statbel'
# MAGIC   AND c.table_name LIKE 'silver_%'
# MAGIC GROUP BY c.table_name, c.column_name, c.comment, c.ordinal_position
# MAGIC ORDER BY c.table_name, c.ordinal_position

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC ✅ The Statbel catalog is now available as tables: datasets, files,
# MAGIC documents and about 4,400 described fields.
# MAGIC
# MAGIC Next: use `meta_field` in the pipeline to fill the column descriptions
# MAGIC of every silver table from its own Statbel dictionary.
