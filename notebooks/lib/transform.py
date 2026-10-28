# Databricks notebook source
# MAGIC %md
# MAGIC # lib · transform — configuration-driven bronze and silver
# MAGIC
# MAGIC Included with `%run ./lib/transform` (it includes `common`,
# MAGIC `config` and `dictionary`). Everything a table needs comes from its
# MAGIC entry in `config/datasets.yml`: no table-specific code.
# MAGIC
# MAGIC | Function | Purpose |
# MAGIC |---|---|
# MAGIC | `load_bronze()` | download every source of a table and write one bronze table per source |
# MAGIC | `build_silver()` | stack the sources, filter, type the columns, add `REF_PERIOD` |
# MAGIC | `column_descriptions()` | descriptions: configuration, else Statbel dictionary, else "missing" |
# MAGIC | `table_description()` | short title + DCAT title, landing page and period |
# MAGIC
# MAGIC **Typing rule by prefix** (Statbel naming convention), overridden by
# MAGIC `types:` in the configuration:
# MAGIC
# MAGIC | Prefix | Type | Note |
# MAGIC |---|---|---|
# MAGIC | `CD_` code, `TX_` label | string | codes keep their leading zeros |
# MAGIC | `MS_` measure | bigint, or double if decimals are found | decimal comma accepted |
# MAGIC | `DT_` date | date | `dd/MM/yyyy` or `yyyy-MM-dd` |
# MAGIC | `FL_` flag | boolean | |
# MAGIC | `LVL_` level | int | |
# MAGIC | `XY_` coordinate | double | |
# MAGIC
# MAGIC A value that cannot be converted becomes NULL and is **counted** in the
# MAGIC run report (`unparsed:<column>`), never silently lost.
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

# MAGIC %run ./common

# COMMAND ----------

# MAGIC %run ./dictionary

# COMMAND ----------

import functools
import json

from pyspark.sql import types as T

PREFIX_TYPES = (
    ("CD_", "string"), ("TX_", "string"), ("MS_", "measure"),
    ("DT_", "date"), ("FL_", "boolean"), ("LVL_", "int"), ("XY_", "double"),
)
DATE_FORMATS = ("dd/MM/yyyy", "yyyy-MM-dd")
# Statbel marks a value that is not available (confidentiality) with "."
MISSING_MARKERS = (".",)
TRUE_TOKENS = ("1", "true", "t", "y", "yes", "j", "ja", "o", "oui")
FALSE_TOKENS = ("0", "false", "f", "n", "no", "nee", "non")
MISSING_DESCRIPTION = "Description missing in the Statbel dictionary"
REF_PERIOD_DESCRIPTION = (
    "Reference date or period of the data (from the project configuration)"
)
SOURCE_CRS = "EPSG:3812"   # Belgian Lambert 2008 (Statbel geometries)
TARGET_CRS = "EPSG:4326"   # WGS 84 (BigQuery GEOGRAPHY)


def sanitize_name(name):
    """Upper-case column name; characters outside A-Z, 0-9, _ become _."""
    clean = re.sub(r"[^A-Z0-9_]+", "_", name.strip().upper())
    return re.sub(r"_+", "_", clean).strip("_") or "COLUMN"


def bronze_names(table):
    """Bronze table names of a table: one per source file."""
    base = f"bronze_{table['table']}"
    if len(table["sources"]) == 1:
        return [base]
    return [f"{base}_{index + 1}" for index in range(len(table["sources"]))]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Landing and bronze
# MAGIC
# MAGIC GeoJSON files are read with Spark: one row per *feature*, its
# MAGIC properties as columns and its geometry as a GeoJSON text column
# MAGIC `GEOMETRY` (still in the source coordinate system).

# COMMAND ----------

def read_geojson(path):
    """Read a GeoJSON FeatureCollection: properties + GEOMETRY (text)."""
    features = (
        spark.read.option("multiLine", True).json(path)
        .select(F.explode("features").alias("feature"))
    )
    flat = features.select(
        "feature.properties.*",
        F.to_json("feature.geometry").alias("GEOMETRY"),
    )
    return flat.select(
        [F.col(f"`{c}`").cast("string").alias(c) for c in flat.columns]
    )


def load_bronze(table):
    """Download the sources of a table and write its bronze tables."""
    landing_dir = f"{LANDING_VOLUME}/{table['table']}"
    for name, source in zip(bronze_names(table), table["sources"]):
        if source["format"] == "zip_geojson":
            path = download_zip(source["url"], landing_dir, (".geojson",))
            raw = read_geojson(path)
        elif source["format"] == "zip_text":
            path = download_zip(source["url"], landing_dir)
            raw = read_statbel_text(path, source["separator"])
        else:
            raise ValueError(f"{table['table']}: unknown format "
                             f"{source['format']}")
        rows = write_table(add_lineage_columns(raw, source["url"]), name)
        report(table["table"], "rows_bronze", rows, source["url"])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver

# COMMAND ----------

def _prepare_source(bronze_name, table, source):
    """Clean names, filter and add the constant columns of one source."""
    df = spark.table(table_name(bronze_name))
    columns = []
    for column in df.columns:
        if column.startswith("_"):  # lineage columns stay in bronze
            continue
        new_name = sanitize_name(column)
        new_name = table["rename"].get(new_name, new_name)
        columns.append(blank_to_null(column).alias(new_name))
    df = df.select(*columns)
    if table.get("filter"):
        df = df.filter(table["filter"])
    for column, value in source["constants"].items():
        df = df.withColumn(column, F.lit(str(value)))
    return df


def _measure_types(df, columns):
    """bigint for whole numbers, double as soon as one value has decimals."""
    if not columns:
        return {}
    not_whole = df.agg(*[
        F.sum(F.when(
            F.col(c).isNotNull()
            & ~F.regexp_replace(c, ",", ".").rlike(r"^-?\d+$"), 1
        ).otherwise(0)).alias(c)
        for c in columns
    ]).first()
    return {c: "double" if not_whole[c] else "bigint" for c in columns}


def column_type(column, table, measure_types):
    """Spark type of a silver column (configuration, else prefix rule)."""
    if column in table["types"]:
        return table["types"][column]
    for prefix, spark_type in PREFIX_TYPES:
        if column.startswith(prefix):
            if spark_type == "measure":
                return measure_types.get(column, "bigint")
            return spark_type
    return "string"


def _typed(column, spark_type):
    """Expression converting a text column to its type (NULL if invalid)."""
    name = f"`{column}`"
    if spark_type == "string":
        return F.col(name)
    if spark_type in ("int", "bigint", "double"):
        return F.expr(f"try_cast(replace({name}, ',', '.') AS {spark_type})")
    if spark_type == "date":
        return F.coalesce(*[
            F.expr(f"try_to_timestamp({name}, '{date_format}')")
            for date_format in DATE_FORMATS
        ]).cast("date")
    if spark_type == "boolean":
        value = F.lower(F.col(name))
        return (F.when(value.isin(*TRUE_TOKENS), True)
                .when(value.isin(*FALSE_TOKENS), False))
    raise ValueError(f"Unknown type {spark_type} for {column}")


@F.udf(T.StringType())
def reproject_geojson(text):
    """Reproject a GeoJSON geometry from Lambert 2008 to WGS 84."""
    if text is None:
        return None
    from pyproj import Transformer  # imported on the Spark workers
    transformer = Transformer.from_crs(SOURCE_CRS, TARGET_CRS,
                                       always_xy=True)

    def convert(coordinates):
        if coordinates and isinstance(coordinates[0][0], (int, float)):
            xs, ys = zip(*[(point[0], point[1]) for point in coordinates])
            lons, lats = transformer.transform(xs, ys)
            return [[round(x, 7), round(y, 7)] for x, y in zip(lons, lats)]
        return [convert(part) for part in coordinates]

    geometry = json.loads(text)
    if geometry["type"] == "Point":
        lon, lat = transformer.transform(*geometry["coordinates"][:2])
        geometry["coordinates"] = [round(lon, 7), round(lat, 7)]
    else:
        geometry["coordinates"] = convert(geometry["coordinates"])
    return json.dumps(geometry, separators=(",", ":"))


def build_silver(table):
    """Return the silver DataFrame of a table, built from its bronze tables.

    Steps: stack the sources by column name, filter, keep the latest
    period if asked, type every column, add REF_PERIOD, reproject the
    GeoJSON geometry. Values that cannot be converted are reported.
    """
    frames = [
        _prepare_source(name, table, source)
        for name, source in zip(bronze_names(table), table["sources"])
    ]
    df = functools.reduce(
        lambda left, right: left.unionByName(right,
                                             allowMissingColumns=True),
        frames,
    )
    if table.get("keep_latest"):
        latest = df.agg(F.max(table["keep_latest"])).first()[0]
        df = df.filter(F.col(table["keep_latest"]) == latest)
        report(table["table"], "kept_latest", 1,
               f"{table['keep_latest']} = {latest}")

    measures = [c for c in df.columns
                if c.startswith("MS_") and c not in table["types"]]
    measure_types = _measure_types(df, measures)
    types = {c: column_type(c, table, measure_types) for c in df.columns}

    # Typed columns, plus two flags per typed column: value marked as not
    # available by Statbel ("."), or value lost because not convertible?
    typed_columns = [c for c, t in types.items() if t != "string"]
    flags = []
    for c in typed_columns:
        raw = F.col(f"`{c}`")
        marked = raw.isin(*MISSING_MARKERS)
        flags.append(marked.cast("int").alias(f"__missing_{c}"))
        flags.append((raw.isNotNull() & ~marked & _typed(c, types[c]).isNull())
                     .cast("int").alias(f"__lost_{c}"))
    typed = df.select(
        *[_typed(c, t).alias(c) for c, t in types.items()], *flags
    )
    if typed_columns:
        counts = typed.agg(*[
            F.sum(f"__{kind}_{c}").alias(f"{kind}:{c}")
            for c in typed_columns for kind in ("missing", "lost")
        ]).first().asDict()
        for c in typed_columns:
            lost = counts[f"lost:{c}"] or 0
            missing = counts[f"missing:{c}"] or 0
            if lost:
                print(f"WARNING {table['table']}.{c}: {lost:,} value(s) "
                      f"could not be converted to {types[c]}")
            report(table["table"], f"unparsed:{c}", lost, types[c])
            if missing:
                report(table["table"], f"missing_value:{c}", missing,
                       "marked '.' (not available) in the Statbel file")
    silver = typed.select(*types).withColumn(
        "REF_PERIOD", F.lit(str(table["period"])))

    geography = table.get("geography") or {}
    if "geojson" in geography:
        silver = silver.withColumn(
            geography["geojson"], reproject_geojson(geography["geojson"]))
    return silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Documentation from the configuration and the Statbel catalog
# MAGIC
# MAGIC Column descriptions, in this order of priority:
# MAGIC 1. `column_descriptions` of the configuration;
# MAGIC 2. the table's own Statbel dictionary (`dictionary`, read in
# MAGIC    `meta_field` by notebook `01`, or directly if absent), with the
# MAGIC    `dictionary_aliases` when the file and the dictionary use different
# MAGIC    names;
# MAGIC 3. otherwise *"Description missing in the Statbel dictionary"*: a gap
# MAGIC    is reported, never hidden.

# COMMAND ----------

def dictionary_descriptions(table):
    """Return {FIELD_NAME: description} from the table's Statbel dictionary."""
    url = table.get("dictionary")
    if not url:
        return {}
    if table_exists("meta_field"):
        rows = (spark.table(table_name("meta_field"))
                .filter(F.col("DICTIONARY_URL") == url)
                .select("FIELD_NAME", "DESCRIPTION").collect())
        if rows:
            return {row.FIELD_NAME.upper(): row.DESCRIPTION for row in rows}
    # Not in the catalog tables: read the dictionary directly
    path = download_file(url, f"{LANDING_VOLUME}/{table['table']}",
                         "field_dictionary.xlsx")
    return best_descriptions(read_dictionary(url, path))


def column_descriptions(table, columns):
    """Return {column: description} for the silver columns of a table."""
    from_dictionary = dictionary_descriptions(table)
    descriptions = {}
    for column in columns:
        dictionary_name = table["dictionary_aliases"].get(column, column)
        descriptions[column] = (
            table["column_descriptions"].get(column)
            or from_dictionary.get(dictionary_name.upper())
            or (REF_PERIOD_DESCRIPTION if column == "REF_PERIOD" else None)
            or MISSING_DESCRIPTION
        )
    missing = [c for c, d in descriptions.items() if d == MISSING_DESCRIPTION]
    report(table["table"], "columns_without_description", len(missing),
           ", ".join(missing) or None)
    return descriptions


def table_description(table):
    """Short title, DCAT title and landing page (if known), period."""
    parts = [f"{table['title']}."]
    if table_exists("meta_distribution") and table_exists("meta_dataset"):
        urls = [source["url"] for source in table["sources"]]
        found = (
            spark.table(table_name("meta_distribution"))
            .filter(F.col("DOWNLOAD_URL").isin(urls))
            .join(spark.table(table_name("meta_dataset")), "DATASET_ID")
            .select("TITLE_EN", "LANDING_PAGE_EN").limit(1).collect()
        )
        if found and found[0].TITLE_EN:
            page = found[0].LANDING_PAGE_EN or "statbel.fgov.be"
            parts.append(f"Statbel dataset: {found[0].TITLE_EN} ({page}).")
    parts.append(f"Period: {table['period']}.")
    return " ".join(parts)
