# Configuration

Two files, two purposes:

| File | In git | Content |
|---|---|---|
| [`config/datasets.yml`](../config/datasets.yml) | yes | **what** to load: the Statbel tables, their sources, keys, dictionaries |
| `environment.yml` | **no** | **where** to load: your Google Cloud project, BigQuery dataset, secret scope. Written by `00_setup` into `/Volumes/<catalog>/<schema>/config/`, following [`config/environment.example.yml`](../config/environment.example.yml) |

## Choose the tables

Each entry of `datasets.yml` has `enabled: true|false`. Notebooks `02` and
`03` process the enabled tables, unless their widget `tables` gives an
explicit list (`ref_municipality, ref_first_name`) or `all`.

## Describe a table

```yaml
  - table: pop_by_sector                 # BigQuery / silver name
    enabled: true
    domain: pop                          # label "domain"
    title: Population by statistical sector
    period: "2025-01-01"                 # column REF_PERIOD, label "period"
    sources:                             # one or more files, stacked
      - url: https://statbel.fgov.be/.../OPENDATA_SECTOREN_2025_NEW.zip
        # format: zip_text (default) | zip_geojson
        # separator: "|" (default)
        # constants: {CD_SEX: F}         # columns added to this source
    rename:                              # after sanitising the names
      OPPERVLAKKTE_IN_HM: MS_TOT_SUR
    types:                               # override the prefix rule
      TOTAL: bigint
    # filter: "DT_VLDT_END = '31/12/9999'"   # SQL on the raw text values
    # keep_latest: CD_YEAR                   # only the highest value
    dictionary: https://statbel.fgov.be/.../Columns%20description2020.xlsx
    dictionary_aliases:                  # column -> name in the dictionary
      TOTAL: POPULATION
    # column_descriptions: {COLUMN: text}    # win over the dictionary
    primary_key: [CD_SECTOR]
    foreign_keys:
      - {column: CD_SECTOR, references: ref_statistical_sector, referenced_column: CD_SECTOR}
    # geography:
    #   geojson: GEOMETRY                    # or point: {longitude: X, latitude: Y}
```

### Column names and types

Names are upper-cased; characters other than `A-Z`, `0-9` and `_` become
`_` (`OPPERVLAKKTE IN HM²` → `OPPERVLAKKTE_IN_HM`, then renamed).

| Prefix | Type |
|---|---|
| `CD_` code, `TX_` label | string (codes keep their leading zeros) |
| `MS_` measure | bigint, or double if decimals are found (decimal comma accepted) |
| `DT_` date | date (`dd/MM/yyyy` or `yyyy-MM-dd`) |
| `FL_` flag | boolean |
| `LVL_` level | int |
| `XY_` coordinate | double |
| other | string |

`-` and empty values become NULL. In typed columns, `.` (Statbel's "not
available") becomes NULL and is counted as `missing_value:<column>`. A value
that cannot be converted becomes NULL and is counted as
`unparsed:<column>` in `run_report`.

### Column descriptions

1. `column_descriptions` of the configuration;
2. the table's Statbel dictionary (`dictionary`), through `meta_field`
   (notebook 01) or read directly, with `dictionary_aliases`;
3. otherwise *"Description missing in the Statbel dictionary"*.

### Keys

- `primary_key` is a **candidate**: it is declared (Unity Catalog and
  BigQuery, *NOT ENFORCED*) only if the silver table has no duplicate and no
  NULL on it. Otherwise the run report says why.
- `foreign_keys` are checked (orphan rows counted) and declared after all
  tables are loaded, if the referenced table is present.

## Settings (`environment.yml`)

| Key | Meaning |
|---|---|
| `bigquery.project_id`, `dataset`, `location` | target of the publication |
| `bigquery.max_table_size_gb` | tables whose estimated BigQuery size is above are skipped (widget `allow_large` to force) |
| `secrets.scope`, `secrets.bigquery_key` | where the service account key is stored (names only) |
| `statbel.dcat_url`, `statbel.user_agent` | Statbel catalog and HTTP client name |
| `licence.data_line` | sentence appended to every table description |

Run `00_setup` again to change them.
