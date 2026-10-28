# Source inspection (step 1 of CLAUDE.md §14)

Inspected on 2026-10-28 by reading the remote zip files (HTTP range requests: header and first
rows only, except the two small reference files read in full). Nothing was created in the cloud.

## 1. Files, sizes, encodings

| # | Table | Archive | Uncompressed | Encoding | Notes |
|---|---|---|---|---|---|
| 1 | `ref_municipality` | 0.02 MB | 0.2 MB txt | **cp1252** | 2 782 rows: all levels **and history** (pre-1977 municipalities) |
| 2 | `ref_nuts_lau` | 0.02 MB | 0.1 MB txt | utf-8 | |
| 3 | `ref_statistical_sector` | 59 MB | **228 MB geojson** | utf-8 | EPSG:3812 multipolygons; names already in the GeoJSON properties |
| 4 | `ref_address` | **212 MB** | **865 MB txt** | utf-8 | ≈ 5–6 M rows (estimate). See §4: size rule of §12 |
| 5 | `ref_nace_bel` | 0.1 MB | 0.4 MB txt | utf-8 | 1 939 codes, 5 levels, `CD_NACEBEL` unique |
| 6 | `ref_country` | – | – | – | EU Vocabularies authority table, URL to confirm |
| 7 | `ref_first_name` | 1.0 MB (F) + male | 8.1 MB txt (F) | utf-8 | |
| 8 | `ref_family_name` | 2.6 MB | 18.9 MB txt | utf-8 | |
| 9 | `pop_structure` | 2.6 MB | 104 MB txt | utf-8 | |
| 10 | `pop_by_sector` | 0.5 MB | 2.4 MB txt | **cp1252** | decimal comma, one invalid column name |
| 11 | `pop_income_by_sector` | 11 MB | 35 MB txt | utf-8 | years 2005–2023 |
| 12 | `eco_vat_enterprise` | 34 MB | 354 MB txt | utf-8 | `-----` = abroad |
| 13 | `eco_bankruptcy` | 5.2 MB | 131 MB txt | utf-8 | `????` = unknown activity |
| 14 | `mob_road_accident` | 0.5 MB | 15.6 MB txt | utf-8 | |
| 15 | `bld_building_stock` | 13 MB | **473 MB txt** | **cp1252** | years 1995–2025, every REFNIS level incl. Belgium `01000` |

> ⚠️ **Encodings above are not reliable.** They were guessed from the first 3 000 bytes only. The
> first-name files, marked utf-8, are in fact **cp1252** (accented names appear further in the file):
> read as UTF-8 they produced 375 false duplicate keys (`Irène`/`Irène` → `Ir�ne`). The pipeline
> must detect the encoding on the **whole** file (strict UTF-8 decode, else cp1252) and fail on `�`.

All text files are pipe-separated (`|`) with a header row. Dates are `dd/mm/yyyy` except accidents
(`yyyy-mm-dd`). Every source server accepts range requests.

## 2. Real column names vs the brief (section 5)

| Brief expects | Real name in the file | Table |
|---|---|---|
| `ref_municipality.CD_REFNIS`, `CD_SUP_REFNIS` | ✅ same (`-` = no parent → NULL) | 1 |
| `ref_nuts_lau.CD_REFNIS` | **`CD_MUNTY_REFNIS`** (key `CD_LAU`) | 2 |
| `ref_statistical_sector.CD_MUNTY_REFNIS`, `CD_SECTOR` | `cd_munty_refnis`, `cd_sector` (lower case in GeoJSON → upper-cased in silver) | 3 |
| `ref_address.CD_SECTOR` | **`CD_SECTOR_2024`** (dictionary says `CD_SECTOR_2023`) | 4 |
| `ref_address.CD_MUNTY_REFNIS` | **`CD_REFNIS`** | 4 |
| `ref_nace_bel.CD_NACE` | **`CD_NACEBEL`** (codes without dots: `A`, `01`, `011`, `0111`, `01110`) | 5 |
| `ref_first_name` / `ref_family_name.CD_REFNIS` | ✅ same | 7, 8 |
| `pop_structure.CD_REFNIS`, `CD_NATLTY` | ✅ same (dictionary says `CD_MUNTY_REFNIS`); `CD_NATLTY` = ISO alpha-3 (`BEL`) | 9 |
| `pop_by_sector.CD_SECTOR` | ✅ same | 10 |
| `pop_income_by_sector.CD_SECTOR` | ✅ same (+ `CD_MUNTY_REFNIS`, `CD_YEAR`) | 11 |
| `eco_vat_enterprise.CD_REFNIS`, `CD_NACE…` | **`CD_MUNTY_REFNIS`**, **`CD_NACE`** (Nace-Bel level 5, e.g. `01191`) | 12 |
| `eco_bankruptcy.CD_REFNIS`, `CD_NACE…` | **`CD_MUNTY_REFNIS`**, **`CD_NACE_REV2_CLASS`** (level 4, e.g. `9609`) | 13 |
| `mob_road_accident.CD_MUNTY_REFNIS` | ✅ same | 14 |
| `bld_building_stock.CD_REFNIS` | ✅ same (dictionary names `TF_REFNIS_NL`… differ from file `CD_REFNIS_NL`…) | 15 |

Other naming issues to handle in silver (names kept, only invalid ones changed):

- `pop_by_sector`: `OPPERVLAKKTE IN HM²` is not a valid BigQuery name → `MS_TOT_SUR` (name used by
  the dictionary); `TOTAL` kept (dictionary calls it `POPULATION`).
- `ref_first_name`, `ref_family_name`: `tx_descr_nl`, `tx_descr_fr` lower case → upper case.
- Dictionaries are matched to columns by name; when a dictionary uses another name, the column
  description says so (no silent guess).

## 3. Natural keys (checked on the small files, to test on all after loading)

| Table | Proposed key | Evidence |
|---|---|---|
| `ref_municipality` | `CD_REFNIS` **if only current units are kept**; otherwise (`CD_REFNIS`, `DT_VLDT_START`) | 9 codes appear twice (e.g. `23000` moved from province `20000` to `20001` in 1995) |
| `ref_nuts_lau` | `CD_LAU` | |
| `ref_statistical_sector` | `CD_SECTOR` | |
| `ref_address` | (`CD_REFNIS`, `CD_STR`, `TX_HO_NR`) | to test |
| `ref_nace_bel` | `CD_NACEBEL` | 1 939 rows, 1 939 distinct |
| `ref_first_name` | (`CD_REFNIS`, `CD_SEX`, `TX_FST_NAME`) | |
| `ref_family_name` | (`CD_REFNIS`, `TX_LST_NAMES`) | |
| `pop_structure` | (`CD_REFNIS`, `CD_SEX`, `CD_NATLTY`, `CD_CIV_STS`, `CD_AGE`) | |
| `pop_by_sector` | `CD_SECTOR` | |
| `pop_income_by_sector` | (`CD_YEAR`, `CD_SECTOR`) | |
| `eco_vat_enterprise` | (`CD_LGL_PSN_VAT`, `CD_NACE`, `CD_MUNTY_REFNIS`) | |
| `eco_bankruptcy` | none certain: aggregated counts (year, month, class, form, municipality, NACE, duration) | to test |
| `mob_road_accident` | none certain: aggregated counts | to test |
| `bld_building_stock` | (`CD_YEAR`, `CD_REFNIS`, `CD_STAT_TYPE`, `CD_BUILDING_TYPE`) | |

## 4. Expected orphans (reported, never dropped)

- `ref_address.CD_SECTOR_2024` → sectors **2026**: codes changed between editions.
- `pop_income_by_sector.CD_SECTOR` (2005–2023) → sectors 2026: old sectors.
- `eco_vat_enterprise.CD_MUNTY_REFNIS = '-----'` (abroad).
- `eco_bankruptcy.CD_NACE_REV2_CLASS = '????'` (unknown activity).
- `bld_building_stock.CD_REFNIS = '01000'` (Belgium is not in the REFNIS list).
- Municipalities merged on 2025-01-01 if a file uses older codes.

## 5. Decisions needed from the owner

1. **`ref_municipality`**: ✅ **decided 2026-10-28: current units only** (`DT_VLDT_END = 31/12/9999`,
   621 rows, PK `CD_REFNIS`). History is not loaded.
2. **`ref_address`**: 865 MB of text, so probably about 1 GB in BigQuery with the `GEOGRAPHY` column.
   Load it in full, or drop the Lambert 72 / EPSG:3035 coordinate pairs to stay under 1 GB?
3. **`bld_building_stock`**: the 2025 edition is a series from 1995 to 2025. Keep only the latest year
   (`REF_PERIOD = 2025`) **(recommended, in line with "latest edition only")**, or the full series?
4. **`ref_statistical_sector`**: use the GeoJSON only (it already holds the names in FR/NL/DE)
   and skip `Nis9_Nis6_refnis_names_01012026.xlsx` **(recommended)**?
5. **`ref_country`**: URL of the EU Vocabularies *Country* table, to confirm before use.
