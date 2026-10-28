# Data quality findings

What the pipeline's checks found in the real Statbel files (editions of
2025-2026). None of these is "an error of Statbel": each is a fact about the
data that a Data Steward has to know, document and decide on. They make good
material for a governance training.

| # | Table | Finding | How the pipeline handles it |
|---|---|---|---|
| 1 | first and family names | Files announced as UTF-8-like are **Windows-1252** further down. Read as UTF-8, `Irène` and `Irêne` both become `Ir�ne`: **375 false duplicate keys**. | encoding detected on the whole file, conversion to UTF-8 before Spark |
| 2 | `ref_family_name` | The natural key (municipality, name) is **not unique in the source**: *Matei* and *Bai* appear twice in Antwerp. | primary key not declared; reported (`duplicate_keys`) |
| 3 | `ref_municipality`, `ref_nuts_lau` | The files hold the **history**: merged municipalities stay with an end date. `CD_REFNIS` alone is not unique. | decision: units valid today only (filter on the end date) |
| 4 | multi-year tables (income, bankruptcies) | Old years use **municipality codes that no longer exist** (mergers of 2019 and 2025): 16,904 and 6,641 orphan rows. | orphans counted, rows kept |
| 5 | `pop_by_sector` | 402 sectors `…ZZZZ` = **"unknown sector"** of a municipality, absent from the sector reference. | orphans counted |
| 6 | `eco_vat_enterprise` | Municipality `-----` = abroad, `XXXXX` = unknown; Nace codes ending in `X` = unknown subclass. | orphans counted |
| 7 | `eco_bankruptcy` | Activity `????` = unknown (1,385 rows). | orphans counted |
| 8 | `bld_building_stock` | `01000` (Belgium) is used as a code but is not in the REFNIS list. The 2025 file holds the whole 1995-2025 series. | orphans counted; latest year kept |
| 9 | `pop_income_by_sector` | `.` marks a value **not available** (confidentiality): 62,392 values. | NULL, counted as `missing_value` (not as an error) |
| 10 | `pop_by_sector` | Column `OPPERVLAKKTE IN HM²` (space, `²`): **refused by Delta** even in bronze; decimal comma `52,98`. | bronze: only Delta-forbidden characters replaced (reported); silver: renamed `MS_TOT_SUR`, decimal comma accepted |
| 11 | `mob_road_accident` | `DT_HOUR` holds an hour (`14`), not a date, despite the `DT_` prefix. | type overridden in the configuration |
| 12 | several | The **same concept has different names**: `CD_REFNIS` / `CD_MUNTY_REFNIS`, and dictionaries that use other names than the files (`CD_SECTOR_2023` for `CD_SECTOR_2024`, `TF_REFNIS_NL` for `CD_REFNIS_NL`). | `dictionary_aliases` in the configuration |
| 13 | field dictionaries | About 50 different layouts, languages in column order (`Description \| Description` = FR then EN), some without header. One dictionary returns HTTP 404. | normalised by `lib/dictionary` (211 of 214 read) |
| 14 | `eco_vat_enterprise` | Documented in a PDF only: no machine-readable dictionary. | descriptions written in the configuration |
| 15 | Statbel web server | Returns an **HTML page** instead of the file to clients announcing `Python-urllib`. | browser-like *User-Agent*; HTML responses refused |

Figures from the local test run of 2026-10-28; the run report of each
pipeline run gives the current values.
