# Databricks notebook source
# MAGIC %md
# MAGIC # lib · dictionary — read Statbel field dictionaries
# MAGIC
# MAGIC Statbel describes the columns of its files in Excel **field
# MAGIC dictionaries** with about 50 different layouts. `read_dictionary()`
# MAGIC turns any of them into rows *(field, language, description)*.
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
# MAGIC
# MAGIC Requires `openpyxl` (`%pip install openpyxl`).
# MAGIC
# MAGIC © 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com

# COMMAND ----------

import re
import warnings

import openpyxl

LANGUAGES = ("en", "fr", "nl", "de")

# Field names: CD_REFNIS, TX_DESCR_FR, but also MM, YYYY, BELGIUM
FIELD_NAME = re.compile(r"^[A-Za-z][A-Za-z0-9]*(_[A-Za-z0-9]+)+$")
NAME_TOKEN = re.compile(r"^[A-Za-z][A-Za-z0-9_]*$")
NAME_WORDS = {"naam", "nom", "name", "variable", "variables", "varname"}
TEXT_WORDS = {"description", "label", "labels", "definition", "descr",
              "omschrijving", "beschrijving", "umschreibung", "beschreibung"}
SKIP_WORDS = {"type", "len", "lenght", "length", "format", "informat",
              "min", "max", "aantal", "nombre"}
LANGUAGE_WORDS = {
    "nl": {"nl", "omschrijving", "beschrijving", "nederlands"},
    "fr": {"fr", "francais", "français"},
    "de": {"de", "umschreibung", "beschreibung", "deutsch"},
    "en": {"en", "english"},
}


def _words(text):
    """Lower-case words of a header: 'TX_DESCR_FR' -> ['tx', 'descr', 'fr']."""
    return [w for w in re.split(r"[^a-zçéèë]+", text.lower()) if w]


def _header_language(header):
    """Language named in a column header (the last one wins), or None."""
    found = None
    for word in _words(header):
        for language, language_words in LANGUAGE_WORDS.items():
            if word in language_words:
                found = language
    return found


def _is_text_header(header):
    """True if a header announces a description column."""
    return bool(set(_words(header)) & TEXT_WORDS) or bool(
        _header_language(header))


def _name_score(rows, column):
    """Share of the values of a column that look like field names."""
    values = [row[column] for row in rows if row[column]]
    if not values:
        return 0
    return sum(bool(FIELD_NAME.match(v)) for v in values) / len(values)


def _parse_long_layout(header, body):
    """Layout 'Language | Name | Label | Description'."""
    lowered = [h.lower() for h in header]
    language_column = lowered.index("language")
    name_column = next(
        i for i, h in enumerate(lowered) if h in ("name", "variable"))
    # Description first, label when the description is empty
    text_columns = (
        [i for i, h in enumerate(lowered) if h.startswith("description")]
        + [i for i, h in enumerate(lowered) if h.startswith("label")]
    )
    records = []
    for row in body:
        language = row[language_column].lower()
        text = next((row[i] for i in text_columns if row[i]), "")
        if FIELD_NAME.match(row[name_column]) and language in LANGUAGES \
                and text:
            records.append((row[name_column], language, text))
    return records


def _description_columns(header, name_column):
    """Return {column index: language} of the description columns."""
    plain_order = iter(("fr", "en", "de"))  # after NL in the Statbel layout
    columns = {}
    for index, title in enumerate(header):
        if index == name_column or not title:
            continue
        if set(_words(title)) & SKIP_WORDS:
            continue
        language = _header_language(title)
        if language is None and _is_text_header(title):
            generic_english = set(_words(title)) & {"label", "labels",
                                                    "definition"}
            language = "en" if generic_english else next(plain_order, None)
        if language and language not in columns.values():
            columns[index] = language
    return columns


def parse_sheet(rows):
    """Return the (field, language, description) records of one sheet."""
    rows = [["" if v is None else str(v).strip() for v in row]
            for row in rows]
    rows = [row for row in rows if sum(1 for cell in row if cell) >= 2]
    if len(rows) < 2:
        return []
    width = max(len(row) for row in rows)
    rows = [row + [""] * (width - len(row)) for row in rows]
    header, body = rows[0], rows[1:]

    if "language" in [h.lower() for h in header]:
        return _parse_long_layout(header, body)

    if FIELD_NAME.match(header[0]) and not _is_text_header(header[1]):
        # No header row: the first row is already a field
        return [(row[0], "und", row[1]) for row in [header] + body
                if NAME_TOKEN.match(row[0]) and row[1]]

    named = [i for i, h in enumerate(header) if set(_words(h)) & NAME_WORDS]
    name_column = named[0] if named else max(
        range(width), key=lambda column: _name_score(body, column))
    if not named and _name_score(body, name_column) < 0.5:
        return []  # no column of field names: not a field dictionary

    columns = _description_columns(header, name_column)
    return [
        (row[name_column], language, row[index])
        for row in body if NAME_TOKEN.match(row[name_column])
        for index, language in columns.items() if row[index]
    ]


def read_dictionary(url, path):
    """Return the field description rows (dicts) of one dictionary file."""
    rows = []
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")  # openpyxl warns about Excel styles
        workbook = openpyxl.load_workbook(path, read_only=True,
                                          data_only=True)
        for sheet in workbook.worksheets:
            for field, language, text in parse_sheet(
                    sheet.iter_rows(values_only=True)):
                rows.append({"DICTIONARY_URL": url,
                             "SHEET_NAME": sheet.title,
                             "FIELD_NAME": field, "LANGUAGE": language,
                             "DESCRIPTION": text})
        workbook.close()
    return rows


def best_descriptions(rows):
    """Return {FIELD_NAME (upper case): description}: English, else French,
    else unknown language, else Dutch, else German (first text found)."""
    preference = ("en", "fr", "und", "nl", "de")
    best = {}
    for row in sorted(rows, key=lambda r: preference.index(r["LANGUAGE"])):
        best.setdefault(row["FIELD_NAME"].upper(), row["DESCRIPTION"])
    return best
