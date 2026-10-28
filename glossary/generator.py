"""Generate the OpenMetadata glossary import files from glossary/terms.yml.

The business glossary (5 glossaries, 127 terms, guideline DG-GDL-001) is
kept in one YAML file. This module turns it into the files imported in
OpenMetadata 2.0:

    <out>/pass1-terms/*.csv           every term, without related terms
    <out>/pass2-related-terms/*.csv   the terms that have related terms
    <out>/setup.json                  TermType tags, domains and glossaries
                                      to create before the import
    <out>/glossary_master.csv         one row per term, with the Statbel
                                      source columns it explains
    <out>/stats.json                  counts (terms, columns, matches)

Two passes are needed because OpenMetadata validates a related term when
the row is imported: it must already exist, possibly in another glossary.
Pass 2 re-imports the same terms with their related terms; they are
updated, not duplicated.

Each term may carry a `source_pattern`: a regular expression matched
against the column names of the Statbel field dictionaries. The matching
columns are written in the term description as mapping evidence. The
column names come from:

* the snapshot `glossary/statbel_columns.json` (default, offline), or
* the Unity Catalog table `meta_field` (notebook 08_glossary_import_files).

Usage, from the repository root (Python 3.9+, PyYAML):

    python glossary/generator.py
    python glossary/generator.py --out /tmp/glossary

(c) 2026 Olivier Bouchez - CC BY-NC 4.0 - https://olivier-bouchez.com
"""

import argparse
import collections
import csv
import json
import os
import re

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_TERMS = os.path.join(HERE, "terms.yml")
DEFAULT_COLUMNS = os.path.join(HERE, "statbel_columns.json")
DEFAULT_OUT = os.path.join(HERE, "openmetadata-import")

# Glossary import format of OpenMetadata 2.0.x (tested on 2.0.6).
HEADER = [
    "parent", "name*", "displayName", "description", "synonyms",
    "relatedTerms", "references", "tags", "reviewers", "owner",
    "glossaryStatus", "color", "iconURL", "domains", "extension",
]
PASSES = (("pass1-terms", False), ("pass2-related-terms", True))
INITIAL_STATUS = "Draft"

TERM_KEYS = {
    "glossary", "path", "name", "type", "definition", "synonyms",
    "related", "source_pattern", "tags", "references", "steward_note",
}
TERM_TYPES = {
    "SubjectArea": "Grouping used to navigate; carries no data itself.",
    "BusinessObject": "A thing with its own identity and life cycle.",
    "Characteristic": "A property that describes a business object.",
    "Role": "A shared object seen from one business context.",
    "Measure": "A quantity the business reads, with unit and aggregation.",
    "Classification": "An official or managed list of codes.",
    "Identifier": "A code that identifies one instance.",
}
MAX_COLUMN_LENGTH = 40  # longer "names" are sentences, not columns
MAX_LISTED_COLUMNS = 12  # source columns written in a description


# ---------------------------------------------------------------- reading

def load_glossary(path=DEFAULT_TERMS):
    """Read terms.yml and return the checked glossary model.

    Returns:
        dict with `glossaries` (list, in import order), `references`
        (key -> {title, url}) and `terms` (list of dicts). Each term gets
        the computed keys `glossary_name`, `fqn` and `level`.

    Raises:
        ValueError: unknown key, glossary, reference or term type,
            duplicate term, or related term that does not exist.
    """
    with open(path, encoding="utf-8") as yaml_file:
        content = yaml.safe_load(yaml_file)
    glossaries = content["glossaries"]
    by_key = {glossary["key"]: glossary for glossary in glossaries}
    references = content.get("references") or {}

    terms = []
    for entry in content["terms"]:
        unknown = set(entry) - TERM_KEYS
        if unknown:
            raise ValueError(f"{entry.get('name')}: unknown keys {unknown}")
        if entry["glossary"] not in by_key:
            raise ValueError(f"{entry['name']}: unknown glossary "
                             f"{entry['glossary']}")
        term_type = entry["type"].replace(" ", "")
        if term_type not in TERM_TYPES:
            raise ValueError(f"{entry['name']}: unknown type {entry['type']}")
        missing = set(entry.get("references") or []) - set(references)
        if missing:
            raise ValueError(f"{entry['name']}: unknown references {missing}")
        term = {
            "glossary": entry["glossary"],
            "path": list(entry.get("path") or []),
            "name": entry["name"],
            "type": entry["type"],
            "definition": entry["definition"].strip(),
            "synonyms": list(entry.get("synonyms") or []),
            "related": list(entry.get("related") or []),
            "source_pattern": entry.get("source_pattern"),
            "tags": list(entry.get("tags") or []),
            "references": list(entry.get("references") or []),
            "steward_note": (entry.get("steward_note") or "").strip(),
        }
        glossary_name = by_key[term["glossary"]]["name"]
        term["glossary_name"] = glossary_name
        term["fqn"] = ".".join([glossary_name] + term["path"] + [term["name"]])
        term["level"] = len(term["path"]) + 1
        terms.append(term)

    check_terms(terms)
    return {"glossaries": glossaries, "references": references,
            "terms": terms}


def check_terms(terms):
    """Check that names are unique and that parents and related terms exist."""
    fqns = collections.Counter(term["fqn"] for term in terms)
    duplicates = [fqn for fqn, count in fqns.items() if count > 1]
    if duplicates:
        raise ValueError(f"Duplicate terms: {duplicates}")
    for term in terms:
        if term["path"]:
            parent = ".".join([term["glossary_name"]] + term["path"])
            if parent not in fqns:
                raise ValueError(f"{term['fqn']}: parent {parent} not found")
        for related in term["related"]:
            if related not in fqns:
                raise ValueError(f"{term['fqn']}: related term {related} "
                                 "not found")


def normalise_columns(rows):
    """Group column names by field dictionary.

    Args:
        rows: iterable of (dictionary_url, column_name) pairs, e.g. the
            rows of the Unity Catalog table meta_field.

    Returns:
        dict dictionary_url -> set of upper-case column names. Empty
        values and texts longer than MAX_COLUMN_LENGTH are ignored.
    """
    columns = collections.defaultdict(set)
    for dictionary_url, column_name in rows:
        name = str(column_name or "").strip().upper()
        if name and len(name) <= MAX_COLUMN_LENGTH:
            columns[dictionary_url].add(name)
    return dict(columns)


def load_column_snapshot(path=DEFAULT_COLUMNS):
    """Read statbel_columns.json (dictionary URL -> column names)."""
    with open(path, encoding="utf-8") as json_file:
        content = json.load(json_file)
    return normalise_columns(
        (url, name)
        for url, names in content["dictionaries"].items()
        for name in names
    )


# ---------------------------------------------------------------- mapping

def map_source_columns(terms, columns_by_dictionary):
    """Add `source_columns` and `dictionaries` to every term.

    A column belongs to a term when its name matches the term's
    `source_pattern`. `dictionaries` counts the field dictionaries that
    contain at least one of these columns.

    Returns:
        dict of counts on the columns (for stats.json).
    """
    occurrences = collections.Counter(
        name for names in columns_by_dictionary.values() for name in names)
    matched = set()
    for term in terms:
        if not term["source_pattern"]:
            term["source_columns"], term["dictionaries"] = [], 0
            continue
        pattern = re.compile(term["source_pattern"])
        term["source_columns"] = sorted(
            name for name in occurrences if pattern.search(name))
        term["dictionaries"] = sum(
            1 for names in columns_by_dictionary.values()
            if any(pattern.search(name) for name in names))
        matched.update(term["source_columns"])
    return {
        "dictionaries": len(columns_by_dictionary),
        "column_occurrences": sum(occurrences.values()),
        "distinct_columns": len(occurrences),
        "columns_in_5_dictionaries_or_more": sum(
            1 for count in occurrences.values() if count >= 5),
        "matched_distinct_columns": len(matched),
        "matched_column_occurrences": sum(
            count for name, count in occurrences.items() if name in matched),
    }


# ---------------------------------------------------------------- writing

def description(term):
    """Term description in Markdown: definition, note, source columns."""
    text = term["definition"]
    if term["steward_note"]:
        text += "\n\n**Steward note:** " + term["steward_note"]
    if term["source_columns"]:
        listed = term["source_columns"][:MAX_LISTED_COLUMNS]
        text += "\n\n**Source columns (Statbel):** " + ", ".join(listed)
        if len(term["source_columns"]) > MAX_LISTED_COLUMNS:
            text += " …"
    return text


def tag_list(term):
    """TermType tag first, then the other tags of the term."""
    return [f"TermType.{term['type'].replace(' ', '')}"] + term["tags"]


def reference_list(term, references):
    """References as OpenMetadata expects them: title;url;title;url."""
    values = []
    for key in term["references"]:
        values += [references[key]["title"], references[key]["url"]]
    return values


def import_row(term, references, with_related):
    """One row of a glossary import file (15 columns, see HEADER).

    Root terms have an empty parent; nested terms have the fully qualified
    name of their parent term (`Glossary.Term`).
    """
    parent = (".".join([term["glossary_name"]] + term["path"])
              if term["path"] else "")
    return [
        parent,
        term["name"],
        term["name"],
        description(term),
        ";".join(term["synonyms"]),
        ";".join(term["related"]) if with_related else "",
        ";".join(reference_list(term, references)),
        ";".join(tag_list(term)),
        "", "", INITIAL_STATUS, "", "", "", "",
    ]


def glossary_file_name(position, glossary):
    """File name of a glossary, prefixed by its import order."""
    return f"{position}-{glossary['name'].replace(' ', '_')}.csv"


def write_import_files(model, out_dir):
    """Write the two passes, one CSV file per glossary.

    Returns:
        list of (pass folder, file name, number of terms).
    """
    written = []
    for folder, with_related in PASSES:
        os.makedirs(os.path.join(out_dir, folder), exist_ok=True)
        for position, glossary in enumerate(model["glossaries"], 1):
            terms = [term for term in model["terms"]
                     if term["glossary"] == glossary["key"]]
            if with_related:
                terms = [term for term in terms if term["related"]]
            file_name = glossary_file_name(position, glossary)
            path = os.path.join(out_dir, folder, file_name)
            with open(path, "w", newline="", encoding="utf-8") as csv_file:
                writer = csv.writer(csv_file, lineterminator="\n")
                writer.writerow(HEADER)
                for term in terms:
                    writer.writerow(
                        import_row(term, model["references"], with_related))
            written.append((folder, file_name, len(terms)))
    return written


def write_master_file(model, path):
    """One row per term with its type, owner and Statbel source columns."""
    owners = {glossary["key"]: glossary["owner"]
              for glossary in model["glossaries"]}
    with open(path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file, lineterminator="\n")
        writer.writerow([
            "glossary", "level", "fqn", "term", "term_type", "definition",
            "synonyms", "related_terms", "tags", "glossary_owner",
            "dictionaries_using", "source_columns",
        ])
        for term in model["terms"]:
            writer.writerow([
                term["glossary_name"], term["level"], term["fqn"],
                term["name"], term["type"], term["definition"],
                "; ".join(term["synonyms"]), "; ".join(term["related"]),
                ";".join(tag_list(term)), owners[term["glossary"]],
                term["dictionaries"], ", ".join(term["source_columns"]),
            ])


def write_json(content, path):
    """Write a JSON file (UTF-8, indented)."""
    with open(path, "w", encoding="utf-8", newline="\n") as json_file:
        json.dump(content, json_file, indent=1, ensure_ascii=False)
        json_file.write("\n")


def setup_reference(model):
    """What to create in OpenMetadata before the import."""
    return {
        "classification": {
            "name": "TermType",
            "description": "Type of a business glossary term, as defined "
                           "in DG-GDL-001 chapter 3.",
            "tags": TERM_TYPES,
        },
        "domains": sorted({glossary["domain"]
                           for glossary in model["glossaries"]}),
        "glossaries": [
            {key: glossary[key]
             for key in ("name", "domain", "owner", "description")}
            for glossary in model["glossaries"]
        ],
    }


def term_stats(terms):
    """Counts of terms by glossary, type and level."""
    return {
        "terms": len(terms),
        "terms_with_related_terms": sum(1 for t in terms if t["related"]),
        "terms_with_source_columns": sum(
            1 for t in terms if t["source_columns"]),
        "by_glossary": dict(collections.Counter(
            t["glossary_name"] for t in terms)),
        "by_type": dict(collections.Counter(t["type"] for t in terms)),
        "by_level": dict(collections.Counter(t["level"] for t in terms)),
    }


def generate(columns_by_dictionary, out_dir=DEFAULT_OUT,
             terms_path=DEFAULT_TERMS):
    """Read the glossary, map the source columns and write every file.

    Args:
        columns_by_dictionary: dict dictionary URL -> set of column names
            (load_column_snapshot() or normalise_columns()).
        out_dir: output folder (created if needed).
        terms_path: the glossary YAML file.

    Returns:
        (model, stats, list of written import files)
    """
    model = load_glossary(terms_path)
    stats = map_source_columns(model["terms"], columns_by_dictionary)
    stats.update(term_stats(model["terms"]))
    os.makedirs(out_dir, exist_ok=True)
    written = write_import_files(model, out_dir)
    write_master_file(model, os.path.join(out_dir, "glossary_master.csv"))
    write_json(setup_reference(model), os.path.join(out_dir, "setup.json"))
    write_json(stats, os.path.join(out_dir, "stats.json"))
    return model, stats, written


def main(argv=None):
    """Command line entry point."""
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--terms", default=DEFAULT_TERMS,
                        help="glossary YAML file (default: %(default)s)")
    parser.add_argument("--columns", default=DEFAULT_COLUMNS,
                        help="Statbel column snapshot (default: %(default)s)")
    parser.add_argument("--out", default=DEFAULT_OUT,
                        help="output folder (default: %(default)s)")
    args = parser.parse_args(argv)

    _, stats, written = generate(load_column_snapshot(args.columns),
                                 args.out, args.terms)
    for folder, file_name, count in written:
        print(f"{folder}/{file_name}: {count} terms")
    print(json.dumps(stats, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
