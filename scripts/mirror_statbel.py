"""Copy the Statbel files the pipeline needs into a Google Cloud Storage bucket (the mirror).

The serverless compute of the EU Databricks workspace (Google Cloud, europe-west3) reaches the Google APIs but not
the public internet: it cannot download from statbel.fgov.be. This script runs where internet is open (a PC, the
VM), downloads the files and stores them in the bucket; the pipeline then reads them from there when
`statbel.mirror` is set in environment.yml (see download_file in notebooks/lib/common.py).

Files copied:
- the DCAT catalog (statbel.dcat_url);
- every source and dictionary of config/datasets.yml;
- every field dictionary (xlsx, xls) listed in the DCAT catalog.

Object name: <host>/<path of the URL, decoded>, e.g. statbel.fgov.be/sites/default/files/.../TA_POP_LST_2026.zip.
Metadata of each object: source-url and source-last-modified (Last-Modified of Statbel), used by the catch-up
job (tables = stale). A file whose Last-Modified has not changed is not copied again.

Each run writes a log in the bucket, `_runs/<UTC time>.json`: every file with its outcome, and the SHA-256 of
the files copied (provenance of the data). The exit code is 1 when the DCAT catalog or a file of config/datasets.yml
failed (a broken dictionary link of the DCAT catalog is only logged).

Authentication: Application Default Credentials (gcloud auth application-default login, or the service account
of the VM) with write access to the bucket. No key in this repository.

    python scripts/mirror_statbel.py --bucket strong-aegis-512509-r4-statbel-landing
    python scripts/mirror_statbel.py --bucket ... --only-config      # without the DCAT dictionaries
    python scripts/mirror_statbel.py --bucket ... --enabled-only     # only the tables with enabled: true

The VM runs it every day (deploy/vm/statbel-mirror.*, see docs/install_eu.md) with --enabled-only, so the files
of the tables not yet approved (the address file, the candidates of DG-PRO-0045) are never refreshed by
themselves.

© 2026 Olivier Bouchez · CC BY-NC 4.0 · https://olivier-bouchez.com
"""

from __future__ import annotations

import argparse
import hashlib
import json
import socket
import sys
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote, unquote, urlsplit

import google.auth
import google.auth.transport.requests
import requests
import yaml

ROOT = Path(__file__).resolve().parents[1]
DCAT_URL = "https://doc.statbel.be/publications/DCAT/DCAT_opendata_datasets.ttl"
USER_AGENT = "Mozilla/5.0 (statbel-lakehouse)"
API = "https://storage.googleapis.com"


def object_name(url: str) -> str:
    """Object name of a URL in the mirror: host and decoded path."""
    parts = urlsplit(url)
    return parts.netloc + unquote(parts.path)


class Bucket:
    def __init__(self, name: str):
        self.name = name
        self.credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/devstorage.read_write"])
        self.session = requests.Session()

    def _headers(self) -> dict:
        if not self.credentials.valid:
            self.credentials.refresh(google.auth.transport.requests.Request())
        return {"Authorization": f"Bearer {self.credentials.token}"}

    def metadata(self, name: str) -> dict | None:
        r = self.session.get(f"{API}/storage/v1/b/{self.name}/o/{quote(name, safe='')}", headers=self._headers(),
                             timeout=60)
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json().get("metadata") or {}

    def upload(self, name: str, data: bytes, metadata: dict) -> None:
        boundary = "statbel-mirror-boundary"
        body = (f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
                + json.dumps({"name": name, "metadata": metadata})
                + f"\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n").encode() + data \
            + f"\r\n--{boundary}--".encode()
        r = self.session.post(f"{API}/upload/storage/v1/b/{self.name}/o?uploadType=multipart", data=body,
                              headers={**self._headers(), "Content-Type": f"multipart/related; boundary={boundary}"},
                              timeout=600)
        r.raise_for_status()


def config_urls(enabled_only: bool = False) -> list[str]:
    content = yaml.safe_load((ROOT / "config" / "datasets.yml").read_text(encoding="utf-8"))
    urls = []
    for table in content["tables"]:
        if enabled_only and not table.get("enabled"):
            continue
        urls += [s["url"] for s in table.get("sources") or [] if str(s.get("url", "")).startswith("http")]
        if str(table.get("dictionary", "")).startswith("http"):
            urls.append(table["dictionary"])
    return urls


def dcat_dictionaries(ttl: bytes) -> list[str]:
    """Field dictionaries (xlsx, xls) of the DCAT catalog, as notebook 01 reads them."""
    from rdflib import Graph
    from rdflib.namespace import DCAT, FOAF, RDF

    graph = Graph()
    graph.parse(data=ttl, format="turtle")
    return [str(page) for subject in graph.subjects(RDF.type, DCAT.Dataset)
            for page in graph.objects(subject, FOAF.page)
            if str(page).rsplit(".", 1)[-1].lower() in ("xlsx", "xls")]


def mirror(bucket: Bucket, url: str) -> dict:
    """Copies one file if Statbel has a newer one; returns its line of the run log."""
    name = object_name(url)
    entry = {"url": url, "object": name}
    try:
        head = requests.head(url, headers={"User-Agent": USER_AGENT}, timeout=60, allow_redirects=True)
        modified = head.headers.get("Last-Modified", "") if head.ok else ""
        current = bucket.metadata(name)
        if current is not None and modified and current.get("source-last-modified") == modified:
            return {**entry, "outcome": "unchanged", "last_modified": modified}
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=600)
        r.raise_for_status()
        if r.headers.get("Content-Type", "").startswith("text/html"):
            return {**entry, "outcome": "error: the server returned a web page"}
        modified = r.headers.get("Last-Modified", modified)
        bucket.upload(name, r.content, {"source-url": url, "source-last-modified": modified})
        return {**entry, "outcome": "copied", "last_modified": modified, "bytes": len(r.content),
                "sha256": hashlib.sha256(r.content).hexdigest()}
    except Exception as error:  # noqa: BLE001 - one file must not stop the others
        return {**entry, "outcome": f"error: {type(error).__name__}: {str(error)[:200]}"}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--dcat-url", default=DCAT_URL)
    parser.add_argument("--only-config", action="store_true", help="skip the dictionaries of the DCAT catalog")
    parser.add_argument("--enabled-only", action="store_true", help="only the tables with enabled: true")
    args = parser.parse_args()

    started = datetime.now(timezone.utc)
    bucket = Bucket(args.bucket)
    results = [mirror(bucket, args.dcat_url)]
    print(f"{args.dcat_url}: {results[0]['outcome']}")
    urls = config_urls(args.enabled_only)
    required = {args.dcat_url, *urls}
    if not args.only_config:
        ttl = requests.get(args.dcat_url, headers={"User-Agent": USER_AGENT}, timeout=300).content
        urls += dcat_dictionaries(ttl)
    urls = sorted(set(urls) - {args.dcat_url})

    with ThreadPoolExecutor(max_workers=8) as pool:
        results += list(pool.map(lambda u: mirror(bucket, u), urls))
    counts = {}
    for r in results:
        key = r["outcome"].split(":")[0]
        counts[key] = counts.get(key, 0) + 1
        if key != "unchanged":
            print(f"{r['outcome']}: {r['url']}")
    print(f"{len(results)} files: {counts}")

    log = {"started": started.isoformat(timespec="seconds"),
           "finished": datetime.now(timezone.utc).isoformat(timespec="seconds"),
           "host": socket.gethostname(), "enabled_only": args.enabled_only, "only_config": args.only_config,
           "counts": counts, "files": results}
    bucket.upload(f"_runs/{started.strftime('%Y%m%dT%H%M%SZ')}.json",
                  json.dumps(log, indent=1).encode(), {"counts": json.dumps(counts)})
    # a broken link of the DCAT catalog (dictionary missing at Statbel) is logged, not fatal: only the files the
    # pipeline loads make the run fail
    return 1 if any(r["outcome"].startswith("error") and r["url"] in required for r in results) else 0


if __name__ == "__main__":
    sys.exit(main())
