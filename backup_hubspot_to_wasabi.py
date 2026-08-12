#!/usr/bin/env python3
"""
Daily HubSpot -> Wasabi backup.

Pulls HubSpot CRM records (standard objects + all custom object types the
private app token can see), packages them as JSON, zips them, and uploads
the zip to a Wasabi bucket (S3-compatible).

First run (or when FULL_SYNC=true) does a complete export of every record.
Subsequent runs only pull records modified since the last successful run,
using a small state file stored in the same Wasabi bucket.

Required environment variables:
    HUBSPOT_PRIVATE_APP_TOKEN   HubSpot private app access token
    WASABI_ACCESS_KEY           Wasabi access key ID
    WASABI_SECRET_KEY           Wasabi secret access key
    WASABI_BUCKET               Target bucket name

Optional environment variables:
    WASABI_REGION               Default: us-east-1
    WASABI_ENDPOINT             Default: https://s3.<region>.wasabisys.com
    FULL_SYNC                   "true" to force a full export (default: false)
    EXTRA_STANDARD_OBJECTS      Comma-separated extra standard object names
                                 to include beyond the built-in list

Known limitations (v1 — see README for details):
    - Associations between records are not exported yet.
    - HubSpot's search endpoint (used for incremental pulls) caps a single
      query at 10,000 matching results. A single day's changes should
      never come close to that, but if you ever see a warning about it,
      the fix is to chunk the incremental window (e.g. by hour).
"""

import json
import logging
import os
import shutil
import sys
import time
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import requests
from botocore.client import Config

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("hubspot-backup")


def env(name, default=None, required=False):
    val = os.environ.get(name, default)
    if required and not val:
        log.error(f"Missing required environment variable: {name}")
        sys.exit(1)
    return val


HUBSPOT_TOKEN = env("HUBSPOT_PRIVATE_APP_TOKEN", required=True)
WASABI_ACCESS_KEY = env("WASABI_ACCESS_KEY", required=True)
WASABI_SECRET_KEY = env("WASABI_SECRET_KEY", required=True)
WASABI_BUCKET = env("WASABI_BUCKET", required=True)
WASABI_REGION = env("WASABI_REGION", "us-east-1")
WASABI_ENDPOINT = env("WASABI_ENDPOINT", f"https://s3.{WASABI_REGION}.wasabisys.com")
FULL_SYNC = env("FULL_SYNC", "false").lower() == "true"

HUBSPOT_API_BASE = "https://api.hubapi.com"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

STANDARD_OBJECTS = [
    "contacts",
    "companies",
    "deals",
    "tickets",
    "products",
    "line_items",
    "quotes",
    "calls",
    "emails",
    "meetings",
    "notes",
    "tasks",
]
extra = env("EXTRA_STANDARD_OBJECTS", "")
if extra:
    STANDARD_OBJECTS += [o.strip() for o in extra.split(",") if o.strip()]

STATE_FILE_KEY = "hubspot-backups/_state/last_run.json"
RUN_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
WORKDIR = Path("./_export") / RUN_DATE


def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=WASABI_ENDPOINT,
        aws_access_key_id=WASABI_ACCESS_KEY,
        aws_secret_access_key=WASABI_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name=WASABI_REGION,
    )


def get_last_run_timestamp(s3):
    try:
        obj = s3.get_object(Bucket=WASABI_BUCKET, Key=STATE_FILE_KEY)
        data = json.loads(obj["Body"].read())
        return data.get("last_run_iso")
    except s3.exceptions.NoSuchKey:
        return None
    except Exception as e:
        log.warning(f"Could not read state file, defaulting to full sync: {e}")
        return None


def set_last_run_timestamp(s3, iso_ts):
    s3.put_object(
        Bucket=WASABI_BUCKET,
        Key=STATE_FILE_KEY,
        Body=json.dumps({"last_run_iso": iso_ts}).encode("utf-8"),
        ContentType="application/json",
    )


def hubspot_request(method, path, **kwargs):
    url = f"{HUBSPOT_API_BASE}{path}"
    for attempt in range(6):
        resp = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)
        if resp.status_code == 429:
            wait = 2 ** attempt
            log.warning(f"Rate limited on {path}, sleeping {wait}s")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise RuntimeError(f"Repeated rate limiting on {path}, giving up")


def discover_custom_object_types():
    data = hubspot_request("GET", "/crm/v3/schemas")
    return [s["objectTypeId"] for s in data.get("results", []) if s.get("objectTypeId")]


def fetch_object_properties(object_type):
    data = hubspot_request("GET", f"/crm/v3/properties/{object_type}")
    return [p["name"] for p in data.get("results", [])]


def list_all_records(object_type, properties):
    """Full export via the plain list endpoint (no 10k result cap)."""
    records = []
    after = None
    while True:
        params = {"limit": 100, "properties": ",".join(properties[:200])}
        if after:
            params["after"] = after
        data = hubspot_request("GET", f"/crm/v3/objects/{object_type}", params=params)
        records.extend(data.get("results", []))
        paging = data.get("paging")
        if paging and paging.get("next"):
            after = paging["next"]["after"]
        else:
            break
    return records


def search_modified_since(object_type, properties, since_iso):
    """Incremental export via the search endpoint (supports filtering, capped at 10k results)."""
    records = []
    after = None
    while True:
        body = {
            "limit": 100,
            "properties": properties[:200],
            "filterGroups": [
                {
                    "filters": [
                        {
                            "propertyName": "hs_lastmodifieddate",
                            "operator": "GTE",
                            "value": since_iso,
                        }
                    ]
                }
            ],
        }
        if after:
            body["after"] = after
        data = hubspot_request("POST", f"/crm/v3/objects/{object_type}/search", json=body)
        records.extend(data.get("results", []))
        paging = data.get("paging")
        if paging and paging.get("next"):
            after = paging["next"]["after"]
        else:
            break
    if len(records) >= 10000:
        log.warning(
            f"{object_type}: hit or near the 10k search cap ({len(records)} records) — "
            "some changes may be missing. Consider chunking the incremental window."
        )
    return records


def export_object_type(object_type, since_iso):
    try:
        props = fetch_object_properties(object_type)
    except requests.HTTPError as e:
        log.warning(f"Skipping {object_type}: could not fetch properties ({e})")
        return None
    # hs_lastmodifieddate isn't a real property on every object type (e.g. some
    # custom objects use hs_lastmodifieddate too, but guard just in case).
    if "hs_lastmodifieddate" not in props:
        props.append("hs_lastmodifieddate")

    try:
        if since_iso:
            records = search_modified_since(object_type, props, since_iso)
        else:
            records = list_all_records(object_type, props)
    except requests.HTTPError as e:
        log.warning(f"Skipping {object_type}: fetch failed ({e})")
        return None

    out_path = WORKDIR / f"{object_type}.json"
    out_path.write_text(json.dumps(records, indent=2))
    log.info(f"{object_type}: exported {len(records)} records")
    return len(records)


def main():
    WORKDIR.mkdir(parents=True, exist_ok=True)
    s3 = s3_client()

    since_iso = None if FULL_SYNC else get_last_run_timestamp(s3)
    mode = "FULL" if since_iso is None else f"INCREMENTAL since {since_iso}"
    log.info(f"Starting HubSpot backup ({mode})")

    object_types = list(STANDARD_OBJECTS)
    try:
        custom_types = discover_custom_object_types()
        log.info(f"Discovered {len(custom_types)} custom object type(s): {custom_types}")
        object_types += custom_types
    except requests.HTTPError as e:
        log.warning(f"Could not discover custom objects (check private app scopes): {e}")

    manifest = {"run_date": RUN_DATE, "mode": mode, "object_counts": {}}
    for obj_type in object_types:
        count = export_object_type(obj_type, since_iso)
        manifest["object_counts"][obj_type] = count

    (WORKDIR / "_manifest.json").write_text(json.dumps(manifest, indent=2))

    zip_path = Path(f"hubspot-backup-{RUN_DATE}.zip")
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for f in WORKDIR.iterdir():
            zf.write(f, f.name)

    key = f"hubspot-backups/{RUN_DATE}/{zip_path.name}"
    s3.upload_file(str(zip_path), WASABI_BUCKET, key)
    log.info(f"Uploaded {zip_path.name} to wasabi://{WASABI_BUCKET}/{key}")

    # Only advance the incremental watermark once the upload has succeeded.
    now_iso = datetime.now(timezone.utc).isoformat()
    set_last_run_timestamp(s3, now_iso)

    shutil.rmtree(WORKDIR)
    zip_path.unlink()
    log.info("Backup complete.")


if __name__ == "__main__":
    main()
