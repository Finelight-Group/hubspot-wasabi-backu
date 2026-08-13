#!/usr/bin/env python3
"""
Daily HubSpot -> Wasabi backup.

Pulls HubSpot CRM records (standard objects + all custom object types the
private app token can see) and uploads each object type to a Wasabi bucket
(S3-compatible) as a .jsonl file the moment that object type finishes —
not batched into one archive at the end of the run. That way, if the
process dies partway through (e.g. on a very large table), everything
that already finished is safely in Wasabi rather than lost with the rest
of the run. Objects are processed roughly smallest-to-largest, with
"contacts" (by far the biggest table) run last for the same reason.

First run (or when FULL_SYNC=true) does a complete export of every record.
Subsequent runs only pull records modified since the last successful run,
using a small state file stored in the same Wasabi bucket. The watermark
only advances if every object type in the run succeeded.

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

Output format:
    Each object type is written as a .jsonl file (one JSON record per line)
    rather than a single JSON array. This is deliberate: earlier versions
    built the full record list in memory before writing it out, which is
    fine for small object types but can exceed the GitHub Actions runner's
    memory limit (8 GB on private repos) for very large tables like
    contacts. Streaming one record at a time keeps memory usage bounded to
    roughly one page (100 records) regardless of table size. Each file is
    uploaded to Wasabi as soon as it's written, then deleted locally, so
    disk usage also stays bounded regardless of how many object types a
    portal has.
"""

import json
import logging
import os
import shutil
import sys
import time
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

# "contacts" is deliberately not in this list — it's the biggest table by far
# (over a million rows) and is appended at the very end of the run order in
# main(), after everything else (including custom objects) has already
# uploaded. That way, if something kills the process on contacts, every
# other object type is already safely sitting in Wasabi instead of being
# lost along with it.
STANDARD_OBJECTS = [
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
    last_network_error = None
    for attempt in range(6):
        try:
            resp = requests.request(method, url, headers=HEADERS, timeout=30, **kwargs)
        except requests.exceptions.RequestException as e:
            # Covers connection resets, timeouts, DNS blips, TLS hiccups, etc.
            # These are transient network-level failures, not HTTP error
            # responses, so they never reach resp.raise_for_status() below —
            # without this, a single dropped connection deep into a large
            # object type would crash the whole script uncaught instead of
            # just retrying. Backoff and retry the same as a 429.
            last_network_error = e
            wait = 2 ** attempt
            log.warning(f"Network error calling {path} ({e}), retrying in {wait}s")
            time.sleep(wait)
            continue
        if resp.status_code == 429:
            wait = 2 ** attempt
            log.warning(f"Rate limited on {path}, sleeping {wait}s")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    if last_network_error:
        raise RuntimeError(f"Repeated network errors on {path}, giving up: {last_network_error}")
    raise RuntimeError(f"Repeated rate limiting on {path}, giving up")


def discover_custom_object_types():
    data = hubspot_request("GET", "/crm/v3/schemas")
    return [s["objectTypeId"] for s in data.get("results", []) if s.get("objectTypeId")]


def fetch_object_properties(object_type):
    data = hubspot_request("GET", f"/crm/v3/properties/{object_type}")
    return [p["name"] for p in data.get("results", [])]


PROGRESS_LOG_EVERY = 1000  # log a progress line every N records, not just at the end


def list_all_records_streaming(object_type, properties, out_file):
    """Full export via the plain list endpoint (no 10k result cap).

    Writes one JSON record per line directly to out_file as each page
    arrives, instead of accumulating everything in memory first. This is
    what keeps a table with hundreds of thousands of rows from exhausting
    the runner's memory.
    """
    count = 0
    after = None
    while True:
        params = {"limit": 100, "properties": ",".join(properties[:200])}
        if after:
            params["after"] = after
        data = hubspot_request("GET", f"/crm/v3/objects/{object_type}", params=params)
        for record in data.get("results", []):
            out_file.write(json.dumps(record))
            out_file.write("\n")
            count += 1
            if count % PROGRESS_LOG_EVERY == 0:
                log.info(f"{object_type}: {count} records so far...")
        paging = data.get("paging")
        if paging and paging.get("next"):
            after = paging["next"]["after"]
        else:
            break
    return count


def search_modified_since_streaming(object_type, properties, since_iso, out_file):
    """Incremental export via the search endpoint (supports filtering, capped at 10k results).

    Same streaming approach as list_all_records_streaming, for the same
    memory-safety reason — a very active day of changes could still be a
    lot of records for a busy portal.
    """
    count = 0
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
        for record in data.get("results", []):
            out_file.write(json.dumps(record))
            out_file.write("\n")
            count += 1
            if count % PROGRESS_LOG_EVERY == 0:
                log.info(f"{object_type}: {count} records so far...")
        paging = data.get("paging")
        if paging and paging.get("next"):
            after = paging["next"]["after"]
        else:
            break
    if count >= 10000:
        log.warning(
            f"{object_type}: hit or near the 10k search cap ({count} records) — "
            "some changes may be missing. Consider chunking the incremental window."
        )
    return count


def upload_manifest(s3, manifest):
    """Overwrite the manifest in Wasabi after every object type, not just at the
    end — so even a run that dies partway through leaves an accurate record in
    Wasabi of exactly what did and didn't make it."""
    key = f"hubspot-backups/{RUN_DATE}/_manifest.json"
    s3.put_object(
        Bucket=WASABI_BUCKET,
        Key=key,
        Body=json.dumps(manifest, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def export_object_type(object_type, since_iso, s3):
    try:
        props = fetch_object_properties(object_type)
    except (requests.HTTPError, RuntimeError) as e:
        log.warning(f"Skipping {object_type}: could not fetch properties ({e})")
        return None
    # hs_lastmodifieddate isn't a real property on every object type (e.g. some
    # custom objects use hs_lastmodifieddate too, but guard just in case).
    if "hs_lastmodifieddate" not in props:
        props.append("hs_lastmodifieddate")

    out_path = WORKDIR / f"{object_type}.jsonl"
    try:
        with out_path.open("w") as out_file:
            if since_iso:
                count = search_modified_since_streaming(object_type, props, since_iso, out_file)
            else:
                count = list_all_records_streaming(object_type, props, out_file)
    except (requests.HTTPError, RuntimeError) as e:
        log.warning(f"Skipping {object_type}: fetch failed ({e})")
        out_path.unlink(missing_ok=True)
        return None

    log.info(f"{object_type}: exported {count} records, uploading to Wasabi...")

    # Upload this object type's file the moment it's done, rather than waiting
    # for every other object type to finish first. This is the key change: if
    # the process dies later (e.g. on contacts), everything uploaded so far is
    # already safe in Wasabi instead of being lost with the rest of the run.
    key = f"hubspot-backups/{RUN_DATE}/{object_type}.jsonl"
    try:
        s3.upload_file(str(out_path), WASABI_BUCKET, key)
    except Exception as e:
        log.warning(f"{object_type}: exported {count} records but upload to Wasabi failed: {e}")
        return None
    log.info(f"{object_type}: uploaded to wasabi://{WASABI_BUCKET}/{key}")

    # Free the local disk copy now that it's safely uploaded.
    out_path.unlink(missing_ok=True)
    return count


def main():
    WORKDIR.mkdir(parents=True, exist_ok=True)
    s3 = s3_client()

    since_iso = None if FULL_SYNC else get_last_run_timestamp(s3)
    mode = "FULL" if since_iso is None else f"INCREMENTAL since {since_iso}"
    log.info(f"Starting HubSpot backup ({mode})")

    # Run order: every standard object except contacts, then every custom
    # object type, then contacts last of all. Contacts is by far the biggest
    # table (over a million rows) — putting it last means a failure there
    # doesn't take the rest of the backup down with it.
    object_types = list(STANDARD_OBJECTS)
    try:
        custom_types = discover_custom_object_types()
        log.info(f"Discovered {len(custom_types)} custom object type(s): {custom_types}")
        object_types += custom_types
    except (requests.HTTPError, RuntimeError) as e:
        log.warning(f"Could not discover custom objects (check private app scopes): {e}")
    object_types.append("contacts")

    manifest = {"run_date": RUN_DATE, "mode": mode, "object_counts": {}, "complete": False}
    all_succeeded = True
    for obj_type in object_types:
        count = export_object_type(obj_type, since_iso, s3)
        manifest["object_counts"][obj_type] = count
        if count is None:
            all_succeeded = False
        upload_manifest(s3, manifest)

    manifest["complete"] = True
    upload_manifest(s3, manifest)

    # Only advance the incremental watermark once every object type in this
    # run succeeded. If anything was skipped, the next run stays in
    # incremental-since-last-known-good mode (or full, if there's no
    # watermark yet) so nothing quietly falls through the gap.
    if all_succeeded:
        now_iso = datetime.now(timezone.utc).isoformat()
        set_last_run_timestamp(s3, now_iso)
    else:
        log.warning("One or more object types failed — not advancing the incremental watermark.")

    shutil.rmtree(WORKDIR, ignore_errors=True)
    log.info("Backup complete." if all_succeeded else "Backup finished with some object types skipped — see manifest.")


if __name__ == "__main__":
    main()
