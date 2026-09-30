"""Extractor Lambda: watermark -> source API -> gzip NDJSON in S3 -> _manifest.json (written LAST).

Invoked (synchronously) by Airflow's insurance_elt_v2 with:
    {"run_id": "scheduled__2026-09-30", "business_date": "2026-09-29",
     "window_end": "2026-09-30T00:00:00"}

Per entity:
  1. read HIGH_WATERMARK from INS_RAW.CONTROL.WATERMARKS (read-only: only Airflow advances it)
  2. window = (watermark - BUFFER_HOURS, window_end]  -> the buffer re-pulls late commits
  3. page through the source API, write parts of up to PART_MAX_ROWS rows:
       raw/api/<entity>/dt=<business_date>/run=<run_id>/attempt=<utc ts>/part-00001.json.gz
Then write raw/api/_manifests/dt=<business_date>/run=<run_id>/_manifest.json - the commit
marker Airflow waits for. It lists exactly which files belong to the run.

Files are immutable: Snowpipe never reloads a file NAME it has loaded before (even if the
content changed), so a retry writes a fresh attempt=<ts>/ folder instead of overwriting.
Rows from an earlier failed attempt may also land in bronze - reconcile counts only the
manifest's files, and silver dedups on (key, updated_at).
"""

import gzip
import json
import os
import random
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

import boto3
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest

PREFIX = os.environ.get("RAW_API_PREFIX", "raw/api")
SOURCE_NAME = os.environ.get("SOURCE_NAME", "policy_admin_api")
REGION = os.environ.get("AWS_REGION", "us-east-1")
BUFFER = timedelta(hours=float(os.environ.get("BUFFER_HOURS", "2")))

ENTITIES = ("policyholders", "policies", "claims")
# The API's change timestamp per entity -> max value becomes the next watermark
CURSOR_FIELD = {"policyholders": "updated_at", "policies": "issued_at", "claims": "updated_at"}

PAGE_SIZE = 500
PART_MAX_ROWS = 10_000  # fewer, bigger files: Snowpipe charges per file, ideal is 100-250 MB
MAX_ATTEMPTS = 5
# run_id becomes part of an S3 key and is parsed back out by Snowpipe -> keep it path-safe
RUN_ID_RE = re.compile(r"^[A-Za-z0-9_.-]{1,128}$")

s3 = boto3.client("s3")  # module level: reused across warm invocations


# ---------------------------------------------------------------- Snowflake: read watermarks
def read_watermarks() -> dict[str, datetime]:
    # Imported here: cold-start cost only when used, and tests run without the connector
    import snowflake.connector

    if os.environ.get("SNOWFLAKE_AUTH", "wif") == "wif":
        # Lambda's IAM role signs the login -> matched to INS_LAMBDA_SVC's WORKLOAD_IDENTITY ARN
        auth = {"authenticator": "WORKLOAD_IDENTITY", "workload_identity_provider": "AWS"}
    else:  # local development: key-pair user (same one dbt dev uses)
        auth = {"authenticator": "SNOWFLAKE_JWT", "private_key_file": os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]}

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        role=os.environ.get("SNOWFLAKE_ROLE", "INS_EXTRACTOR"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "INS_WH"),
        **auth,
    )
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT ENTITY, HIGH_WATERMARK FROM INS_RAW.CONTROL.WATERMARKS WHERE SOURCE_NAME = %s",
            (SOURCE_NAME,),
        )
        return dict(cur.fetchall())
    finally:
        conn.close()


# ---------------------------------------------------------------- source API client
def fetch_page(params: dict) -> dict:
    """GET one page from the source API (Function URL with IAM auth -> SigV4-signed request).

    Retries 429 / 5xx / network errors with exponential backoff + full jitter.
    Any other 4xx is OUR bug (bad request) -> fail fast, retrying can't fix it.
    """
    url = f"{os.environ['SOURCE_API_URL'].rstrip('/')}/?{urllib.parse.urlencode(params)}"

    for attempt in range(1, MAX_ATTEMPTS + 1):
        # Sign every attempt: signatures expire and Lambda credentials rotate
        credentials = boto3.Session().get_credentials().get_frozen_credentials()
        signed = AWSRequest(method="GET", url=url)
        SigV4Auth(credentials, "lambda", REGION).add_auth(signed)

        retry_after = None
        try:
            request = urllib.request.Request(url, headers=dict(signed.headers))
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.loads(response.read())
        except urllib.error.HTTPError as exc:  # must come before URLError (it's a subclass)
            retryable = exc.code == 429 or exc.code >= 500
            if not retryable or attempt == MAX_ATTEMPTS:
                raise RuntimeError(f"source API {exc.code}: {exc.read().decode()[:500]}") from exc
            retry_after = exc.headers.get("Retry-After")
            reason = f"HTTP {exc.code}"
        except (urllib.error.URLError, TimeoutError) as exc:
            if attempt == MAX_ATTEMPTS:
                raise
            reason = repr(exc)

        # Full jitter: random 0..cap -> many clients don't all retry at the same moment
        delay = float(retry_after) if retry_after else random.uniform(0, min(30, 2**attempt))
        print(f"attempt {attempt}/{MAX_ATTEMPTS} failed ({reason}), retrying in {delay:.1f}s")
        time.sleep(delay)

    raise AssertionError("unreachable")


# ---------------------------------------------------------------- S3 writer
def _put_part(rows: list[dict], part_no: int, base: str) -> dict:
    # NDJSON: one object per line, stable key order -> easy to diff two files
    body = "".join(json.dumps(r, sort_keys=True, separators=(",", ":")) + "\n" for r in rows)
    key = f"{base}/part-{part_no:05d}.json.gz"
    s3.put_object(Bucket=os.environ["DATA_LAKE_BUCKET"], Key=key, Body=gzip.compress(body.encode()))
    return {"key": key, "rows": len(rows)}


def extract_entity(entity: str, since: datetime, until: datetime, base: str) -> dict:
    """Page through (since, until] and write it to S3. Returns counts, files, max cursor value."""
    cursor = CURSOR_FIELD[entity]
    files, pending, max_changed_at = [], [], None

    if since < until:  # watermark already past window_end (e.g. re-running an old run) -> nothing
        params = {
            "entity": entity,
            "updated_since": since.isoformat(),
            "until": until.isoformat(),
            "page_size": PAGE_SIZE,
        }
        while True:
            page = fetch_page(params)
            for row in page["data"]:
                # ISO-8601 strings sort like the timestamps they represent
                if max_changed_at is None or row[cursor] > max_changed_at:
                    max_changed_at = row[cursor]
            pending.extend(page["data"])

            while len(pending) >= PART_MAX_ROWS:  # while: one page can fill several parts
                files.append(_put_part(pending[:PART_MAX_ROWS], len(files) + 1, base))
                pending = pending[PART_MAX_ROWS:]
            if not page["next_page_token"]:
                break
            params["page_token"] = page["next_page_token"]

    if pending:
        files.append(_put_part(pending, len(files) + 1, base))

    return {
        "row_count": sum(f["rows"] for f in files),
        "max_changed_at": max_changed_at,  # None = no rows -> watermark must not move
        "files": files,
    }


# ---------------------------------------------------------------- entry point
def _to_naive_utc(value: str) -> datetime:
    # Source timestamps are naive UTC -> compare like with like
    ts = datetime.fromisoformat(value)
    return ts.astimezone(timezone.utc).replace(tzinfo=None) if ts.tzinfo else ts


def handler(event, context):
    run_id = event["run_id"]
    if not RUN_ID_RE.match(run_id):
        raise ValueError(f"run_id {run_id!r} is not path-safe ({RUN_ID_RE.pattern})")
    business_date = date.fromisoformat(event["business_date"]).isoformat()
    window_end = _to_naive_utc(event["window_end"])
    attempt = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")

    watermarks = read_watermarks()
    missing = set(ENTITIES) - set(watermarks)
    if missing:  # never silently default to "pull everything"
        raise RuntimeError(f"no watermark for {sorted(missing)}: run the seed in snowflake/04_control.sql")

    manifest = {
        "run_id": run_id,
        "business_date": business_date,
        "source_name": SOURCE_NAME,
        "attempt": attempt,
        "buffer_hours": BUFFER.total_seconds() / 3600,
        "entities": {},
    }
    for entity in ENTITIES:
        watermark = watermarks[entity]
        since = watermark - BUFFER
        base = f"{PREFIX}/{entity}/dt={business_date}/run={run_id}/attempt={attempt}"
        result = extract_entity(entity, since, window_end, base)

        manifest["entities"][entity] = {
            "watermark": watermark.isoformat(),
            "window_start": since.isoformat(),
            "window_end": window_end.isoformat(),
            **result,
        }
        print(json.dumps({"entity": entity, "window_start": since.isoformat(),
                          "window_end": window_end.isoformat(), "rows": result["row_count"],
                          "files": len(result["files"])}))

    # LAST: once this object exists, every data file above is already in S3
    manifest["generated_at"] = datetime.now(timezone.utc).isoformat()
    manifest_key = f"{PREFIX}/_manifests/dt={business_date}/run={run_id}/_manifest.json"
    s3.put_object(
        Bucket=os.environ["DATA_LAKE_BUCKET"],
        Key=manifest_key,
        Body=json.dumps(manifest, indent=2).encode(),
        ContentType="application/json",
    )
    return {"manifest_key": manifest_key, "manifest": manifest}
