"""Pipeline control for insurance_elt_v2: reconcile a run, advance watermarks, record failures.

Runs with the dbt venv's Python (it has the Snowflake connector), called by DAG tasks:
    python control.py reconcile '<manifest json>'   exit 0 = all loaded, 3 = not yet, else error
    python control.py advance   '<manifest json>'   moves watermarks + audit rows, one transaction
    python control.py fail <run_id> <business_date> '<error>'

Only the manifest's files count (RUN_ID + its attempt=<ts>/ folder): an earlier failed attempt
of the same run may also have loaded rows, and those must not be counted twice.
"""

import json
import os
import sys
from datetime import date
from pathlib import Path

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from insurance.config import SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER

NOT_YET = 3
SOURCE_NAME = "policy_admin_api"
# entity -> the change timestamp inside RECORD (the same cursor the extractor pages on)
CURSOR_FIELD = {"policyholders": "updated_at", "policies": "issued_at", "claims": "updated_at"}


def _connect():
    if os.environ.get("SNOWFLAKE_AUTH", "wif") == "wif":
        auth = {"user": SNOWFLAKE_USER, "authenticator": "WORKLOAD_IDENTITY", "workload_identity_provider": "AWS"}
    else:  # local: key-pair dev user (read-only roles -> reconcile works, advance doesn't)
        auth = {
            "user": os.environ["SNOWFLAKE_DEV_USER"],
            "authenticator": "SNOWFLAKE_JWT",
            "private_key_file": os.environ["SNOWFLAKE_DEV_PRIVATE_KEY_PATH"],
        }
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        role=os.environ.get("SNOWFLAKE_ROLE", "INS_LOADER"),  # owns INS_RAW.API + INS_RAW.CONTROL
        warehouse="INS_WH",
        **auth,
    )


def _loaded(cur, entity: str, manifest: dict) -> tuple[int, int, object]:
    """(rows, files, max change timestamp) that Snowpipe loaded from THIS manifest's files."""
    if entity not in CURSOR_FIELD:  # the entity becomes part of the SQL text -> allow-list it
        raise ValueError(f"unknown entity {entity!r}")
    cur.execute(
        f"""
        SELECT COUNT(*), COUNT(DISTINCT SOURCE_FILE), MAX(RECORD:{CURSOR_FIELD[entity]}::TIMESTAMP_NTZ)
        FROM INS_RAW.API.{entity.upper()}
        WHERE RUN_ID = %s AND SOURCE_FILE LIKE %s
        """,
        (manifest["run_id"], f"%/attempt={manifest['attempt']}/%"),
    )
    return cur.fetchone()


# ---------------------------------------------------------------- reconcile
def reconcile(manifest: dict) -> int:
    conn = _connect()
    try:
        cur = conn.cursor()
        report = {}
        for entity, info in manifest["entities"].items():
            rows, files, _ = _loaded(cur, entity, manifest)
            report[entity] = {
                "expected_rows": info["row_count"],
                "loaded_rows": rows,
                "expected_files": len(info["files"]),
                "loaded_files": files,
            }
    finally:
        conn.close()

    print(json.dumps({"run_id": manifest["run_id"], "attempt": manifest["attempt"], "entities": report}))
    # MORE rows than the manifest can't be "still loading" -> fail now instead of waiting
    too_many = [e for e, r in report.items() if r["loaded_rows"] > r["expected_rows"]]
    if too_many:
        raise RuntimeError(f"more rows loaded than the manifest lists for {too_many} - duplicate load?")
    done = all(r["loaded_rows"] == r["expected_rows"] for r in report.values())
    return 0 if done else NOT_YET


# ---------------------------------------------------------------- advance watermark
MERGE_WATERMARK = """
MERGE INTO INS_RAW.CONTROL.WATERMARKS t
USING (SELECT %s AS SOURCE_NAME, %s AS ENTITY, %s::TIMESTAMP_NTZ AS MAX_CHANGED_AT, %s AS RUN_ID) s
   ON t.SOURCE_NAME = s.SOURCE_NAME AND t.ENTITY = s.ENTITY
-- No rows loaded -> MAX is NULL -> keep the watermark (GREATEST(x, NULL) would be NULL!)
WHEN MATCHED AND s.MAX_CHANGED_AT IS NOT NULL THEN UPDATE SET
     HIGH_WATERMARK = GREATEST(t.HIGH_WATERMARK, s.MAX_CHANGED_AT),  -- only ever moves forward
     LAST_RUN_ID    = s.RUN_ID,
     UPDATED_AT     = CURRENT_TIMESTAMP()
"""

MERGE_RUN_SUCCEEDED = """
MERGE INTO INS_RAW.CONTROL.EXTRACT_RUNS t
USING (SELECT %s AS RUN_ID, %s AS SOURCE_NAME, %s AS ENTITY, %s::DATE AS BUSINESS_DATE,
              %s::TIMESTAMP_NTZ AS WINDOW_START, %s::TIMESTAMP_NTZ AS WINDOW_END,
              %s::NUMBER AS ROW_COUNT_MANIFEST, %s::NUMBER AS ROW_COUNT_LOADED,
              %s::TIMESTAMP_NTZ AS MAX_UPDATED_AT) s
   ON t.RUN_ID = s.RUN_ID AND t.ENTITY = s.ENTITY
WHEN MATCHED THEN UPDATE SET
     STATUS = 'SUCCEEDED', ERROR_MESSAGE = NULL,
     WINDOW_START = s.WINDOW_START, WINDOW_END = s.WINDOW_END,
     ROW_COUNT_MANIFEST = s.ROW_COUNT_MANIFEST, ROW_COUNT_LOADED = s.ROW_COUNT_LOADED,
     MAX_UPDATED_AT = s.MAX_UPDATED_AT, FINISHED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT
     (RUN_ID, SOURCE_NAME, ENTITY, BUSINESS_DATE, WINDOW_START, WINDOW_END,
      ROW_COUNT_MANIFEST, ROW_COUNT_LOADED, MAX_UPDATED_AT, STATUS)
VALUES (s.RUN_ID, s.SOURCE_NAME, s.ENTITY, s.BUSINESS_DATE, s.WINDOW_START, s.WINDOW_END,
        s.ROW_COUNT_MANIFEST, s.ROW_COUNT_LOADED, s.MAX_UPDATED_AT, 'SUCCEEDED')
"""


def advance(manifest: dict) -> int:
    conn = _connect()
    conn.autocommit(False)  # all entities move together, or none do
    try:
        cur = conn.cursor()
        for entity, info in manifest["entities"].items():
            rows, _, max_changed_at = _loaded(cur, entity, manifest)
            # Last line of defence: never move a watermark over data that isn't fully loaded
            if rows != info["row_count"]:
                raise RuntimeError(f"{entity}: manifest {info['row_count']} rows, loaded {rows} - not advancing")

            cur.execute(MERGE_WATERMARK, (SOURCE_NAME, entity, max_changed_at, manifest["run_id"]))
            cur.execute(
                MERGE_RUN_SUCCEEDED,
                (
                    manifest["run_id"], SOURCE_NAME, entity, manifest["business_date"],
                    info["window_start"], info["window_end"],
                    info["row_count"], rows, max_changed_at,
                ),
            )
            print(f"{entity}: {rows} rows, watermark -> max({max_changed_at}, current)")
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return 0


# ---------------------------------------------------------------- record a failure
MERGE_RUN_FAILED = """
MERGE INTO INS_RAW.CONTROL.EXTRACT_RUNS t
USING (SELECT %s AS RUN_ID, %s AS SOURCE_NAME, %s AS ENTITY, %s::DATE AS BUSINESS_DATE, %s AS ERROR_MESSAGE) s
   ON t.RUN_ID = s.RUN_ID AND t.ENTITY = s.ENTITY
-- A run that already SUCCEEDED stays succeeded (e.g. a later task of the same run failed)
WHEN MATCHED AND t.STATUS <> 'SUCCEEDED' THEN UPDATE SET
     STATUS = 'FAILED', ERROR_MESSAGE = s.ERROR_MESSAGE, FINISHED_AT = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (RUN_ID, SOURCE_NAME, ENTITY, BUSINESS_DATE, STATUS, ERROR_MESSAGE)
VALUES (s.RUN_ID, s.SOURCE_NAME, s.ENTITY, s.BUSINESS_DATE, 'FAILED', s.ERROR_MESSAGE)
"""


def fail(run_id: str, business_date: str, error: str) -> int:
    date.fromisoformat(business_date)
    conn = _connect()
    try:
        cur = conn.cursor()
        for entity in CURSOR_FIELD:
            cur.execute(MERGE_RUN_FAILED, (run_id, SOURCE_NAME, entity, business_date, error[:4000]))
    finally:
        conn.close()
    print(f"run {run_id} recorded as FAILED - watermarks unchanged")
    return 0


if __name__ == "__main__":
    command, *args = sys.argv[1:]
    if command == "reconcile":
        sys.exit(reconcile(json.loads(args[0])))
    elif command == "advance":
        sys.exit(advance(json.loads(args[0])))
    elif command == "fail":
        sys.exit(fail(*args))
    sys.exit(f"unknown command {command!r}")
