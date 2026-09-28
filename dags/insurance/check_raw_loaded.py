"""Exit 0 when Snowflake raw tables hold every extracted row for a business date.

Runs with the dbt venv's Python (it has the Snowflake connector), called by the
check_snow_data sensor.   Exit codes: 0 = loaded, 3 = not yet, anything else = error.
"""

import json
import sys
from datetime import date
from pathlib import Path

import snowflake.connector

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from insurance.config import SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER

NOT_YET = 3


def main(ds: str, expected: dict) -> int:
    date.fromisoformat(ds)  # validate before it goes near SQL

    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        authenticator="WORKLOAD_IDENTITY",
        workload_identity_provider="AWS",
        role="INS_LOADER",
        warehouse="INS_WH",
        database="INS_RAW",
        schema="POLICY_ADMIN",
    )
    try:
        cur = conn.cursor()
        loaded = {}
        for entity in expected:
            cur.execute(f"SELECT COUNT(*) FROM {entity.upper()} WHERE BUSINESS_DATE = %s", (ds,))
            loaded[entity] = cur.fetchone()[0]
    finally:
        conn.close()

    print(json.dumps({"business_date": ds, "expected": expected, "loaded": loaded}))
    return 0 if all(loaded[e] >= expected[e] for e in expected) else NOT_YET


if __name__ == "__main__":
    sys.exit(main(sys.argv[1], json.loads(sys.argv[2])))
