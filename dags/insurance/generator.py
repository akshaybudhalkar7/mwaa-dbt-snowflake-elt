"""Synthetic "policy admin system" extract for one business date.

Deterministic: every random choice is seeded by the business date, so generating the
same date twice returns identical rows. That keeps retries and backfills idempotent.

Daily extract per business date (ds):
- policyholders: FULL snapshot of all customers (a few change city / risk tier each day)
  -> dbt snapshot builds SCD Type 2 history from it.
- policies:      policies ISSUED on ds (new business)
- claims:        claims REPORTED on ds, plus status UPDATES to claims reported earlier
  -> late-arriving updates, handled by a dbt incremental MERGE on claim_id.

Dates and timestamps are ISO strings; dbt staging casts them to proper types.
"""

import random
from datetime import date, timedelta

from insurance.config import EPOCH

NUM_POLICYHOLDERS = 100
FIRST_NAMES = ["Aarav", "Diya", "Rohan", "Isha", "Kabir", "Meera", "Arjun", "Sara", "Vivaan", "Anika"]
LAST_NAMES = ["Sharma", "Patel", "Iyer", "Khan", "Deshmukh", "Reddy", "Nair", "Gupta", "Joshi", "Rao"]
CITIES = [("Pune", "MH"), ("Mumbai", "MH"), ("Bengaluru", "KA"), ("Chennai", "TN"), ("Hyderabad", "TS"), ("Delhi", "DL")]
RISK_TIERS = ["LOW", "MEDIUM", "HIGH"]
PRODUCTS = {"AUTO": (400, 1500), "HOME": (600, 2500), "LIFE": (300, 3000)}
CLAIM_FLOW = {"OPEN": ["APPROVED", "REJECTED"], "APPROVED": ["PAID"]}


def _rng(ds: date, entity: str) -> random.Random:
    # Same (date, entity) -> same random sequence -> same data
    return random.Random(f"{ds.isoformat()}|{entity}")


def _days(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


# ---------------------------------------------------------------- policyholders
def _initial_policyholders() -> dict[int, dict]:
    rng = random.Random("policyholders-initial")
    holders = {}
    for pid in range(1, NUM_POLICYHOLDERS + 1):
        city, state = rng.choice(CITIES)
        holders[pid] = {
            "policyholder_id": pid,
            "first_name": rng.choice(FIRST_NAMES),
            "last_name": rng.choice(LAST_NAMES),
            "date_of_birth": date(rng.randint(1960, 2002), rng.randint(1, 12), rng.randint(1, 28)).isoformat(),
            "city": city,
            "state": state,
            "risk_tier": rng.choice(RISK_TIERS),
            "updated_at": f"{EPOCH}T00:00:00",
        }
    return holders


def policyholders_snapshot(ds: date) -> list[dict]:
    """Replay every day's changes from EPOCH to ds -> full snapshot as of ds."""
    holders = _initial_policyholders()
    for day in _days(date.fromisoformat(EPOCH) + timedelta(days=1), ds):
        rng = _rng(day, "policyholder-changes")
        for pid in rng.sample(sorted(holders), k=3):  # ~3 customers change per day
            h = holders[pid]
            if rng.random() < 0.5:
                h["city"], h["state"] = rng.choice([c for c in CITIES if c[0] != h["city"]])
            else:
                h["risk_tier"] = rng.choice([t for t in RISK_TIERS if t != h["risk_tier"]])
            h["updated_at"] = f"{day.isoformat()}T09:00:00"
    return [dict(h) for h in holders.values()]


# ---------------------------------------------------------------- policies
def _policy_count(day: date) -> int:
    return _rng(day, "policy-count").randint(4, 8)


def _policy_id(day: date, n: int) -> str:
    return f"POL-{day.strftime('%Y%m%d')}-{n:03d}"


def policies_issued(ds: date) -> list[dict]:
    rng = _rng(ds, "policies")
    rows = []
    for n in range(1, _policy_count(ds) + 1):
        product = rng.choice(sorted(PRODUCTS))
        low, high = PRODUCTS[product]
        rows.append(
            {
                "policy_id": _policy_id(ds, n),
                "policyholder_id": rng.randint(1, NUM_POLICYHOLDERS),
                "product_line": product,
                "annual_premium": round(rng.uniform(low, high), 2),
                "effective_date": ds.isoformat(),
                "expiration_date": (ds + timedelta(days=365)).isoformat(),
                "status": "ACTIVE",
                "issued_at": f"{ds.isoformat()}T10:{rng.randint(0, 59):02d}:00",
            }
        )
    return rows


# ---------------------------------------------------------------- claims
def _claim_count(day: date) -> int:
    return _rng(day, "claim-count").randint(2, 5)


def _new_claims(day: date) -> list[dict]:
    rng = _rng(day, "claims")
    epoch = date.fromisoformat(EPOCH)
    rows = []
    for n in range(1, _claim_count(day) + 1):
        # The claimed policy was issued 0-20 days earlier (never before the source system existed)
        issue_day = max(epoch, day - timedelta(days=rng.randint(0, 20)))
        policy_id = _policy_id(issue_day, rng.randint(1, _policy_count(issue_day)))
        rows.append(
            {
                "claim_id": f"CLM-{day.strftime('%Y%m%d')}-{n:03d}",
                "policy_id": policy_id,
                "loss_date": max(issue_day, day - timedelta(days=rng.randint(0, 5))).isoformat(),
                "reported_date": day.isoformat(),
                "claim_amount": round(rng.uniform(200, 20000), 2),
                "status": "OPEN",
                "updated_at": f"{day.isoformat()}T11:{rng.randint(0, 59):02d}:00",
            }
        )
    return rows


def claims_changed(ds: date) -> list[dict]:
    """Claims reported on ds + status updates (OPEN -> APPROVED/REJECTED -> PAID) to older claims."""
    rows = _new_claims(ds)
    rng = _rng(ds, "claim-updates")
    epoch = date.fromisoformat(EPOCH)
    for back in (3, 7):  # claims reported 3 and 7 days ago move one step forward
        old_day = ds - timedelta(days=back)
        if old_day < epoch:
            continue
        for claim in _new_claims(old_day):
            if rng.random() < 0.6:
                status = rng.choice(CLAIM_FLOW["OPEN"]) if back == 3 else "PAID"
                if back == 7 and rng.random() < 0.3:
                    continue  # some claims never get paid (rejected earlier)
                rows.append({**claim, "status": status, "updated_at": f"{ds.isoformat()}T15:00:00"})
    return rows


def generate_day(ds: str) -> dict[str, list[dict]]:
    """All entities for one business date (ds = 'YYYY-MM-DD')."""
    day = date.fromisoformat(ds)
    return {
        "policyholders": policyholders_snapshot(day),
        "policies": policies_issued(day),
        "claims": claims_changed(day),
    }
