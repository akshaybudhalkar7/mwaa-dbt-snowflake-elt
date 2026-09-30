"""Mock "policy admin" REST API - a Lambda behind a Function URL.

GET /?entity=claims&updated_since=2026-09-01T00:00:00&until=2026-09-05T00:00:00[&page_token=..]
-> 200 {"entity", "count", "data": [...], "next_page_token": "..." | null}

Behaves like a real change-data API: every version changed in (updated_since, until],
in a stable order, paginated. Data comes from the same deterministic generator as v1.
"""

import base64
import json
import os
import random
from datetime import datetime

from insurance.generator import FEEDS, changes_between

ID_FIELD = {"policyholders": "policyholder_id", "policies": "policy_id", "claims": "claim_id"}
DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 500

# Chaos switch: share of requests answered with 429 -> proves the extractor's retry logic
THROTTLE_RATE = float(os.environ.get("THROTTLE_RATE", "0"))


def _response(status: int, body: dict) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(body),
    }


# Opaque token: clients must not build or parse it -> we can change pagination later
def _encode_token(offset: int) -> str:
    return base64.urlsafe_b64encode(json.dumps({"offset": offset}).encode()).decode()


def _decode_token(token: str) -> int:
    return json.loads(base64.urlsafe_b64decode(token.encode()))["offset"]


def handler(event, context):
    params = event.get("queryStringParameters") or {}

    if random.random() < THROTTLE_RATE:
        return _response(429, {"error": "rate limit exceeded, retry later"})

    try:
        entity = params["entity"]
        if entity not in FEEDS:
            raise ValueError(f"unknown entity {entity!r}, expected one of {sorted(FEEDS)}")
        since = datetime.fromisoformat(params["updated_since"])
        until = datetime.fromisoformat(params["until"])
        if since >= until:
            raise ValueError("updated_since must be before until")
        page_size = max(1, min(int(params.get("page_size", DEFAULT_PAGE_SIZE)), MAX_PAGE_SIZE))
        offset = _decode_token(params["page_token"]) if params.get("page_token") else 0
    except (KeyError, ValueError) as exc:
        return _response(400, {"error": f"bad request: missing or invalid parameter {exc}"})

    _, cursor = FEEDS[entity]
    rows = sorted(
        changes_between(entity, since, until),
        key=lambda r: (r[cursor], r[ID_FIELD[entity]]),  # stable order -> pages never shift
    )
    page = rows[offset : offset + page_size]
    has_more = offset + page_size < len(rows)

    return _response(200, {
        "entity": entity,
        "count": len(page),
        "data": page,
        "next_page_token": _encode_token(offset + page_size) if has_more else None,
    })
