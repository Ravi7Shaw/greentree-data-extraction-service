"""Sign a Coordinator request using the exact bytes sent over HTTP."""

import argparse
import json
import sys
import time
import uuid
from pathlib import Path

# Allow execution as `python scripts/coordinator_request.py` from the checkout.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import requests
from api.auth import signature
from config import Config


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("method", choices=["GET", "POST", "DELETE"])
    parser.add_argument("target", help="Exact /api path, including any query string")
    parser.add_argument("--tenant", required=True)
    parser.add_argument(
        "--body", type=Path, help="JSON file; bytes are signed unchanged"
    )
    parser.add_argument("--base-url", default="http://localhost:5200")
    args = parser.parse_args()
    if not args.target.startswith("/api/"):
        parser.error("target must start with /api/")
    secret = Config.COORDINATOR_HMAC_SECRET
    if len(secret) < 32:
        parser.error("Set COORDINATOR_HMAC_SECRET in the environment or .env")
    body = args.body.read_bytes() if args.body else b""
    stamp, nonce = str(int(time.time())), uuid.uuid4().hex
    headers = {
        "X-Organization-Id": args.tenant,
        "X-Coordinator-Timestamp": stamp,
        "X-Coordinator-Nonce": nonce,
        "Content-Type": "application/json",
        "X-Coordinator-Signature": signature(
            secret, args.method, args.target, args.tenant, stamp, nonce, body
        ),
    }
    response = requests.request(
        args.method,
        args.base_url.rstrip("/") + args.target,
        headers=headers,
        data=body,
        timeout=60,
    )
    print(json.dumps(response.json(), indent=2))
    response.raise_for_status()


if __name__ == "__main__":
    main()
