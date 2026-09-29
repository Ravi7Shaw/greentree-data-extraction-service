"""Coordinator HMAC v1: method, exact request target, tenant and body are signed."""

import hashlib
import hmac
import re
import time
from fastapi import Header, HTTPException, Request


def signature(secret, method, target, tenant, timestamp, nonce, body):
    message = "\n".join(
        (
            timestamp,
            nonce,
            method.upper(),
            target,
            tenant,
            hashlib.sha256(body).hexdigest(),
        )
    )
    return hmac.new(secret.encode(), message.encode(), hashlib.sha256).hexdigest()


async def coordinator_tenant(
    request: Request,
    tenant: str = Header(default="", alias="X-Organization-Id"),
    stamp: str = Header(default="", alias="X-Coordinator-Timestamp"),
    nonce: str = Header(default="", alias="X-Coordinator-Nonce"),
    supplied: str = Header(default="", alias="X-Coordinator-Signature"),
):
    secret = request.app.state.hmac_secret
    try:
        valid_time = abs(time.time() - int(stamp)) <= 300
    except ValueError:
        valid_time = False
    if (
        not tenant
        or len(tenant) > 128
        or not valid_time
        or not re.fullmatch(r"[A-Za-z0-9_-]{16,128}", nonce)
    ):
        raise HTTPException(401, "Invalid Coordinator authentication")
    target = request.scope.get("raw_path", request.url.path.encode()).decode("ascii")
    if request.scope["query_string"]:
        target += "?" + request.scope["query_string"].decode("ascii")
    expected = signature(
        secret, request.method, target, tenant, stamp, nonce, await request.body()
    )
    if not hmac.compare_digest(expected, supplied):
        raise HTTPException(401, "Invalid Coordinator authentication")
    if not request.app.state.scans.controller.claim_nonce(nonce, int(stamp) + 301):
        raise HTTPException(401, "Coordinator nonce already used")
    return tenant
