"""SQLite WAL is the durable scan/checkpoint journal; credentials are encrypted."""

import json
import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

from cryptography.fernet import Fernet


class ScanConflict(ValueError):
    pass


class BE2PipelineController:
    def __init__(self, state_dir, encryption_key):
        self.root = Path(state_dir)
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.cipher = Fernet(encryption_key)
        self.path = self.root / "state.sqlite3"
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript("""
                CREATE TABLE IF NOT EXISTS scans (
                    id TEXT PRIMARY KEY, tenant TEXT NOT NULL, state TEXT NOT NULL,
                    token BLOB NOT NULL, created REAL NOT NULL, removed INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS watermarks (
                    tenant TEXT, resource TEXT, value TEXT,
                    PRIMARY KEY (tenant, resource)
                );
                CREATE TABLE IF NOT EXISTS nonces (
                    nonce TEXT PRIMARY KEY, expires REAL NOT NULL
                );
            """)
        self.path.chmod(0o600)

    @contextmanager
    def connect(self):
        db = sqlite3.connect(self.path, timeout=30)
        db.execute("PRAGMA synchronous=FULL")
        try:
            with db:
                yield db
        finally:
            db.close()

    def claim_nonce(self, nonce, expires):
        with self.connect() as db:
            db.execute("DELETE FROM nonces WHERE expires < ?", (time.time(),))
            try:
                db.execute("INSERT INTO nonces VALUES (?, ?)", (nonce, expires))
            except sqlite3.IntegrityError:
                return False
        return True

    def create(self, scan_id, tenant, token, resources, limit):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            existing = db.execute(
                "SELECT tenant, state, removed FROM scans WHERE id=?", (scan_id,)
            ).fetchone()
            if existing:
                if existing[0] != tenant:
                    raise ScanConflict("scanId already belongs to another tenant")
                if existing[2]:
                    raise ScanConflict("scanId was removed; use a new scanId")
                return json.loads(existing[1]), True
            # Prevent overlapping snapshots advancing the same tenant's watermarks.
            for (raw,) in db.execute(
                "SELECT state FROM scans WHERE tenant=? AND removed=0", (tenant,)
            ):
                state = json.loads(raw)
                if state["status"] not in ("completed", "cancelled", "failed"):
                    if set(resources) & state["resources"].keys():
                        raise ScanConflict(
                            "An active scan already owns these tenant resources"
                        )
            checkpoints = {}
            for resource in resources:
                row = db.execute(
                    "SELECT value FROM watermarks WHERE tenant=? AND resource=?",
                    (tenant, resource),
                ).fetchone()
                checkpoints[resource] = dict(
                    cursor=None,
                    page=0,
                    records=0,
                    done=False,
                    watermark=row[0] if row else None,
                    updated_at=row[0] if row else None,
                    pending=None,
                )
            state = dict(
                scanId=scan_id,
                organizationId=tenant,
                status="pending",
                limit=limit,
                resources=checkpoints,
                error=None,
                createdAt=time.time(),
            )
            db.execute(
                "INSERT INTO scans (id, tenant, state, token, created) VALUES (?, ?, ?, ?, ?)",
                (
                    scan_id,
                    tenant,
                    json.dumps(state),
                    self.cipher.encrypt(token.encode()),
                    time.time(),
                ),
            )
            return state, False

    def get(self, scan_id, tenant=None):
        with self.connect() as db:
            row = db.execute(
                "SELECT tenant, state, removed FROM scans WHERE id=?", (scan_id,)
            ).fetchone()
        if not row or row[2] or (tenant is not None and row[0] != tenant):
            raise KeyError(scan_id)
        return json.loads(row[1])

    def token(self, scan_id):
        with self.connect() as db:
            row = db.execute(
                "SELECT token FROM scans WHERE id=?", (scan_id,)
            ).fetchone()
        return self.cipher.decrypt(row[0]).decode()

    def mutate(self, scan_id, change):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT state FROM scans WHERE id=?", (scan_id,)
            ).fetchone()
            if not row:
                raise KeyError(scan_id)
            state = json.loads(row[0])
            change(state)
            if state["status"] == "pending":
                for (raw,) in db.execute(
                    "SELECT state FROM scans WHERE tenant=? AND id<>? AND removed=0",
                    (state["organizationId"], scan_id),
                ):
                    other = json.loads(raw)
                    if (
                        other["status"] not in ("completed", "cancelled", "failed")
                        and state["resources"].keys() & other["resources"].keys()
                    ):
                        raise ScanConflict(
                            "An active scan already owns these tenant resources"
                        )
            db.execute(
                "UPDATE scans SET state=? WHERE id=?", (json.dumps(state), scan_id)
            )
        return state

    def transition(self, scan_id, allowed, target):
        def change(state):
            if state["status"] not in allowed:
                raise ScanConflict(f"Cannot {target} a {state['status']} scan")
            state["status"] = target
            state["error"] = None

        return self.mutate(scan_id, change)

    def checkpoint(self, scan_id, resource, pending):
        def change(state):
            cp = state["resources"][resource]
            cp.update(
                cursor=pending["next_cursor"],
                page=cp["page"] + 1,
                records=cp["records"] + len(pending["records"]),
                done=pending["next_cursor"] is None,
                updated_at=pending["updated_at"],
                pending=None,
            )

        return self.mutate(scan_id, change)

    def finish(self, scan_id):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            state = json.loads(
                db.execute("SELECT state FROM scans WHERE id=?", (scan_id,)).fetchone()[
                    0
                ]
            )
            status = state["status"]
            if status == "pausing":
                state["status"] = "paused"
            elif status == "cancelling":
                state["status"] = "cancelled"
            elif status == "running" and all(
                cp["done"] for cp in state["resources"].values()
            ):
                state["status"] = "completed"
                for resource, cp in state["resources"].items():
                    if cp["updated_at"]:
                        # Do not skip an object updated after its page was read.
                        started = datetime.fromtimestamp(
                            state["createdAt"], timezone.utc
                        ).isoformat(timespec="milliseconds")
                        watermark = min(cp["updated_at"], started)
                        db.execute(
                            """INSERT INTO watermarks VALUES (?, ?, ?)
                            ON CONFLICT(tenant, resource) DO UPDATE SET value=max(value, excluded.value)""",
                            (state["organizationId"], resource, watermark),
                        )
            db.execute(
                "UPDATE scans SET state=? WHERE id=?", (json.dumps(state), scan_id)
            )
        return state

    def list(self, tenant=None):
        with self.connect() as db:
            rows = (
                db.execute(
                    "SELECT state FROM scans WHERE tenant=? AND removed=0 ORDER BY created DESC",
                    (tenant,),
                )
                if tenant
                else db.execute(
                    "SELECT state FROM scans WHERE removed=0 ORDER BY created DESC"
                )
            )
            return [json.loads(row[0]) for row in rows]

    def remove(self, scan_id, tenant):
        with self.connect() as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute(
                "SELECT state FROM scans WHERE id=? AND tenant=? AND removed=0",
                (scan_id, tenant),
            ).fetchone()
            if not row:
                raise KeyError(scan_id)
            if json.loads(row[0])["status"] not in ("completed", "cancelled", "failed"):
                raise ScanConflict("Cancel the active scan before removal")
            tombstone = json.dumps(
                {"scanId": scan_id, "organizationId": tenant, "status": "removed"}
            )
            db.execute(
                "UPDATE scans SET removed=1, token=?, state=? WHERE id=?",
                (b"", tombstone, scan_id),
            )
