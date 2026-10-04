import json
import sqlite3
from datetime import datetime, timezone

from .domain import ConflictError, NotFoundError


def utcnow():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class SQLiteRepository:
    def __init__(self, path):
        self.path = str(path)
        self._initialize()

    def _connect(self):
        connection = sqlite3.connect(self.path, timeout=30)
        connection.row_factory = sqlite3.Row
        return connection

    def _initialize(self):
        with self._connect() as connection:
            connection.executescript("""
                CREATE TABLE IF NOT EXISTS entities (
                    id TEXT PRIMARY KEY,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL,
                    data TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_entities_kind_status
                    ON entities(kind, status);
                CREATE TABLE IF NOT EXISTS audit_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_id TEXT NOT NULL,
                    actor_id TEXT NOT NULL,
                    actor_role TEXT NOT NULL,
                    action TEXT NOT NULL,
                    from_status TEXT,
                    to_status TEXT NOT NULL,
                    detail TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_audit_entity
                    ON audit_log(entity_id, id);
                CREATE TABLE IF NOT EXISTS idempotency (
                    actor_id TEXT NOT NULL,
                    idem_key TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    PRIMARY KEY(actor_id, idem_key)
                );
                CREATE TABLE IF NOT EXISTS observation_checkpoints (
                    observation_id TEXT PRIMARY KEY,
                    frames_acquired INTEGER NOT NULL,
                    failed_at TEXT NOT NULL,
                    fault_reason TEXT NOT NULL,
                    reported_by TEXT NOT NULL,
                    original_observation_id TEXT NOT NULL,
                    segment_observation_id TEXT NOT NULL,
                    remainder_observation_id TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
            """)
            self._migrate_checkpoints(connection)

    @staticmethod
    def _migrate_checkpoints(connection):
        """Backfill checkpoints for pre-upgrade observations that never recorded one."""
        rows = connection.execute(
            "SELECT id, status, data, created_by FROM entities WHERE kind = 'observation'"
        ).fetchall()
        now = utcnow()
        for row in rows:
            data = json.loads(row["data"])
            if data.get("checkpoint"):
                continue
            data["checkpoint"] = {"state": "not_started"}
            connection.execute(
                "UPDATE entities SET data = ? WHERE id = ?",
                (json.dumps(data, ensure_ascii=False, sort_keys=True), row["id"]),
            )
            if row["status"] != "interrupted":
                continue
            # Legacy interrupted rows predate checkpoints; keep the original
            # window on both sides of the split because the lock-loss moment
            # was never recorded.
            connection.execute(
                "INSERT OR IGNORE INTO observation_checkpoints("
                "observation_id, frames_acquired, failed_at, fault_reason, reported_by,"
                " original_observation_id, segment_observation_id, remainder_observation_id, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    row["id"],
                    int(data.get("frames_acquired") or 0),
                    str(data.get("failed_at") or data.get("start_at") or now),
                    str(data.get("fault_reason") or "tracking lock lost"),
                    str(data.get("reported_by") or row["created_by"]),
                    row["id"],
                    row["id"],
                    row["id"],
                    now,
                ),
            )

    @staticmethod
    def _entity_from_row(row):
        return {
            "id": row["id"],
            "kind": row["kind"],
            "status": row["status"],
            "version": int(row["version"]),
            "data": json.loads(row["data"]),
            "created_by": row["created_by"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def create_entity(self, entity_id, kind, status, data, actor_id):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                (entity_id, kind, status, payload, actor_id, now, now),
            )
        return self.get_entity(entity_id)

    def get_entity(self, entity_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
        return self._entity_from_row(row) if row else None

    def list_entities(self, kind=None, status=None):
        clauses = []
        params = []
        if kind:
            clauses.append("kind = ?")
            params.append(kind)
        if status:
            clauses.append("status = ?")
            params.append(status)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        with self._connect() as connection:
            rows = connection.execute(
                "SELECT * FROM entities" + where + " ORDER BY created_at, id", params
            ).fetchall()
        return [self._entity_from_row(row) for row in rows]

    def find_entities(self, kind, field, value):
        return [
            entity
            for entity in self.list_entities(kind=kind)
            if (entity["id"] == value if field == "id" else entity["data"].get(field) == value)
        ]

    def update_entity(self, entity_id, expected_version, status, data):
        now = utcnow()
        payload = json.dumps(data, ensure_ascii=False, sort_keys=True)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (entity_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + entity_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            connection.execute(
                "UPDATE entities SET status = ?, version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (status, payload, now, entity_id, current_version),
            )
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(entity_id)

    def record_failure(
        self,
        observation_id,
        expected_version,
        observation_data,
        segment,
        remainder,
        checkpoint,
    ):
        """Persist one failure report atomically.

        Writes the completed segment, the re-queued remainder and the updated
        original observation plus its checkpoint in a single transaction, so a
        retried report can never duplicate frames that are already stored.
        """
        now = utcnow()
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = connection.execute(
                "SELECT version FROM entities WHERE id = ?", (observation_id,)
            ).fetchone()
            if not row:
                raise NotFoundError("entity not found: " + observation_id)
            current_version = int(row["version"])
            if expected_version is not None and current_version != int(expected_version):
                raise ConflictError(
                    "version conflict: expected %s, found %s"
                    % (expected_version, current_version)
                )
            for entity_id, kind, status, data, actor_id in (segment, remainder):
                if self._entity_exists(connection, entity_id):
                    raise ConflictError("entity already exists: " + entity_id)
                connection.execute(
                    "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                    "VALUES (?, ?, ?, 1, ?, ?, ?, ?)",
                    (
                        entity_id,
                        kind,
                        status,
                        json.dumps(data, ensure_ascii=False, sort_keys=True),
                        actor_id,
                        now,
                        now,
                    ),
                )
            connection.execute(
                "UPDATE entities SET status = 'interrupted', version = version + 1, data = ?, updated_at = ? "
                "WHERE id = ? AND version = ?",
                (
                    json.dumps(observation_data, ensure_ascii=False, sort_keys=True),
                    now,
                    observation_id,
                    current_version,
                ),
            )
            connection.execute(
                "INSERT INTO observation_checkpoints("
                "observation_id, frames_acquired, failed_at, fault_reason, reported_by,"
                " original_observation_id, segment_observation_id, remainder_observation_id, created_at"
                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    observation_id,
                    int(checkpoint["frames_acquired"]),
                    checkpoint["failed_at"],
                    checkpoint["fault_reason"],
                    checkpoint["reported_by"],
                    checkpoint["original_observation_id"],
                    segment[0],
                    remainder[0],
                    now,
                ),
            )
            connection.commit()
        except sqlite3.IntegrityError as exc:
            connection.rollback()
            if "observation_checkpoints" in str(exc):
                raise ConflictError(
                    "failure already recorded for observation: " + observation_id
                )
            raise
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
        return self.get_entity(observation_id)

    @staticmethod
    def _entity_exists(connection, entity_id):
        row = connection.execute(
            "SELECT 1 FROM entities WHERE id = ?", (entity_id,)
        ).fetchone()
        return row is not None

    def get_checkpoint(self, observation_id):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT * FROM observation_checkpoints WHERE observation_id = ?",
                (observation_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "observation_id": row["observation_id"],
            "frames_acquired": int(row["frames_acquired"]),
            "failed_at": row["failed_at"],
            "fault_reason": row["fault_reason"],
            "reported_by": row["reported_by"],
            "original_observation_id": row["original_observation_id"],
            "segment_observation_id": row["segment_observation_id"],
            "remainder_observation_id": row["remainder_observation_id"],
            "created_at": row["created_at"],
        }

    def append_audit(self, entity_id, actor_id, actor_role, action, from_status, to_status, detail):
        with self._connect() as connection:
            connection.execute(
                "INSERT INTO audit_log(entity_id, actor_id, actor_role, action, from_status, to_status, detail, created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    entity_id,
                    actor_id,
                    actor_role,
                    action,
                    from_status,
                    to_status,
                    json.dumps(detail, ensure_ascii=False, sort_keys=True),
                    utcnow(),
                ),
            )

    def list_audit(self, entity_id=None):
        with self._connect() as connection:
            if entity_id:
                rows = connection.execute(
                    "SELECT * FROM audit_log WHERE entity_id = ? ORDER BY id", (entity_id,)
                ).fetchall()
            else:
                rows = connection.execute("SELECT * FROM audit_log ORDER BY id").fetchall()
        return [
            {
                "id": row["id"],
                "entity_id": row["entity_id"],
                "actor_id": row["actor_id"],
                "actor_role": row["actor_role"],
                "action": row["action"],
                "from_status": row["from_status"],
                "to_status": row["to_status"],
                "detail": json.loads(row["detail"]),
                "created_at": row["created_at"],
            }
            for row in rows
        ]

    def get_idempotency(self, actor_id, idem_key):
        with self._connect() as connection:
            row = connection.execute(
                "SELECT entity_id FROM idempotency WHERE actor_id = ? AND idem_key = ?",
                (actor_id, idem_key),
            ).fetchone()
        return row["entity_id"] if row else None

    def save_idempotency(self, actor_id, idem_key, entity_id):
        with self._connect() as connection:
            connection.execute(
                "INSERT OR REPLACE INTO idempotency(actor_id, idem_key, entity_id, created_at) "
                "VALUES (?, ?, ?, ?)",
                (actor_id, idem_key, entity_id, utcnow()),
            )

    def ping(self):
        with self._connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return True
