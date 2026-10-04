from uuid import uuid4

from .audit import AuditTrail
from .domain import ConflictError, NotFoundError
from .rules import RuleEngine


class DomainService:
    def __init__(self, repository, rules=None):
        self.repository = repository
        self.rules = rules or RuleEngine()
        self.audit = AuditTrail(repository)

    def _lookup(self, kind, field, value):
        return self.repository.find_entities(self.rules.normalize_kind(kind), field, value)

    def health(self):
        return {"status": "ok" if self.repository.ping() else "error"}

    def create(self, actor, kind, data, idempotency_key=None):
        kind = self.rules.normalize_kind(kind)
        payload = dict(data or {})
        if idempotency_key:
            existing = self.repository.get_idempotency(actor.user_id, idempotency_key)
            if existing:
                entity = self.repository.get_entity(existing)
                if entity:
                    return entity
        validated = self.rules.validate_create(actor, kind, payload, self._lookup)
        if validated:
            payload.update(validated)
        entity_id = str(payload.pop("id", "") or uuid4())
        if self.repository.get_entity(entity_id):
            raise ConflictError("entity already exists: " + entity_id)
        status = self.rules.initial_status(kind)
        entity = self.repository.create_entity(entity_id, kind, status, payload, actor.user_id)
        self.audit.record(entity_id, actor, "create", None, status, {"kind": kind})
        if idempotency_key:
            self.repository.save_idempotency(actor.user_id, idempotency_key, entity_id)
        return entity

    def transition(self, actor, entity_id, action, data=None, expected_version=None):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        expected = int(expected_version) if expected_version is not None else entity["version"]
        if expected != entity["version"]:
            raise ConflictError(
                "version conflict: expected %s, found %s" % (expected, entity["version"])
            )
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
        if action == "report_failure" and entity["kind"] == "observation":
            return self._record_failure(actor, entity, patch, expected)
        merged = dict(entity["data"])
        merged.update(patch)
        updated = self.repository.update_entity(entity_id, expected, next_status, merged)
        self.audit.record(
            entity_id,
            actor,
            action,
            entity["status"],
            updated["status"],
            {"patch": patch},
        )
        return updated

    def _record_failure(self, actor, entity, patch, expected_version):
        """Cut the original window at the lock-loss moment and persist the split.

        The acquired part is stored as a completed segment observation, the
        remainder goes back to requested, and the original keeps the failure
        reason, the reporting operator and the requester. A repeated report
        for the same observation is rejected by the checkpoint, so frames
        that are already stored are never written twice.
        """
        data = entity["data"]
        failed_at = patch["failed_at"]
        frames_acquired = patch["frames_acquired"]
        segment_id = entity["id"] + "-segment"
        remainder_id = entity["id"] + "-remainder"
        base = {
            key: value
            for key, value in data.items()
            if key not in ("checkpoint", "frames_acquired", "failed_at", "fault_reason", "reported_by")
        }
        segment_data = dict(base)
        segment_data.update(
            {
                "start_at": data.get("start_at"),
                "end_at": failed_at,
                "parent_observation_id": entity["id"],
                "segment_role": "acquired",
                "frames_acquired": frames_acquired,
                "checkpoint": {"state": "completed"},
            }
        )
        remainder_data = dict(base)
        remainder_data.update(
            {
                "start_at": failed_at,
                "end_at": data.get("end_at"),
                "parent_observation_id": entity["id"],
                "segment_role": "remainder",
                "reschedule_of": entity["id"],
                "checkpoint": {"state": "not_started"},
            }
        )
        observation_data = dict(data)
        observation_data.update(patch)
        checkpoint = {
            "frames_acquired": frames_acquired,
            "failed_at": failed_at,
            "fault_reason": patch["fault_reason"],
            "reported_by": patch["reported_by"],
            "original_observation_id": entity["id"],
        }
        updated = self.repository.record_failure(
            entity["id"],
            expected_version,
            observation_data,
            (segment_id, "observation", "completed", segment_data, actor.user_id),
            (remainder_id, "observation", "requested", remainder_data, actor.user_id),
            checkpoint,
        )
        self.audit.record(
            entity["id"],
            actor,
            "report_failure",
            entity["status"],
            updated["status"],
            {
                "patch": patch,
                "segment_observation_id": segment_id,
                "remainder_observation_id": remainder_id,
            },
        )
        return updated

    def get_checkpoint(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return self.repository.get_checkpoint(entity_id)

    def get(self, entity_id):
        entity = self.repository.get_entity(entity_id)
        if not entity:
            raise NotFoundError("entity not found: " + entity_id)
        return entity

    def list(self, kind=None, status=None):
        if kind:
            kind = self.rules.normalize_kind(kind)
        return self.repository.list_entities(kind=kind, status=status)

    def audit_log(self, entity_id=None):
        return self.repository.list_audit(entity_id=entity_id)
