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
        next_status, patch = self.rules.validate_transition(
            actor, entity, action, dict(data or {}), self._lookup
        )
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
        if action == "report_fault":
            self._handle_report_fault(actor, updated)
        return updated

    def _handle_report_fault(self, actor, observation):
        """Split the observation window after a lock-loss fault.

        Collected frames are stored as ``frame`` entities and a new
        ``requested`` observation covering the remaining window is created so
        the coordinator can reschedule it. Both creations are idempotent: a
        retry of the same fault report reuses the existing frames and the
        existing remaining observation instead of duplicating them.
        """
        checkpoint = observation["data"].get("checkpoint") or {}
        frames_collected = int(checkpoint.get("frames_collected", 0))
        frame_ids = self._store_frames(
            observation["id"], frames_collected, checkpoint.get("lock_lost_at")
        )
        remaining_id = self._ensure_remaining_observation(observation)
        self.audit.record(
            observation["id"],
            actor,
            "store_frames",
            observation["status"],
            observation["status"],
            {"frame_ids": frame_ids, "frames_collected": frames_collected},
        )
        self.audit.record(
            observation["id"],
            actor,
            "create_remaining",
            observation["status"],
            observation["status"],
            {"remaining_observation_id": remaining_id},
        )

    def _store_frames(self, observation_id, count, lock_lost_at):
        frame_ids = []
        for index in range(1, count + 1):
            frame_id = "frame:%s:%d" % (observation_id, index)
            if self.repository.get_entity(frame_id):
                frame_ids.append(frame_id)
                continue
            data = {
                "observation_id": observation_id,
                "frame_index": index,
                "observed_at": lock_lost_at,
            }
            self.repository.create_entity(frame_id, "frame", "recorded", data, "system")
            frame_ids.append(frame_id)
        return frame_ids

    def _ensure_remaining_observation(self, original):
        observation_id = original["id"]
        existing = self._lookup("observation", "resumed_from", observation_id)
        if existing:
            return existing[0]["id"]
        checkpoint = original["data"].get("checkpoint") or {}
        data = {
            "candidate_id": original["data"].get("candidate_id"),
            "telescope_id": original["data"].get("telescope_id"),
            "team_id": original["data"].get("team_id"),
            "mode": original["data"].get("mode"),
            "start_at": checkpoint.get("lock_lost_at"),
            "end_at": original["data"].get("end_at"),
            "resumed_from": observation_id,
            "segment": "remaining",
            "checkpoint": {"status": "not_started"},
        }
        remaining_id = str(uuid4())
        self.repository.create_entity(
            remaining_id, "observation", "requested", data, "system"
        )
        return remaining_id

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
