import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, InvalidTransition, PermissionDenied, ValidationError
from src.repository import SQLiteRepository
from src.rules import RuleEngine
from src.service import DomainService


class CheckpointTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmp.name) / "checkpoint.db"
        self.service = DomainService(SQLiteRepository(self.db_path), RuleEngine())
        self.analyst = Actor("analyst-1", "analyst")
        self.coordinator = Actor("coordinator-1", "coordinator")
        self.operator = Actor("operator-1", "operator")

    def tearDown(self):
        self.tmp.cleanup()

    def _scheduled_observation(self, start="2026-09-28T10:00:00Z", end="2026-09-28T11:00:00Z",
                               team_id="team-north", telescope_id=None):
        source = self.service.create(
            self.analyst, "source", {"name": "Survey", "survey_name": "S"}
        )
        candidate = self.service.create(
            self.analyst,
            "candidate",
            {
                "source_id": source["id"],
                "event_id": "AT-" + source["id"][:8],
                "ra": 10,
                "dec": 20,
                "magnitude": 18,
                "transient_type": "unknown",
                "observed_at": "2026-09-27T00:00:00Z",
            },
        )
        if telescope_id is None:
            telescope_id = self.service.create(
                self.coordinator,
                "telescope",
                {"name": "North 2m", "aperture_m": 2.0, "site_name": "NAO"},
            )["id"]
        observation = self.service.create(
            self.coordinator,
            "observation",
            {
                "candidate_id": candidate["id"],
                "telescope_id": telescope_id,
                "team_id": team_id,
                "start_at": start,
                "end_at": end,
                "mode": "imaging",
            },
        )
        return self.service.transition(
            self.coordinator, observation["id"], "schedule", {"operator_id": "op-1"}
        )

    def _report(self, observation_id, actor=None, expected_version=None, **overrides):
        data = {
            "frames_acquired": 12,
            "failed_at": "2026-09-28T10:30:00Z",
            "fault_reason": "tracking lock lost",
        }
        data.update(overrides)
        return self.service.transition(
            actor or self.operator,
            observation_id,
            "report_failure",
            data,
            expected_version=expected_version,
        )

    def test_failure_splits_window_and_resumes_remainder(self):
        observation = self._scheduled_observation()
        updated = self._report(observation["id"])

        self.assertEqual(updated["status"], "interrupted")
        self.assertEqual(updated["data"]["frames_acquired"], 12)
        self.assertEqual(updated["data"]["failed_at"], "2026-09-28T10:30:00Z")
        self.assertEqual(updated["data"]["fault_reason"], "tracking lock lost")
        self.assertEqual(updated["data"]["reported_by"], "operator-1")
        self.assertEqual(updated["created_by"], "coordinator-1")
        self.assertEqual(updated["data"]["checkpoint"]["state"], "failed")

        segment = self.service.get(observation["id"] + "-segment")
        self.assertEqual(segment["status"], "completed")
        self.assertEqual(segment["data"]["start_at"], "2026-09-28T10:00:00Z")
        self.assertEqual(segment["data"]["end_at"], "2026-09-28T10:30:00Z")
        self.assertEqual(segment["data"]["frames_acquired"], 12)
        self.assertEqual(segment["data"]["parent_observation_id"], observation["id"])

        remainder = self.service.get(observation["id"] + "-remainder")
        self.assertEqual(remainder["status"], "requested")
        self.assertEqual(remainder["data"]["start_at"], "2026-09-28T10:30:00Z")
        self.assertEqual(remainder["data"]["end_at"], "2026-09-28T11:00:00Z")
        self.assertEqual(remainder["data"]["candidate_id"], observation["data"]["candidate_id"])
        self.assertEqual(remainder["data"]["checkpoint"]["state"], "not_started")

        checkpoint = self.service.get_checkpoint(observation["id"])
        self.assertEqual(checkpoint["frames_acquired"], 12)
        self.assertEqual(checkpoint["fault_reason"], "tracking lock lost")
        self.assertEqual(checkpoint["reported_by"], "operator-1")
        self.assertEqual(checkpoint["original_observation_id"], observation["id"])
        self.assertEqual(checkpoint["segment_observation_id"], segment["id"])
        self.assertEqual(checkpoint["remainder_observation_id"], remainder["id"])

        actions = [row["action"] for row in self.service.audit_log(observation["id"])]
        self.assertIn("report_failure", actions)

        # The remainder goes back through the queue and can be scheduled again.
        remainder = self.service.transition(
            self.coordinator, remainder["id"], "schedule", {"operator_id": "op-2"}
        )
        self.assertEqual(remainder["status"], "scheduled")
        remainder = self.service.transition(
            self.operator, remainder["id"], "complete", {"quality": "recovered"}
        )
        self.assertEqual(remainder["status"], "completed")

    def test_reschedule_requires_free_telescope_and_team(self):
        first = self._scheduled_observation()
        telescope_id = first["data"]["telescope_id"]
        self._report(first["id"])
        remainder_id = first["id"] + "-remainder"

        # Telescope busy in the remainder window: rescheduling is rejected.
        telescope_blocker = self._scheduled_observation(
            start="2026-09-28T10:45:00Z",
            end="2026-09-28T11:30:00Z",
            team_id="team-east",
            telescope_id=telescope_id,
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, remainder_id, "schedule", {"operator_id": "op-2"}
            )
        self.service.transition(
            self.analyst, telescope_blocker["id"], "withdraw", {"reason": "freed"}
        )

        # Team busy in the remainder window: still rejected.
        team_blocker = self._scheduled_observation(
            start="2026-09-28T10:45:00Z",
            end="2026-09-28T11:30:00Z",
            team_id="team-north",
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, remainder_id, "schedule", {"operator_id": "op-2"}
            )
        self.service.transition(
            self.analyst, team_blocker["id"], "withdraw", {"reason": "freed"}
        )

        # Both free: the remainder is scheduled and fills the fault window.
        remainder = self.service.transition(
            self.coordinator, remainder_id, "schedule", {"operator_id": "op-2"}
        )
        self.assertEqual(remainder["status"], "scheduled")

        # The filled window blocks later overlapping applications.
        later = self.service.create(
            self.coordinator,
            "observation",
            {
                "candidate_id": first["data"]["candidate_id"],
                "telescope_id": telescope_id,
                "team_id": "team-east",
                "start_at": "2026-09-28T10:30:00Z",
                "end_at": "2026-09-28T11:00:00Z",
                "mode": "imaging",
            },
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                self.coordinator, later["id"], "schedule", {"operator_id": "op-3"}
            )

    def test_concurrent_reports_accept_only_one(self):
        observation = self._scheduled_observation()
        version = observation["version"]
        station_a = Actor("station-a", "operator")
        station_b = Actor("station-b", "operator")

        accepted = self._report(observation["id"], actor=station_a, expected_version=version)
        self.assertEqual(accepted["status"], "interrupted")

        # The losing station sees the version change.
        with self.assertRaises(ConflictError):
            self._report(observation["id"], actor=station_b, expected_version=version)
        # Without a pinned version the state machine rejects the repeat.
        with self.assertRaises(InvalidTransition):
            self._report(observation["id"], actor=station_b)

        segments = [
            row for row in self.service.list("observation") if row["id"].endswith("-segment")
        ]
        self.assertEqual(len(segments), 1)
        self.assertEqual(
            self.service.get_checkpoint(observation["id"])["reported_by"], "station-a"
        )

    def test_retry_after_write_failure_does_not_duplicate_frames(self):
        observation = self._scheduled_observation()
        # Simulate a failed first attempt: the remainder id is already taken,
        # so the atomic write rolls back before anything is stored.
        self.service.repository.create_entity(
            observation["id"] + "-remainder", "observation", "withdrawn", {}, "other"
        )
        with self.assertRaises(ConflictError):
            self._report(observation["id"])
        still_scheduled = self.service.get(observation["id"])
        self.assertEqual(still_scheduled["status"], "scheduled")
        self.assertIsNone(self.service.get_checkpoint(observation["id"]))

        # Clean up the blocker and retry against the same observation.
        connection = sqlite3.connect(self.service.repository.path)
        connection.execute(
            "DELETE FROM entities WHERE id = ?", (observation["id"] + "-remainder",)
        )
        connection.commit()
        connection.close()
        updated = self._report(observation["id"])
        self.assertEqual(updated["status"], "interrupted")

        # A retry of an already persisted report is rejected; frames stay single.
        with self.assertRaises(ConflictError):
            self._report(observation["id"], expected_version=updated["version"] - 1)
        segments = [
            row
            for row in self.service.list("observation", status="completed")
            if row["data"].get("parent_observation_id") == observation["id"]
        ]
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]["data"]["frames_acquired"], 12)

    def test_report_failure_validation_and_roles(self):
        observation = self._scheduled_observation()
        with self.assertRaises(PermissionDenied):
            self._report(observation["id"], actor=self.coordinator)
        with self.assertRaises(ValidationError):
            self._report(observation["id"], failed_at="2026-09-28T09:00:00Z")
        with self.assertRaises(ValidationError):
            self._report(observation["id"], failed_at="2026-09-28T11:00:00Z")
        with self.assertRaises(ValidationError):
            self._report(observation["id"], frames_acquired=-1)
        with self.assertRaises(ValidationError):
            self._report(observation["id"], fault_reason="  ")
        with self.assertRaises(ValidationError):
            self.service.transition(
                self.operator, observation["id"], "report_failure", {"frames_acquired": 3}
            )

    def test_migration_backfills_missing_checkpoints(self):
        observation = self._scheduled_observation()
        connection = sqlite3.connect(self.db_path)
        rows = connection.execute(
            "SELECT id, data FROM entities WHERE kind = 'observation'"
        ).fetchall()
        for row_id, raw in rows:
            data = json.loads(raw)
            data.pop("checkpoint", None)
            connection.execute(
                "UPDATE entities SET data = ? WHERE id = ?", (json.dumps(data), row_id)
            )
        legacy_id = "legacy-interrupted"
        connection.execute(
            "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
            "VALUES (?, 'observation', 'interrupted', 3, ?, 'op-legacy', '2026-09-01', '2026-09-01')",
            (
                legacy_id,
                json.dumps(
                    {
                        "candidate_id": "c-legacy",
                        "telescope_id": "t-legacy",
                        "team_id": "team-legacy",
                        "start_at": "2026-09-01T10:00:00Z",
                        "end_at": "2026-09-01T11:00:00Z",
                        "mode": "imaging",
                    }
                ),
            ),
        )
        connection.commit()
        connection.close()

        # Reopening the repository upgrades the historical rows.
        upgraded = DomainService(SQLiteRepository(self.db_path), RuleEngine())
        for entity in upgraded.list("observation"):
            self.assertEqual(entity["data"]["checkpoint"], {"state": "not_started"})
        legacy = upgraded.get_checkpoint(legacy_id)
        self.assertIsNotNone(legacy)
        self.assertEqual(legacy["original_observation_id"], legacy_id)
        self.assertEqual(legacy["frames_acquired"], 0)
        self.assertEqual(upgraded.get(legacy_id)["status"], "interrupted")


if __name__ == "__main__":
    unittest.main()
