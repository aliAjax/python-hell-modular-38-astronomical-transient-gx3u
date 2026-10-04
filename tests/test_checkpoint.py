import json
import tempfile
import unittest
from pathlib import Path

from src.domain import Actor, ConflictError, InvalidTransition, ValidationError
from src.repository import SQLiteRepository, utcnow
from src.service import DomainService
from src.rules import RuleEngine


class FailingRepository:
    """Test double that fails the first ``frame`` writes, then delegates."""

    def __init__(self, real, fail_on_frame_create=1):
        self._real = real
        self._fail_on_frame_create = fail_on_frame_create
        self._frame_creates = 0

    def create_entity(self, entity_id, kind, *args, **kwargs):
        if kind == "frame":
            self._frame_creates += 1
            if self._frame_creates <= self._fail_on_frame_create:
                raise RuntimeError("simulated write failure")
        return self._real.create_entity(entity_id, kind, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._real, name)


class CheckpointTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.service = DomainService(
            SQLiteRepository(Path(self.tmp.name) / "checkpoint.db"),
            RuleEngine(),
        )

    def tearDown(self):
        self.tmp.cleanup()

    def _setup_scheduled_observation(self, start="2026-09-28T10:00:00Z", end="2026-09-28T11:00:00Z"):
        analyst = Actor("analyst-1", "analyst")
        coordinator = Actor("coordinator-1", "coordinator")
        source = self.service.create(
            analyst, "source", {"name": "Survey", "survey_name": "S"}
        )
        candidate = self.service.create(
            analyst,
            "candidate",
            {
                "source_id": source["id"],
                "event_id": "AT-1",
                "ra": 10,
                "dec": 20,
                "magnitude": 18,
                "transient_type": "unknown",
                "observed_at": "2026-09-27T00:00:00Z",
            },
        )
        telescope = self.service.create(
            coordinator,
            "telescope",
            {"name": "Tel", "aperture_m": 2.0, "site_name": "S"},
        )
        observation = self.service.create(
            coordinator,
            "observation",
            {
                "candidate_id": candidate["id"],
                "telescope_id": telescope["id"],
                "team_id": "team-1",
                "start_at": start,
                "end_at": end,
                "mode": "imaging",
            },
        )
        observation = self.service.transition(
            coordinator, observation["id"], "schedule", {"operator_id": "op-1"}
        )
        return analyst, coordinator, candidate, telescope, observation

    def test_report_fault_splits_window_and_stores_frames(self):
        _, _, _, _, observation = self._setup_scheduled_observation()
        operator = Actor("operator-1", "operator")
        updated = self.service.transition(
            operator,
            observation["id"],
            "report_fault",
            {
                "frames_collected": 3,
                "lock_lost_at": "2026-09-28T10:30:00Z",
                "fault_reason": "guide star lost",
            },
            expected_version=observation["version"],
        )
        self.assertEqual(updated["status"], "interrupted")
        cp = updated["data"]["checkpoint"]
        self.assertEqual(cp["status"], "interrupted")
        self.assertEqual(cp["frames_collected"], 3)
        self.assertEqual(cp["lock_lost_at"], "2026-09-28T10:30:00Z")
        self.assertEqual(cp["fault_reason"], "guide star lost")
        self.assertEqual(cp["reported_by"], "operator-1")
        self.assertEqual(cp["original_start_at"], "2026-09-28T10:00:00Z")
        self.assertEqual(cp["original_end_at"], "2026-09-28T11:00:00Z")
        self.assertEqual(cp["collected_start_at"], "2026-09-28T10:00:00Z")
        self.assertEqual(cp["collected_end_at"], "2026-09-28T10:30:00Z")
        # Collected frames are stored in the database.
        frames = self.service.list("frame")
        self.assertEqual(len(frames), 3)
        for index, frame in enumerate(frames, start=1):
            self.assertEqual(frame["data"]["observation_id"], observation["id"])
            self.assertEqual(frame["data"]["frame_index"], index)
        # The remaining window is back to pending scheduling.
        remaining = self.service.list("observation", status="requested")
        self.assertEqual(len(remaining), 1)
        rem = remaining[0]
        self.assertEqual(rem["data"]["resumed_from"], observation["id"])
        self.assertEqual(rem["data"]["start_at"], "2026-09-28T10:30:00Z")
        self.assertEqual(rem["data"]["end_at"], "2026-09-28T11:00:00Z")
        self.assertEqual(rem["data"]["candidate_id"], observation["data"]["candidate_id"])
        self.assertEqual(rem["data"]["telescope_id"], observation["data"]["telescope_id"])
        self.assertEqual(rem["data"]["team_id"], observation["data"]["team_id"])
        self.assertEqual(rem["data"]["mode"], observation["data"]["mode"])
        # Audit trail records the fault, frame storage and remaining creation.
        actions = [entry["action"] for entry in self.service.audit_log(observation["id"])]
        self.assertIn("report_fault", actions)
        self.assertIn("store_frames", actions)
        self.assertIn("create_remaining", actions)

    def test_report_fault_validates_input(self):
        _, _, candidate, telescope, observation = self._setup_scheduled_observation()
        operator = Actor("operator-1", "operator")
        base = {
            "frames_collected": 3,
            "lock_lost_at": "2026-09-28T10:30:00Z",
            "fault_reason": "lost",
        }
        # lock_lost_at outside the observation window
        with self.assertRaises(ValidationError):
            self.service.transition(
                operator,
                observation["id"],
                "report_fault",
                {**base, "lock_lost_at": "2026-09-28T11:30:00Z"},
                expected_version=observation["version"],
            )
        # negative frame count
        with self.assertRaises(ValidationError):
            self.service.transition(
                operator,
                observation["id"],
                "report_fault",
                {**base, "frames_collected": -1},
                expected_version=observation["version"],
            )
        # missing fault reason
        with self.assertRaises(ValidationError):
            self.service.transition(
                operator,
                observation["id"],
                "report_fault",
                {"frames_collected": 3, "lock_lost_at": "2026-09-28T10:30:00Z"},
                expected_version=observation["version"],
            )
        # report_fault is not allowed on a requested observation
        coordinator = Actor("coordinator-1", "coordinator")
        requested = self.service.create(
            coordinator,
            "observation",
            {
                "candidate_id": candidate["id"],
                "telescope_id": telescope["id"],
                "team_id": "team-1",
                "start_at": "2026-09-28T12:00:00Z",
                "end_at": "2026-09-28T13:00:00Z",
                "mode": "imaging",
            },
        )
        with self.assertRaises(InvalidTransition):
            self.service.transition(
                operator,
                requested["id"],
                "report_fault",
                base,
            )

    def test_concurrent_fault_reports_one_wins(self):
        _, _, _, _, observation = self._setup_scheduled_observation()
        operator_a = Actor("operator-a", "operator")
        operator_b = Actor("operator-b", "operator")
        fault_data = {
            "frames_collected": 3,
            "lock_lost_at": "2026-09-28T10:30:00Z",
            "fault_reason": "guide star lost",
        }
        version = observation["version"]
        # Station A reports first and wins.
        updated_a = self.service.transition(
            operator_a,
            observation["id"],
            "report_fault",
            fault_data,
            expected_version=version,
        )
        self.assertEqual(updated_a["status"], "interrupted")
        # Station B reports with the stale version and sees the version change.
        with self.assertRaises(ConflictError) as ctx:
            self.service.transition(
                operator_b,
                observation["id"],
                "report_fault",
                fault_data,
                expected_version=version,
            )
        self.assertIn("version conflict", str(ctx.exception))
        # Only one checkpoint and no duplicate frames or remaining observations.
        self.assertEqual(len(self.service.list("frame")), 3)
        remaining = self.service.list("observation", status="requested")
        self.assertEqual(len(remaining), 1)
        self.assertEqual(remaining[0]["data"]["resumed_from"], observation["id"])

    def test_retry_after_write_failure_no_duplicate_frames(self):
        _, _, _, _, observation = self._setup_scheduled_observation()
        operator = Actor("operator-1", "operator")
        real_repo = self.service.repository
        self.service.repository = FailingRepository(real_repo, fail_on_frame_create=1)
        fault_data = {
            "frames_collected": 5,
            "lock_lost_at": "2026-09-28T10:30:00Z",
            "fault_reason": "guide star lost",
        }
        # First attempt: the observation update commits but the first frame write fails.
        with self.assertRaises(RuntimeError):
            self.service.transition(
                operator,
                observation["id"],
                "report_fault",
                fault_data,
                expected_version=observation["version"],
            )
        after_fail = real_repo.get_entity(observation["id"])
        self.assertEqual(after_fail["status"], "interrupted")
        self.assertEqual(len(real_repo.list_entities(kind="frame")), 0)
        # Retry on the same observation: frames already stored are not duplicated.
        self.service.repository = real_repo
        retried = self.service.transition(
            operator,
            after_fail["id"],
            "report_fault",
            fault_data,
            expected_version=after_fail["version"],
        )
        self.assertEqual(retried["status"], "interrupted")
        frames = real_repo.list_entities(kind="frame")
        self.assertEqual(len(frames), 5)
        remaining = real_repo.list_entities(kind="observation", status="requested")
        self.assertEqual(len(remaining), 1)

    def test_store_frames_is_idempotent(self):
        _, _, _, _, observation = self._setup_scheduled_observation()
        operator = Actor("operator-1", "operator")
        updated = self.service.transition(
            operator,
            observation["id"],
            "report_fault",
            {
                "frames_collected": 3,
                "lock_lost_at": "2026-09-28T10:30:00Z",
                "fault_reason": "lost",
            },
            expected_version=observation["version"],
        )
        # Calling the frame storage again with the same count does not duplicate.
        self.service._store_frames(updated["id"], 3, "2026-09-28T10:30:00Z")
        self.assertEqual(len(self.service.list("frame")), 3)

    def test_reschedule_remaining_segment_conflict_check(self):
        _, _, _, telescope, observation = self._setup_scheduled_observation()
        operator = Actor("operator-1", "operator")
        coordinator = Actor("coordinator-1", "coordinator")
        updated = self.service.transition(
            operator,
            observation["id"],
            "report_fault",
            {
                "frames_collected": 2,
                "lock_lost_at": "2026-09-28T10:30:00Z",
                "fault_reason": "lost",
            },
            expected_version=observation["version"],
        )
        remaining = self.service.list("observation", status="requested")[0]
        # The coordinator reschedules the remaining segment when the window is free.
        scheduled = self.service.transition(
            coordinator,
            remaining["id"],
            "schedule",
            {"operator_id": "op-2"},
            expected_version=remaining["version"],
        )
        self.assertEqual(scheduled["status"], "scheduled")
        # A new application overlapping the scheduled remaining window is blocked.
        blocked = self.service.create(
            coordinator,
            "observation",
            {
                "candidate_id": observation["data"]["candidate_id"],
                "telescope_id": telescope["id"],
                "team_id": "team-1",
                "start_at": "2026-09-28T10:30:00Z",
                "end_at": "2026-09-28T11:00:00Z",
                "mode": "imaging",
            },
        )
        with self.assertRaises(ConflictError):
            self.service.transition(
                coordinator,
                blocked["id"],
                "schedule",
                {"operator_id": "op-3"},
                expected_version=blocked["version"],
            )

    def test_backfill_checkpoint_not_started(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "backfill.db"
            repo = SQLiteRepository(path)
            now = utcnow()
            with repo._connect() as conn:
                conn.execute(
                    "INSERT INTO entities(id, kind, status, version, data, created_by, created_at, updated_at) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        "obs-hist",
                        "observation",
                        "completed",
                        1,
                        json.dumps(
                            {
                                "candidate_id": "cand-1",
                                "telescope_id": "tel-1",
                                "team_id": "team-1",
                                "start_at": "2026-09-28T10:00:00Z",
                                "end_at": "2026-09-28T11:00:00Z",
                                "mode": "imaging",
                            }
                        ),
                        "user",
                        now,
                        now,
                    ),
                )
            # Re-create the repository to re-run the upgrade migration.
            repo2 = SQLiteRepository(path)
            obs = repo2.get_entity("obs-hist")
            self.assertEqual(obs["data"]["checkpoint"]["status"], "not_started")


if __name__ == "__main__":
    unittest.main()
