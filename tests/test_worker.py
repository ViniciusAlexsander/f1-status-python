import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from worker.worker import (
    build_worker_schedule,
    is_schedule_live_active,
    seconds_until_next_worker_run,
    should_listen_to_live_streams,
)


class WorkerScheduleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.now = datetime(2026, 9, 21, 12, 0, tzinfo=timezone.utc)

    def race_with_session(self, status: str, start_time: datetime | None):
        return SimpleNamespace(
            schedule=[
                SimpleNamespace(status=status, startTime=start_time),
            ]
        )

    def test_listens_to_an_ongoing_session(self) -> None:
        race = self.race_with_session("ongoing", None)

        self.assertTrue(should_listen_to_live_streams(race, self.now, 10))

    def test_listens_to_session_starting_within_window(self) -> None:
        race = self.race_with_session(
            "scheduled", self.now + timedelta(minutes=10)
        )

        self.assertTrue(should_listen_to_live_streams(race, self.now, 10))

    def test_does_not_listen_to_session_starting_later(self) -> None:
        race = self.race_with_session(
            "scheduled", self.now + timedelta(minutes=11)
        )

        self.assertFalse(should_listen_to_live_streams(race, self.now, 10))

    def test_does_not_listen_to_finished_session(self) -> None:
        race = self.race_with_session(
            "finished", self.now - timedelta(minutes=1)
        )

        self.assertFalse(should_listen_to_live_streams(race, self.now, 10, 240))

    def test_listens_to_scheduled_session_after_start_within_stop_window(self) -> None:
        race = self.race_with_session(
            "scheduled", self.now - timedelta(minutes=1)
        )

        self.assertTrue(should_listen_to_live_streams(race, self.now, 10, 240))

    def test_does_not_listen_after_stop_window(self) -> None:
        race = self.race_with_session(
            "scheduled", self.now - timedelta(minutes=241)
        )

        self.assertFalse(should_listen_to_live_streams(race, self.now, 10, 240))

    def test_build_schedule_chooses_nearest_future_session(self) -> None:
        race = SimpleNamespace(
            id="race-1",
            name="Race 1",
            schedule=[
                SimpleNamespace(
                    id="session-late",
                    name="Late",
                    status="scheduled",
                    startTime=self.now + timedelta(hours=2),
                ),
                SimpleNamespace(
                    id="session-next",
                    name="Next",
                    status="scheduled",
                    startTime=self.now + timedelta(minutes=30),
                ),
            ],
        )

        schedule = build_worker_schedule([race], self.now, 10, 240)

        self.assertIsNotNone(schedule)
        self.assertEqual(schedule.sessionId, "session-next")
        self.assertEqual(schedule.listenFrom, self.now + timedelta(minutes=20))
        self.assertEqual(schedule.listenUntil, self.now + timedelta(minutes=270))

    def test_build_schedule_ignores_finished_sessions(self) -> None:
        race = SimpleNamespace(
            id="race-1",
            name="Race 1",
            schedule=[
                SimpleNamespace(
                    id="session-finished",
                    name="Finished",
                    status="finished",
                    startTime=self.now + timedelta(minutes=5),
                ),
            ],
        )

        self.assertIsNone(build_worker_schedule([race], self.now, 10, 240))

    def test_build_schedule_uses_ongoing_session_immediately(self) -> None:
        race = SimpleNamespace(
            id="race-1",
            name="Race 1",
            schedule=[
                SimpleNamespace(
                    id="session-live",
                    name="Live",
                    status="ongoing",
                    startTime=self.now - timedelta(minutes=30),
                ),
            ],
        )

        schedule = build_worker_schedule([race], self.now, 10, 240)

        self.assertIsNotNone(schedule)
        self.assertEqual(schedule.sessionId, "session-live")
        self.assertEqual(schedule.listenFrom, self.now)
        self.assertTrue(is_schedule_live_active(schedule, self.now))

    def test_next_worker_run_sleeps_until_listen_window_or_idle_check(self) -> None:
        race = SimpleNamespace(
            id="race-1",
            name="Race 1",
            schedule=[
                SimpleNamespace(
                    id="session-next",
                    name="Next",
                    status="scheduled",
                    startTime=self.now + timedelta(hours=2),
                ),
            ],
        )
        schedule = build_worker_schedule([race], self.now, 10, 240)

        self.assertEqual(
            seconds_until_next_worker_run(
                schedule,
                self.now,
                False,
                idle_check_seconds=3600,
                near_session_check_seconds=300,
            ),
            3600,
        )
