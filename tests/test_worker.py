import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from worker.worker import should_listen_to_live_streams


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

        self.assertFalse(should_listen_to_live_streams(race, self.now, 10))