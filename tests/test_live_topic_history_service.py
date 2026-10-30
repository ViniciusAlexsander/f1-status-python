import json
import unittest
from datetime import datetime, timezone

from api.domain.livetiming_signalrcore_client import LiveTimingEvent
from api.services.live_topic_history_service import LiveTopicHistoryService


class FakeRedis:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}
        self.streams: list[dict[str, str]] = []

    async def get(self, key: str) -> str | None:
        return self.values.get(key)

    async def set(self, key: str, value: str) -> None:
        self.values[key] = value

    async def publish(self, channel: str, value: str) -> None:
        return None

    async def xadd(self, key: str, fields: dict[str, str]) -> str:
        self.streams.append(fields)
        return "1-0"


class FakeLiveTimingClient:
    pass


class LiveTopicHistoryServiceTest(unittest.IsolatedAsyncioTestCase):
    async def test_track_status_and_weather_events_update_redis_snapshots(self) -> None:
        redis = FakeRedis()
        service = LiveTopicHistoryService(FakeLiveTimingClient(), redis)
        events = [
            LiveTimingEvent(
                topic="TrackStatus",
                payload={"Status": "2"},
                received_at=datetime(2026, 10, 30, tzinfo=timezone.utc),
                raw_payload={"Status": "2", "Message": "Yellow"},
            ),
            LiveTimingEvent(
                topic="WeatherData",
                payload={"AirTemp": 25},
                received_at=datetime(2026, 10, 30, tzinfo=timezone.utc),
                raw_payload={"AirTemp": 25, "TrackTemp": 37},
            ),
        ]

        for event in events:
            await service.save_event(event)
            snapshot = json.loads(
                redis.values[f"live_timing:snapshot:{event.topic}"]
            )
            self.assertEqual(snapshot["payload"], event.raw_payload)
            self.assertEqual(snapshot["topic"], event.topic)

        self.assertEqual(len(redis.streams), 2)

    async def test_topic_snapshot_merges_partial_updates(self) -> None:
        redis = FakeRedis()
        service = LiveTopicHistoryService(FakeLiveTimingClient(), redis)
        received_at = datetime(2026, 10, 30, tzinfo=timezone.utc)
        events = [
            LiveTimingEvent(
                topic="TyreStintSeries",
                payload={"Stints": {"1": {"0": {"Compound": "SOFT"}}}},
                received_at=received_at,
                is_snapshot=True,
                raw_payload={"Stints": {"1": {"0": {"Compound": "SOFT"}}}},
            ),
            LiveTimingEvent(
                topic="TyreStintSeries",
                payload={"Stints": {"1": {"0": {"TotalLaps": 5}}}},
                received_at=received_at,
                raw_payload={"Stints": {"1": {"0": {"TotalLaps": 5}}}},
            ),
        ]

        for event in events:
            await service.save_event(event)

        snapshot = json.loads(redis.values["live_timing:snapshot:TyreStintSeries"])
        self.assertEqual(
            snapshot["payload"]["Stints"]["1"]["0"],
            {"Compound": "SOFT", "TotalLaps": 5},
        )

    async def test_topic_keyframe_replaces_stale_snapshot(self) -> None:
        redis = FakeRedis()
        service = LiveTopicHistoryService(FakeLiveTimingClient(), redis)
        received_at = datetime(2026, 10, 30, tzinfo=timezone.utc)
        events = [
            {"Stints": {"1": {"0": {"Compound": "SOFT"}}}},
            {"Stints": {"2": {"0": {"Compound": "HARD"}}}, "_kf": True},
        ]

        for index, payload in enumerate(events):
            await service.save_event(
                LiveTimingEvent(
                    topic="TyreStintSeries",
                    payload=payload,
                    received_at=received_at,
                    is_snapshot=index == 0,
                    raw_payload=payload,
                )
            )

        snapshot = json.loads(redis.values["live_timing:snapshot:TyreStintSeries"])
        self.assertEqual(snapshot["payload"], events[-1])

    async def test_race_control_snapshot_accumulates_messages(self) -> None:
        redis = FakeRedis()
        service = LiveTopicHistoryService(FakeLiveTimingClient(), redis)
        received_at = datetime(2026, 10, 30, tzinfo=timezone.utc)
        messages = [
            {"Utc": "2026-10-30T12:00:00Z", "Message": "Green"},
            {"Utc": "2026-10-30T12:01:00Z", "Message": "Yellow"},
        ]

        for message in messages:
            await service.save_event(
                LiveTimingEvent(
                    topic="RaceControlMessages",
                    payload={"Messages": [message]},
                    received_at=received_at,
                    raw_payload={"Messages": [message]},
                )
            )

        snapshot = json.loads(
            redis.values["live_timing:snapshot:RaceControlMessages"]
        )
        self.assertEqual(snapshot["payload"]["Messages"], messages)

    async def test_race_control_snapshot_accumulates_indexed_messages(self) -> None:
        redis = FakeRedis()
        service = LiveTopicHistoryService(FakeLiveTimingClient(), redis)
        received_at = datetime(2026, 10, 30, tzinfo=timezone.utc)
        messages = {
            "65": {"Utc": "2026-10-30T12:00:00Z", "Message": "Green"},
            "66": {"Utc": "2026-10-30T12:01:00Z", "Message": "Yellow"},
        }

        for message_id, message in messages.items():
            await service.save_event(
                LiveTimingEvent(
                    topic="RaceControlMessages",
                    payload={"Messages": {message_id: message}},
                    received_at=received_at,
                    raw_payload={"Messages": {message_id: message}},
                )
            )

        snapshot = json.loads(
            redis.values["live_timing:snapshot:RaceControlMessages"]
        )
        self.assertEqual(snapshot["payload"]["Messages"], messages)


if __name__ == "__main__":
    unittest.main()
