from collections.abc import AsyncGenerator
from datetime import date
from typing import Any

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.domain.parsers.utils import normalize_stream_data
from api.services.live_timing_cache import LiveTimingCache


class LiveSessionService:
    def __init__(
            self, 
            client: LivetimingSignalrcoreClient, 
            cache: LiveTimingCache
        ) -> None:
        self.client = client
        self.cache = cache

    async def stream_session_state(self, ) -> AsyncGenerator[dict, None]:
        cache_key = f"SessionData:{date.today()}"
        cached_event = await self.cache.get_snapshot(cache_key)

        if cached_event is not None:
            cached_payload = cached_event.get("payload")
            yield cached_payload

        await self.client.ensure_connected()
        queue = self.client.subscribe("SessionData")

        try:
            while True:
                event = await queue.get()
                payload = event.payload

                if not isinstance(payload, dict):
                    continue

                current_state = self._parse_session_state(payload)

                event_data = {
                    "topic": event.topic,
                    "payload": current_state,
                    "receivedAt": event.received_at.isoformat(),
                    "isSnapshot": event.is_snapshot,
                }

                await self.cache.save_snapshot(cache_key, event_data)

                yield current_state

        finally:
            self.client.unsubscribe("SessionData", queue)

    def _parse_session_state(self, payload: dict[str, Any]) -> dict[str, Any]:
        current_state: dict[str, Any] = {
            "lap": 0,
            "trackStatus": None,
            "sessionStatus": None,
            "qualifyingPart": None,
            "qualifyingPartStartedAt": None,
            "qualifyingPartFinishedAt": None,
            "qualifyingRedFlagIntervals": [],
        }

        if "Series" in payload:
            series_data = list(normalize_stream_data(payload["Series"]))
            for lap_data in series_data:
                if "Lap" in lap_data:
                    current_state["lap"] = lap_data["Lap"]

            qualifying_parts = [
                item
                for item in series_data
                if isinstance(item.get("QualifyingPart"), int)
                and not isinstance(item.get("QualifyingPart"), bool)
                and item["QualifyingPart"] in (1, 2, 3)
            ]
            if qualifying_parts:
                qualifying_parts.sort(key=lambda item: str(item.get("Utc", "")))
                latest_part = qualifying_parts[-1]
                current_state["qualifyingPart"] = latest_part["QualifyingPart"]

                part_started_at = str(latest_part.get("Utc", ""))
                next_part_at = next(
                    (
                        str(item.get("Utc", ""))
                        for item in qualifying_parts
                        if str(item.get("Utc", "")) > part_started_at
                    ),
                    None,
                )

                status_series = list(
                    normalize_stream_data(payload.get("StatusSeries"))
                )
                status_series.sort(key=lambda item: str(item.get("Utc", "")))
                starts = [
                    item
                    for item in status_series
                    if item.get("SessionStatus") == "Started"
                    and str(item.get("Utc", "")) >= part_started_at
                    and (
                        next_part_at is None
                        or str(item.get("Utc", "")) < next_part_at
                    )
                ]

                if starts:
                    started_at = str(starts[0].get("Utc"))
                    current_state["qualifyingPartStartedAt"] = started_at
                    finished = next(
                        (
                            item
                            for item in status_series
                            if item.get("SessionStatus") == "Finished"
                            and str(item.get("Utc", "")) > started_at
                            and (
                                next_part_at is None
                                or str(item.get("Utc", "")) < next_part_at
                            )
                        ),
                        None,
                    )
                    if finished:
                        current_state["qualifyingPartFinishedAt"] = str(
                            finished.get("Utc")
                        )

                    red_started_at: str | None = None
                    red_intervals: list[dict[str, str | None]] = []
                    for item in status_series:
                        item_utc = str(item.get("Utc", ""))
                        if item_utc < started_at:
                            continue
                        if item.get("TrackStatus") == "Red":
                            if red_started_at is None:
                                red_started_at = item_utc
                        elif red_started_at is not None:
                            red_intervals.append(
                                {"startedAt": red_started_at, "endedAt": item_utc}
                            )
                            red_started_at = None

                    if red_started_at is not None:
                        red_intervals.append(
                            {"startedAt": red_started_at, "endedAt": None}
                        )
                    current_state["qualifyingRedFlagIntervals"] = red_intervals

        if "StatusSeries" in payload:
            status_series = sorted(
                normalize_stream_data(payload["StatusSeries"]),
                key=lambda item: str(item.get("Utc", "")),
            )
            for status_data in status_series:
                if "TrackStatus" in status_data:
                    current_state["trackStatus"] = status_data["TrackStatus"]

                if "SessionStatus" in status_data:
                    current_state["sessionStatus"] = status_data["SessionStatus"]

        return current_state
