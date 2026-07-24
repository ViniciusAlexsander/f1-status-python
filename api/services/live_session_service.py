from collections.abc import AsyncGenerator

from api.domain.livetiming_signalrcore_client import LivetimingSignalrcoreClient
from api.domain.parsers.utils import normalize_stream_data


class LiveSessionService:
    def __init__(self, client: LivetimingSignalrcoreClient) -> None:
        self.client = client

    async def stream_session_state(self) -> AsyncGenerator[dict, None]:
        await self.client.ensure_connected()
        queue = self.client.subscribe("SessionData")

        try:
            while True:
                event = await queue.get()
                payload = event.payload

                if not isinstance(payload, dict):
                    continue

                current_state = {
                    "lap": 0,
                    "trackStatus": None,
                    "sessionStatus": None,
                }

                if "Series" in payload:
                    for lap_data in normalize_stream_data(payload["Series"]):
                        if "Lap" in lap_data:
                            current_state["lap"] = lap_data["Lap"]

                if "StatusSeries" in payload:
                    for status_data in normalize_stream_data(payload["StatusSeries"]):
                        if "TrackStatus" in status_data:
                            current_state["trackStatus"] = status_data["TrackStatus"]

                        if "SessionStatus" in status_data:
                            current_state["sessionStatus"] = status_data["SessionStatus"]

                yield current_state.copy()

        finally:
            self.client.unsubscribe("SessionData", queue)
