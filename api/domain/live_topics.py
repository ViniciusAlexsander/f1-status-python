"""SignalR topics used by the live timing API."""

EXISTING_SIGNALR_TOPICS = ("TimingData", "SessionData", "CurrentTyres")
RAW_FORWARD_SIGNALR_TOPICS = (
    "SessionInfo",
    "SessionStatus",
    "ExtrapolatedClock",
    "ArchiveStatus",
    "Heartbeat",
    "TimingDataF1",
    "TimingAppData",
    "TimingStats",
    "LapSeries",
    "TopThree",
    "DriverList",
    "TrackStatus",
    "WeatherData",
    "RaceControlMessages",
    "DriverTracker",
    "Position.z",
    "CarData.z",
    "PitLaneTimeCollection",
    "WeatherDataSeries",
    "TyreStintSeries",
    "ContentStreams",
    "AudioStreams",
)
REQUIRED_SIGNALR_TOPICS = EXISTING_SIGNALR_TOPICS + RAW_FORWARD_SIGNALR_TOPICS
