"""SignalR topics used by the live timing API."""

EXISTING_SIGNALR_TOPICS = ("TimingData", "SessionData", "CurrentTyres")
RAW_FORWARD_SIGNALR_TOPICS = (
    "SessionInfo",
    "ExtrapolatedClock",
    "WeatherData",
    "RaceControlMessages",
)
REQUIRED_SIGNALR_TOPICS = EXISTING_SIGNALR_TOPICS + RAW_FORWARD_SIGNALR_TOPICS
