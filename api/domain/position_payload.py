import base64
import json
import zlib
from typing import Any


def decode_z_payload(payload: Any) -> Any:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, (bytes, bytearray)):
        payload = payload.decode("utf-8-sig")
    if not isinstance(payload, str):
        return payload

    text = payload.strip()
    if not text:
        return payload
    if text[0] == "{":
        return json.loads(text)
    if text[0] == '"':
        text = json.loads(text)

    inflated = zlib.decompress(base64.b64decode(text), -zlib.MAX_WBITS)
    return json.loads(inflated.decode("utf-8-sig"))


def compact_position_payload(payload: Any) -> dict[str, Any] | None:
    try:
        decoded = decode_z_payload(payload)
    except (ValueError, TypeError, json.JSONDecodeError, zlib.error):
        return None

    if not isinstance(decoded, dict):
        return None

    samples = decoded.get("Position")
    if not isinstance(samples, list) or not samples:
        return None

    sample = samples[-1]
    if not isinstance(sample, dict):
        return None

    entries = sample.get("Entries")
    if not isinstance(entries, dict):
        return None

    cars: dict[str, dict[str, Any]] = {}
    for racing_number, entry in entries.items():
        if not isinstance(entry, dict):
            continue
        x = entry.get("X")
        y = entry.get("Y")
        if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
            continue
        status = entry.get("Status")
        cars[str(racing_number)] = {
            "x": x,
            "y": y,
            "status": str(status) if status is not None else "OnTrack",
        }

    if not cars:
        return None

    timestamp = sample.get("Timestamp")
    return {
        "timestamp": str(timestamp) if timestamp is not None else None,
        "cars": cars,
    }
