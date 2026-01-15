import asyncio
from datetime import datetime, timezone
import json
import redis.asyncio as redis

from openf1_client import get_active_sessions, get_current_meeting, get_positions

ACTIVE_SESSION_DELAY = 10        # 10s
NO_SESSION_DELAY = 60 * 60       # 1h
ERROR_DELAY = 30                 # 30s em erro

async def main():
    print(f"Starting fetch positions worker: {datetime.now(timezone.utc).isoformat()}")
    redis_client = redis.from_url("redis://redis:6379")

    while True:
      sleep_time = ERROR_DELAY
      try:
        meeting = await get_current_meeting()
        active_sessions = await get_active_sessions(meeting["meeting_key"])

        if not active_sessions:
          payload = {
              "status": "NO_ACTIVE_SESSION",
              "updated_at": datetime.now(timezone.utc).isoformat(),
              "message": "No active F1 session at the moment"
          }

          await redis_client.set(
              "openf1:active_session_positions",
              json.dumps(payload),
              ex=NO_SESSION_DELAY
          )

          print("No active session found", flush=True)
          sleep_time = NO_SESSION_DELAY
        else:
          current_session = active_sessions[0]
          raw_positions = await get_positions(current_session["session_key"])
          latest_positions = get_latest_positions(raw_positions)

          payload = {
            "status": "ACTIVE",
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "session": {
                "session_key": current_session["session_key"],
                "session_name": current_session["session_name"],
                "meeting_key": current_session["meeting_key"]
            },
            "positions": latest_positions
          }

          await redis_client.set(
            "openf1:active_session_positions",
            json.dumps(payload),
            ex=30
          )
          print(
              f"Positions updated | session={current_session['session_key']}",
              flush=True
          )

          sleep_time = ACTIVE_SESSION_DELAY
      except Exception as e:
        print(f"Worker error: {e}", flush=True)
        sleep_time = ERROR_DELAY

      print(f"Sleeping for {sleep_time}s", flush=True)
      await asyncio.sleep(sleep_time)

def get_latest_positions(positions: list[dict]) -> list[dict]:
    latest = {}

    for p in positions:
        driver = p["driver_number"]
        ts = datetime.fromisoformat(p["date"].replace("Z", "+00:00"))

        if driver not in latest or ts > latest[driver][0]:
            latest[driver] = (ts, p)

    return sorted(
        [item[1] for item in latest.values()],
        key=lambda x: x["position"]
    )

asyncio.run(main())