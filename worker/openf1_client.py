import httpx

OPENF1_API_URL = "https://api.openf1.org/v1"


async def get_positions(session_key: int):
    print(f"get_positions: {session_key}")
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            f"{OPENF1_API_URL}/position", params={"session_key": session_key}
        )

        print(f"OpenF1 status: {response.status_code}", flush=True)

        response.raise_for_status()  # explode se não for 200

        data = response.json()
        print(f"OpenF1 payload size: {len(data)}", flush=True)

        return data


async def get_current_meeting():
    print(f"get_current_meeting")
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{OPENF1_API_URL}/meetings", params={"meeting_key": "latest"}
        )
        r.raise_for_status()
        return r.json()[0]


async def get_active_sessions(meeting_key: int):
    print(f"get_active_sessions: {meeting_key}")
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(
            f"{OPENF1_API_URL}/sessions", params={"meeting_key": meeting_key}
        )
        r.raise_for_status()

        sessions = r.json()
        return [s for s in sessions if s["date_end"] is None]
