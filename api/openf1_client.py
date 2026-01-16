import httpx

OPENF1_API_URL = "https://api.openf1.org/v1"


async def get_meetings(year: int):
    print(f"get_meetings")
    async with httpx.AsyncClient(timeout=10) as client:
        response = await client.get(
            f"{OPENF1_API_URL}/meetings", params={"year": year}
        )

        response.raise_for_status()
        data = response.json()
        return data
