import os
import json
import redis.asyncio as redis
from fastapi import FastAPI
import uvicorn

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT = int(os.getenv("PORT", 8000))

app = FastAPI()

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True
)

@app.get("/positions")
async def get_positions():
    try:
        data = await redis_client.get("openf1:active_session_positions")
        if data:
            return json.loads(data)
        else:
            # Redis acessível mas ainda não tem dados
            return {
                "status": "NO_ACTIVE_SESSION",
                "updated_at": None,
                "message": "No active F1 session at the moment"
            }
    except Exception as e:
        # Redis não acessível
        return {
            "status": "ERROR",
            "updated_at": None,
            "message": f"Redis not available: {e}"
        }

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT)
