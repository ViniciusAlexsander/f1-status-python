import os
import json
import redis.asyncio as redis
from fastapi import FastAPI

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")

app = FastAPI()

redis_client = redis.from_url(
    REDIS_URL,
    decode_responses=True
)


@app.get("/positions")
async def get_positions():
  data = await redis_client.get("openf1:active_session_positions")
  return json.loads(data) if data else []