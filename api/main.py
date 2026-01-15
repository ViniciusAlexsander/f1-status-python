from fastapi import FastAPI
import redis.asyncio as redis
import json

app = FastAPI()
redis_client = redis.from_url("redis://redis:6379")


@app.get("/positions")
async def get_positions():
  data = await redis_client.get("openf1:active_session_positions")
  return json.loads(data) if data else []