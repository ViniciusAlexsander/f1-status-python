import os
import json
import redis.asyncio as redis
from fastapi import FastAPI
from strawberry.fastapi import GraphQLRouter
from schema.schema import schema
import uvicorn
from fastapi.middleware.cors import CORSMiddleware

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
PORT = int(os.getenv("PORT", 8000))

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "https://f1-status-frontend-production.up.railway.app",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


redis_client = redis.from_url(REDIS_URL, decode_responses=True)

graphql_app = GraphQLRouter(schema)

app.include_router(graphql_app, prefix="/graphql")

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
                "message": "No active F1 session at the moment",
            }
    except Exception as e:
        # Redis não acessível
        return {
            "status": "ERROR",
            "updated_at": None,
            "message": f"Redis not available: {e}",
        }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=PORT)
