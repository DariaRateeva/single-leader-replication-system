from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import logging
from typing import Dict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Follower Key-Value Store")

# In-memory key-value store
store: Dict[str, str] = {}

# Follower identification
FOLLOWER_ID = os.getenv("FOLLOWER_ID", "unknown")

logger.info(f"Follower {FOLLOWER_ID} started")


class ReplicationRequest(BaseModel):
    key: str
    value: str


class ReadResponse(BaseModel):
    key: str
    value: str


@app.post("/replicate")
async def replicate(request: ReplicationRequest):
    """
    Receives replication requests from the leader and applies them.
    Executes concurrently (multiple replication requests can be processed simultaneously).
    """
    key = request.key
    value = request.value

    # Apply the write to follower's local store
    store[key] = value
    logger.info(f"Follower {FOLLOWER_ID} replicated: {key}={value}")

    # Send acknowledgment back to leader
    return {"status": "acknowledged", "follower_id": FOLLOWER_ID}


@app.get("/read/{key}", response_model=ReadResponse)
async def read_key_value(key: str):
    """
    Read operation: Returns value from follower's store.
    """
    if key not in store:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")

    return ReadResponse(key=key, value=store[key])


@app.get("/data")
async def get_all_data():
    """
    Returns all key-value pairs in the follower's store.
    Used for consistency checking.
    """
    return {
        "role": "follower",
        "follower_id": FOLLOWER_ID,
        "data": store,
        "count": len(store)
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "role": "follower", "follower_id": FOLLOWER_ID}
