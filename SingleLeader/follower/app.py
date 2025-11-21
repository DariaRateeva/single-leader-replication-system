from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import os
import logging
import time
from typing import Dict

# Configure logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = FastAPI(title="Follower Key-Value Store")

# In-memory key-value store
store: Dict[str, str] = {}

# Follower identification
FOLLOWER_ID = os.getenv("FOLLOWER_ID", "unknown")

logger.info(f"🔵 Follower {FOLLOWER_ID} started and ready")


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
    start_time = time.time()
    key = request.key
    value = request.value

    # Apply the write to follower's local store
    write_start = time.time()
    store[key] = value
    write_duration = (time.time() - write_start) * 1000

    total_duration = (time.time() - start_time) * 1000

    logger.info(
        f"[{FOLLOWER_ID}] REPLICATE: {key}={value[:20]}... | "
        f"write={write_duration:.3f}ms, total={total_duration:.3f}ms | "
        f"store_size={len(store)}"
    )

    # Send acknowledgment back to leader
    return {
        "status": "acknowledged",
        "follower_id": FOLLOWER_ID,
        "processing_time_ms": round(total_duration, 3)
    }


@app.get("/read/{key}", response_model=ReadResponse)
async def read_key_value(key: str):
    """
    Read operation: Returns value from follower's store.
    """
    start_time = time.time()

    if key not in store:
        duration = (time.time() - start_time) * 1000
        logger.warning(
            f"[{FOLLOWER_ID}] READ: Key '{key}' not found (took {duration:.3f}ms)"
        )
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")

    duration = (time.time() - start_time) * 1000
    logger.info(
        f"[{FOLLOWER_ID}] READ: {key} retrieved in {duration:.3f}ms"
    )
    return ReadResponse(key=key, value=store[key])


@app.get("/data")
async def get_all_data():
    """
    Returns all key-value pairs in the follower's store.
    Used for consistency checking.
    """
    start_time = time.time()
    data = {
        "role": "follower",
        "follower_id": FOLLOWER_ID,
        "data": store,
        "count": len(store)
    }
    duration = (time.time() - start_time) * 1000
    logger.info(
        f"[{FOLLOWER_ID}] DATA: Retrieved {len(store)} keys in {duration:.3f}ms"
    )
    return data


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "role": "follower",
        "follower_id": FOLLOWER_ID,
        "store_size": len(store)
    }