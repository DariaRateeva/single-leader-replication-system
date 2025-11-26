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

# In-memory key-value store and version tracking
store: Dict[str, str] = {}
key_versions: Dict[str, int] = {}  # Tracks the latest version for each key

# Follower identification
FOLLOWER_ID = os.getenv("FOLLOWER_ID", "unknown")

logger.info(f"🔵 Follower {FOLLOWER_ID} started and ready")


class ReplicationRequest(BaseModel):
    key: str
    value: str
    version: int  # Added version field


class ReadResponse(BaseModel):
    key: str
    value: str
    version: int


@app.post("/replicate")
async def replicate(request: ReplicationRequest):
    """
    Receives replication requests. Uses versioning to handle race conditions
    caused by out-of-order network packets.
    """
    start_time = time.time()
    key = request.key
    value = request.value
    new_version = request.version

    # Check local version to decide if we should update
    current_version = key_versions.get(key, 0)

    write_start = time.time()

    if new_version > current_version:
        # Valid update: apply it
        store[key] = value
        key_versions[key] = new_version
        status_msg = "updated"
    else:
        # Stale update: ignore it (idempotency)
        # We still acknowledge success so the leader knows we are "in sync"
        # (or at least ahead of what the leader sent)
        status_msg = f"ignored (stale v{new_version} <= v{current_version})"

    write_duration = (time.time() - write_start) * 1000
    total_duration = (time.time() - start_time) * 1000

    logger.info(
        f"[{FOLLOWER_ID}] REPLICATE: {key}={value[:20]}... (v{new_version}) | "
        f"Result: {status_msg} | "
        f"write={write_duration:.3f}ms, total={total_duration:.3f}ms"
    )

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

    version = key_versions.get(key, 0)
    duration = (time.time() - start_time) * 1000
    logger.info(
        f"[{FOLLOWER_ID}] READ: {key} (v{version}) retrieved in {duration:.3f}ms"
    )
    return ReadResponse(key=key, value=store[key], version=version)


@app.get("/data")
async def get_all_data():
    """
    Returns all key-value pairs in the follower's store.
    """
    start_time = time.time()
    data = {
        "role": "follower",
        "follower_id": FOLLOWER_ID,
        "data": store,
        "versions": key_versions,
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