from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import asyncio
import os
import random
from typing import Dict, List
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Leader Key-Value Store")

# In-memory key-value store
store: Dict[str, str] = {}

# Configuration from environment variables
WRITE_QUORUM = int(os.getenv("WRITE_QUORUM", "3"))
MIN_DELAY = float(os.getenv("MIN_DELAY", "0.0001"))  # 0.1ms
MAX_DELAY = float(os.getenv("MAX_DELAY", "0.001"))  # 1ms

# Follower URLs (configured in docker-compose)
FOLLOWER_URLS = [
    f"http://follower{i}:8000" for i in range(1, 6)
]

logger.info(f"Leader started with WRITE_QUORUM={WRITE_QUORUM}, "
            f"MIN_DELAY={MIN_DELAY}, MAX_DELAY={MAX_DELAY}")


class WriteRequest(BaseModel):
    key: str
    value: str


class ReplicationRequest(BaseModel):
    key: str
    value: str


class WriteResponse(BaseModel):
    status: str
    key: str
    value: str
    acks_received: int
    required_acks: int


class ReadResponse(BaseModel):
    key: str
    value: str


async def replicate_to_follower(
        client: httpx.AsyncClient,
        follower_url: str,
        key: str,
        value: str
) -> bool:
    """
    Replicate a write to a single follower with simulated network delay.
    Returns True if acknowledgment received, False otherwise.
    """
    try:
        # Simulate network lag
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        await asyncio.sleep(delay)

        # Send replication request
        response = await client.post(
            f"{follower_url}/replicate",
            json={"key": key, "value": value},
            timeout=5.0
        )

        if response.status_code == 200:
            logger.info(f"Replication to {follower_url} succeeded (delay: {delay:.4f}s)")
            return True
        else:
            logger.warning(f"Replication to {follower_url} failed: {response.status_code}")
            return False

    except Exception as e:
        logger.error(f"Replication to {follower_url} error: {e}")
        return False


@app.post("/write", response_model=WriteResponse)
async def write_key_value(request: WriteRequest):
    """
    Write operation: Updates leader's store and replicates to followers.
    Uses semi-synchronous replication with configurable write quorum.
    """
    key = request.key
    value = request.value

    # Step 1: Write to leader's local store
    store[key] = value
    logger.info(f"Leader wrote: {key}={value}")

    # Step 2: Replicate to all followers concurrently
    async with httpx.AsyncClient() as client:
        # Create replication tasks for all followers
        replication_tasks = [
            replicate_to_follower(client, follower_url, key, value)
            for follower_url in FOLLOWER_URLS
        ]

        # FIXED: Wait only for quorum, not all followers
        if WRITE_QUORUM >= len(FOLLOWER_URLS):
            # If quorum equals total followers, wait for all
            results = await asyncio.gather(*replication_tasks, return_exceptions=True)
            acks_received = sum(1 for result in results if result is True)
        else:
            # Wait for only the required quorum (semi-synchronous!)
            done_tasks = []
            pending_tasks = set(replication_tasks)

            while len(done_tasks) < WRITE_QUORUM and pending_tasks:
                # Wait for next task to complete
                done, pending_tasks = await asyncio.wait(
                    pending_tasks,
                    return_when=asyncio.FIRST_COMPLETED
                )
                done_tasks.extend(done)

            # Count successful acknowledgments from completed tasks
            acks_received = sum(1 for task in done_tasks if task.result() is True)

            # Cancel remaining pending tasks (don't wait for them!)
            for task in pending_tasks:
                task.cancel()

    logger.info(f"Replication complete: {acks_received}/{len(FOLLOWER_URLS)} acks "
                f"(required: {WRITE_QUORUM})")

    # Step 3: Check if write quorum is satisfied
    if acks_received >= WRITE_QUORUM:
        return WriteResponse(
            status="success",
            key=key,
            value=value,
            acks_received=acks_received,
            required_acks=WRITE_QUORUM
        )
    else:
        return WriteResponse(
            status="partial_failure",
            key=key,
            value=value,
            acks_received=acks_received,
            required_acks=WRITE_QUORUM
        )


@app.get("/read/{key}", response_model=ReadResponse)
async def read_key_value(key: str):
    """
    Read operation: Returns value from leader's store.
    """
    if key not in store:
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")

    return ReadResponse(key=key, value=store[key])


@app.get("/data")
async def get_all_data():
    """
    Returns all key-value pairs in the leader's store.
    Used for consistency checking.
    """
    return {"role": "leader", "data": store, "count": len(store)}


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy", "role": "leader"}
