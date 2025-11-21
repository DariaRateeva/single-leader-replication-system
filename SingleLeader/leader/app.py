from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import httpx
import asyncio
import os
import random
import time
from typing import Dict, List
import logging
from contextlib import asynccontextmanager

# Configure logging with timestamps
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Global HTTP client
http_client = None


# Lifespan context manager
@asynccontextmanager
async def lifespan(app: FastAPI):
    global http_client
    start_time = time.time()
    http_client = httpx.AsyncClient()
    duration = (time.time() - start_time) * 1000
    logger.info(f"✓ HTTP client initialized in {duration:.2f}ms")
    yield
    start_time = time.time()
    await http_client.aclose()
    duration = (time.time() - start_time) * 1000
    logger.info(f"✓ HTTP client closed in {duration:.2f}ms")


# Create FastAPI app with lifespan (ONLY ONCE!)
app = FastAPI(title="Leader Key-Value Store", lifespan=lifespan)

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

logger.info(f"🚀 Leader started with configuration:")
logger.info(f"   WRITE_QUORUM = {WRITE_QUORUM}")
logger.info(f"   MIN_DELAY = {MIN_DELAY}s ({MIN_DELAY * 1000:.2f}ms)")
logger.info(f"   MAX_DELAY = {MAX_DELAY}s ({MAX_DELAY * 1000:.2f}ms)")
logger.info(f"   FOLLOWERS = {len(FOLLOWER_URLS)}")


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
    timing: dict  # Added timing information


class ReadResponse(BaseModel):
    key: str
    value: str


async def replicate_to_follower(
        client: httpx.AsyncClient,
        follower_url: str,
        key: str,
        value: str,
        request_id: str
) -> tuple[bool, float, float]:
    """
    Replicate a write to a single follower with simulated network delay.
    Returns (success, delay_time, total_time) tuple.
    """
    operation_start = time.time()

    try:
        # Simulate network lag
        delay = random.uniform(MIN_DELAY, MAX_DELAY)
        delay_start = time.time()
        await asyncio.sleep(delay)
        delay_duration = (time.time() - delay_start) * 1000

        # Send replication request
        request_start = time.time()
        response = await client.post(
            f"{follower_url}/replicate",
            json={"key": key, "value": value},
            timeout=5.0
        )
        request_duration = (time.time() - request_start) * 1000
        total_duration = (time.time() - operation_start) * 1000

        if response.status_code == 200:
            logger.info(
                f"[{request_id}] ✓ {follower_url.split('//')[1]}: "
                f"delay={delay_duration:.2f}ms, request={request_duration:.2f}ms, "
                f"total={total_duration:.2f}ms"
            )
            return True, delay_duration, total_duration
        else:
            logger.warning(
                f"[{request_id}] ✗ {follower_url.split('//')[1]} failed: "
                f"HTTP {response.status_code} (total={total_duration:.2f}ms)"
            )
            return False, delay_duration, total_duration

    except Exception as e:
        total_duration = (time.time() - operation_start) * 1000
        logger.error(
            f"[{request_id}] ✗ {follower_url.split('//')[1]} error: {e} "
            f"(total={total_duration:.2f}ms)"
        )
        return False, 0, total_duration


@app.post("/write", response_model=WriteResponse)
async def write_key_value(request: WriteRequest):
    """
    Write operation: Updates leader's store and replicates to followers.
    Uses semi-synchronous replication with configurable write quorum.
    """
    request_id = f"W-{int(time.time() * 1000) % 10000:04d}"  # Short unique ID
    operation_start = time.time()

    key = request.key
    value = request.value

    logger.info(f"[{request_id}] ═══ WRITE REQUEST STARTED: {key}={value[:20]}... ═══")

    # Step 1: Write to leader's local store
    step1_start = time.time()
    store[key] = value
    step1_duration = (time.time() - step1_start) * 1000
    logger.info(f"[{request_id}] Step 1: Leader write completed in {step1_duration:.3f}ms")

    # Step 2: Replicate to all followers concurrently
    step2_start = time.time()
    logger.info(f"[{request_id}] Step 2: Starting replication to {len(FOLLOWER_URLS)} followers")

    # Create tasks for all followers
    pending_tasks = [
        asyncio.create_task(
            replicate_to_follower(http_client, follower_url, key, value, request_id)
        )
        for follower_url in FOLLOWER_URLS
    ]

    done_tasks = []
    pending_set = set(pending_tasks)

    # Wait for quorum or all tasks to complete
    quorum_start = time.time()
    while len(done_tasks) < WRITE_QUORUM and pending_set:
        # Wait for next task to complete
        done, pending_set = await asyncio.wait(
            pending_set,
            return_when=asyncio.FIRST_COMPLETED
        )
        done_tasks.extend(done)

        # Log progress
        successful = sum(
            1 for task in done_tasks
            if not task.cancelled() and task.exception() is None and task.result()[0] is True
        )
        logger.info(
            f"[{request_id}]    Progress: {len(done_tasks)}/{len(FOLLOWER_URLS)} "
            f"completed, {successful} successful"
        )

    quorum_duration = (time.time() - quorum_start) * 1000

    # Count successful acknowledgments and collect timing info
    acks_received = 0
    follower_timings = []

    for task in done_tasks:
        if not task.cancelled() and task.exception() is None:
            success, delay_time, total_time = task.result()
            if success:
                acks_received += 1
            follower_timings.append({
                "delay_ms": delay_time,
                "total_ms": total_time,
                "success": success
            })

    # Cancel remaining pending tasks if quorum is met
    if pending_set:
        cancel_start = time.time()
        for task in pending_set:
            task.cancel()

        # Wait for cancelled tasks to complete
        await asyncio.gather(*pending_set, return_exceptions=True)
        cancel_duration = (time.time() - cancel_start) * 1000
        logger.info(
            f"[{request_id}] Step 2b: Cancelled {len(pending_set)} "
            f"remaining tasks in {cancel_duration:.2f}ms"
        )

    step2_duration = (time.time() - step2_start) * 1000
    total_duration = (time.time() - operation_start) * 1000

    logger.info(
        f"[{request_id}] Step 2: Replication completed in {step2_duration:.2f}ms "
        f"(quorum reached in {quorum_duration:.2f}ms)"
    )
    logger.info(
        f"[{request_id}] Step 3: Acknowledgments: {acks_received}/{len(FOLLOWER_URLS)} "
        f"(required: {WRITE_QUORUM})"
    )

    # Step 3: Prepare response
    status = "success" if acks_received >= WRITE_QUORUM else "partial_failure"

    timing_info = {
        "total_ms": round(total_duration, 3),
        "leader_write_ms": round(step1_duration, 3),
        "replication_ms": round(step2_duration, 3),
        "quorum_wait_ms": round(quorum_duration, 3),
        "follower_details": follower_timings
    }

    logger.info(
        f"[{request_id}] ═══ WRITE REQUEST COMPLETED: {status.upper()} "
        f"in {total_duration:.2f}ms ═══"
    )

    return WriteResponse(
        status=status,
        key=key,
        value=value,
        acks_received=acks_received,
        required_acks=WRITE_QUORUM,
        timing=timing_info
    )


@app.get("/read/{key}", response_model=ReadResponse)
async def read_key_value(key: str):
    """
    Read operation: Returns value from leader's store.
    """
    start_time = time.time()

    if key not in store:
        duration = (time.time() - start_time) * 1000
        logger.warning(f"READ: Key '{key}' not found (took {duration:.3f}ms)")
        raise HTTPException(status_code=404, detail=f"Key '{key}' not found")

    duration = (time.time() - start_time) * 1000
    logger.info(f"READ: {key} retrieved in {duration:.3f}ms")
    return ReadResponse(key=key, value=store[key])


@app.get("/data")
async def get_all_data():
    """
    Returns all key-value pairs in the leader's store.
    Used for consistency checking.
    """
    start_time = time.time()
    data = {
        "role": "leader",
        "data": store,
        "count": len(store)
    }
    duration = (time.time() - start_time) * 1000
    logger.info(f"DATA: Retrieved {len(store)} keys in {duration:.3f}ms")
    return data


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "role": "leader",
        "quorum": WRITE_QUORUM,
        "followers": len(FOLLOWER_URLS)
    }