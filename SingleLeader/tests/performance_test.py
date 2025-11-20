import httpx
import asyncio
import time
import random
from typing import List, Dict
import statistics
import matplotlib.pyplot as plt
import os
import subprocess

BASE_URL = "http://localhost:8000"
NUM_WRITES = 10000
NUM_KEYS = 100
NUM_THREADS = 20  # >10 as required


async def perform_write(client: httpx.AsyncClient, key: str, value: str) -> float:
    """
    Perform a single write operation and return the latency in milliseconds.
    """
    start_time = time.time()
    try:
        response = await client.post(
            f"{BASE_URL}/write",
            json={"key": key, "value": value},
            timeout=10.0
        )
        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000  # Convert to milliseconds
        return latency_ms
    except Exception as e:
        end_time = time.time()
        latency_ms = (end_time - start_time) * 1000
        print(f"Write error: {e}")
        return latency_ms


async def run_concurrent_writes(num_writes: int, num_keys: int) -> List[float]:
    """
    Run concurrent writes and return list of latencies.
    """
    print(f"Executing {num_writes} writes concurrently across {num_keys} keys...")

    latencies = []

    async with httpx.AsyncClient() as client:
        # Create batches to avoid overwhelming the system
        batch_size = NUM_THREADS

        for batch_start in range(0, num_writes, batch_size):
            batch_end = min(batch_start + batch_size, num_writes)
            tasks = []

            for i in range(batch_start, batch_end):
                key = f"key_{i % num_keys}"  # Distribute across NUM_KEYS
                value = f"value_{i}_{random.randint(1000, 9999)}"
                task = perform_write(client, key, value)
                tasks.append(task)

            # Execute batch concurrently
            batch_latencies = await asyncio.gather(*tasks)
            latencies.extend(batch_latencies)

            # Progress indicator
            if (batch_start + batch_size) % 1000 == 0:
                print(f"Progress: {batch_start + batch_size}/{num_writes} writes completed")

    return latencies


async def check_consistency() -> Dict[str, int]:
    """
    Check data consistency across leader and followers.
    Returns dictionary with data counts from each node.
    """
    print("\nChecking data consistency...")

    consistency_data = {}

    async with httpx.AsyncClient() as client:
        # Check leader
        try:
            leader_response = await client.get(f"{BASE_URL}/data", timeout=5.0)
            leader_data = leader_response.json()
            consistency_data["leader"] = leader_data["count"]
            print(f"Leader: {leader_data['count']} keys")
        except Exception as e:
            print(f"Error fetching leader data: {e}")
            consistency_data["leader"] = -1

        # Note: In the docker-compose setup, followers are not exposed
        # You would need to expose them on different ports to check
        # For demonstration, showing how it would work:
        print("\nNote: Followers are not exposed on host ports in current setup.")
        print("To check followers, you'd need to add port mappings in docker-compose.yml")

    return consistency_data


def update_docker_compose_quorum(quorum: int):
    """
    Update the WRITE_QUORUM in .env file and restart docker-compose.
    """
    print(f"\n{'=' * 60}")
    print(f"Configuring system with WRITE_QUORUM={quorum}")
    print(f"{'=' * 60}")

    # Update .env file
    with open(".env", "w") as f:
        f.write(f"WRITE_QUORUM={quorum}\n")
        f.write("MIN_DELAY=0.0001\n")
        f.write("MAX_DELAY=0.001\n")

    # Restart docker-compose (FIXED: use space instead of hyphen)
    print("Restarting containers...")
    subprocess.run(["docker", "compose", "down"], capture_output=True)
    subprocess.run(["docker", "compose", "up", "-d"], capture_output=True)

    # Wait for containers to be ready
    print("Waiting for containers to be ready...")
    time.sleep(10)


async def performance_analysis():
    """
    Main performance analysis: test quorum values from 1 to 5.
    """
    print("\n" + "=" * 60)
    print("PERFORMANCE ANALYSIS: Write Quorum vs. Latency")
    print("=" * 60)

    results = {}

    for quorum in range(1, 6):
        # Update configuration
        update_docker_compose_quorum(quorum)

        # Run performance test
        print(f"\nTesting with WRITE_QUORUM={quorum}")
        latencies = await run_concurrent_writes(NUM_WRITES, NUM_KEYS)

        # Calculate statistics
        avg_latency = statistics.mean(latencies)
        median_latency = statistics.median(latencies)
        p95_latency = statistics.quantiles(latencies, n=20)[18]  # 95th percentile
        p99_latency = statistics.quantiles(latencies, n=100)[98]  # 99th percentile

        results[quorum] = {
            "avg": avg_latency,
            "median": median_latency,
            "p95": p95_latency,
            "p99": p99_latency,
            "all_latencies": latencies
        }

        print(f"\nResults for WRITE_QUORUM={quorum}:")
        print(f"  Average latency: {avg_latency:.2f} ms")
        print(f"  Median latency:  {median_latency:.2f} ms")
        print(f"  P95 latency:     {p95_latency:.2f} ms")
        print(f"  P99 latency:     {p99_latency:.2f} ms")

        # Check consistency
        consistency = await check_consistency()
        print(f"  Consistency check: {consistency}")

    # Plot results
    plot_results(results)

    # Print analysis
    print_analysis(results)

    return results


def plot_results(results: Dict[int, Dict]):
    """
    Create plots of quorum vs latency.
    """
    print("\nGenerating plots...")

    quorums = sorted(results.keys())
    avg_latencies = [results[q]["avg"] for q in quorums]
    median_latencies = [results[q]["median"] for q in quorums]
    p95_latencies = [results[q]["p95"] for q in quorums]

    # Create plot
    plt.figure(figsize=(12, 6))

    # Plot 1: Average latency
    plt.subplot(1, 2, 1)
    plt.plot(quorums, avg_latencies, marker='o', linewidth=2, markersize=8)
    plt.xlabel('Write Quorum', fontsize=12)
    plt.ylabel('Average Latency (ms)', fontsize=12)
    plt.title('Write Quorum vs. Average Latency', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.xticks(quorums)

    # Plot 2: Multiple percentiles
    plt.subplot(1, 2, 2)
    plt.plot(quorums, avg_latencies, marker='o', label='Average', linewidth=2)
    plt.plot(quorums, median_latencies, marker='s', label='Median', linewidth=2)
    plt.plot(quorums, p95_latencies, marker='^', label='P95', linewidth=2)
    plt.xlabel('Write Quorum', fontsize=12)
    plt.ylabel('Latency (ms)', fontsize=12)
    plt.title('Write Quorum vs. Latency (Multiple Metrics)', fontsize=14, fontweight='bold')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xticks(quorums)

    plt.tight_layout()
    plt.savefig('quorum_vs_latency.png', dpi=300, bbox_inches='tight')
    print("Plot saved as 'quorum_vs_latency.png'")
    plt.show()


def print_analysis(results: Dict[int, Dict]):
    """
    Print detailed analysis and explanation of results.
    """
    print("\n" + "=" * 60)
    print("ANALYSIS AND EXPLANATION")
    print("=" * 60)

    print("\n1. LATENCY TRENDS:")
    print("-" * 60)
    for quorum in sorted(results.keys()):
        avg = results[quorum]["avg"]
        print(f"   Quorum {quorum}: {avg:.2f} ms average latency")

    print("\n2. WHY LATENCY INCREASES WITH QUORUM:")
    print("-" * 60)
    print("""
   - With QUORUM=1: Leader waits for only 1 follower acknowledgment
     → Fastest response (leader picks the quickest follower)

   - With QUORUM=3: Leader waits for 3 followers to acknowledge
     → Medium latency (must wait for 3rd-fastest follower)

   - With QUORUM=5: Leader waits for ALL 5 followers
     → Slowest response (must wait for the slowest follower)

   The random network delay (0.1-1ms) means each follower responds
   at different times. Higher quorum = waiting for more followers
   = waiting for slower ones = higher latency.
    """)

    print("\n3. DATA CONSISTENCY EXPECTATIONS:")
    print("-" * 60)
    print("""
   - Semi-synchronous replication means the leader doesn't wait
     for ALL followers before responding to the client.

   - With QUORUM < 5: Some followers might lag behind or miss
     updates if they're slow or temporarily unavailable.

   - Expected outcome: Leader has all 100 keys updated ~100 times each.
     Followers with QUORUM=1 might have missing or stale data.
     Followers with QUORUM=5 should match the leader perfectly.

   - This demonstrates the trade-off between:
     * Performance (low quorum = fast writes)
     * Durability (high quorum = more replicas guaranteed updated)
    """)

    print("\n4. KEY INSIGHTS:")
    print("-" * 60)
    print(f"""
   - Latency increase: {results[5]["avg"] / results[1]["avg"]:.2f}x from quorum=1 to quorum=5
   - This is the classic CAP theorem trade-off in action:
     * Availability (fast responses) vs. Consistency (data safety)
   - Semi-synchronous replication is a practical middle ground
     used by real databases like MySQL and PostgreSQL.
    """)


async def main():
    """Main entry point for performance testing."""
    print("=" * 60)
    print("LAB 4: SINGLE-LEADER REPLICATION PERFORMANCE ANALYSIS")
    print("=" * 60)
    print(f"\nConfiguration:")
    print(f"  - Total writes: {NUM_WRITES}")
    print(f"  - Number of keys: {NUM_KEYS}")
    print(f"  - Concurrent threads: {NUM_THREADS}")
    print(f"  - Quorum values tested: 1, 2, 3, 4, 5")

    results = await performance_analysis()

    print("\n" + "=" * 60)
    print("PERFORMANCE ANALYSIS COMPLETE!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
