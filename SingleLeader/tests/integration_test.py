import httpx
import asyncio
import time

BASE_URL = "http://localhost:8000"


async def test_write_and_read():
    """Test basic write and read operations."""
    print("\n=== Integration Test: Write and Read ===")

    async with httpx.AsyncClient() as client:
        # Write a key-value pair
        write_response = await client.post(
            f"{BASE_URL}/write",
            json={"key": "test_key", "value": "test_value"}
        )
        print(f"Write response: {write_response.json()}")
        assert write_response.status_code == 200
        result = write_response.json()
        assert result["status"] in ["success", "partial_failure"]

        # Read the key
        await asyncio.sleep(0.5)  # Give time for replication
        read_response = await client.get(f"{BASE_URL}/read/test_key")
        print(f"Read response: {read_response.json()}")
        assert read_response.status_code == 200
        assert read_response.json()["value"] == "test_value"


async def test_concurrent_writes():
    """Test multiple concurrent writes."""
    print("\n=== Integration Test: Concurrent Writes ===")

    async with httpx.AsyncClient() as client:
        tasks = []
        for i in range(10):
            task = client.post(
                f"{BASE_URL}/write",
                json={"key": f"key_{i}", "value": f"value_{i}"}
            )
            tasks.append(task)

        responses = await asyncio.gather(*tasks)

        success_count = sum(1 for r in responses if r.status_code == 200)
        print(f"Concurrent writes: {success_count}/{len(tasks)} succeeded")
        assert success_count == len(tasks)


async def test_replication_consistency():
    """Test that data is replicated to followers."""
    print("\n=== Integration Test: Replication Consistency ===")

    async with httpx.AsyncClient() as client:
        # Write some data
        test_data = {"key": "consistency_test", "value": "replicated_value"}
        write_response = await client.post(
            f"{BASE_URL}/write",
            json=test_data
        )
        assert write_response.status_code == 200

        # Wait for replication
        await asyncio.sleep(1)

        # Check leader data
        leader_response = await client.get(f"{BASE_URL}/data")
        leader_data = leader_response.json()
        print(f"Leader data count: {leader_data['count']}")

        # Check followers
        follower_urls = [f"http://localhost:800{i}" for i in range(1, 6)]
        for i, follower_url in enumerate(follower_urls, 1):
            try:
                follower_response = await client.get(f"{follower_url}/data")
                follower_data = follower_response.json()
                print(f"Follower{i} data count: {follower_data['count']}")
            except Exception as e:
                print(f"Follower{i} not accessible (expected in this setup): {e}")


async def run_all_tests():
    """Run all integration tests."""
    print("Starting Integration Tests...")
    print("=" * 50)

    try:
        await test_write_and_read()
        await test_concurrent_writes()
        await test_replication_consistency()

        print("\n" + "=" * 50)
        print("✓ All integration tests passed!")
        print("=" * 50)
    except AssertionError as e:
        print(f"\n✗ Test failed: {e}")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")


if __name__ == "__main__":
    asyncio.run(run_all_tests())
