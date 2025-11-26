import httpx
import asyncio
import sys

# Configuration
LEADER_URL = "http://localhost:8000"
FOLLOWER_1_URL = "http://localhost:8001"


async def test_leader_versioning():
    """
    Test 1: Verify Leader assigns strictly increasing versions.
    """
    print(f"\n[Test 1] Integration: Leader Version Increment...")
    key = "version_test_key"

    async with httpx.AsyncClient() as client:
        # Write 1
        resp1 = await client.post(f"{LEADER_URL}/write", json={"key": key, "value": "v1"})
        data1 = resp1.json()
        ver1 = data1.get("version")

        # Write 2
        resp2 = await client.post(f"{LEADER_URL}/write", json={"key": key, "value": "v2"})
        data2 = resp2.json()
        ver2 = data2.get("version")

        # Assertions
        if ver1 is not None and ver2 is not None and ver2 > ver1:
            print(f"  ✓ Leader assigned versions {ver1} -> {ver2} correctly.")
            return True
        else:
            print(f"  ✗ Failed: Versions {ver1} -> {ver2}")
            return False


async def test_replication_ordering():
    """
    Test 2: Verify follower handles out-of-order delivery correctly.
    """
    print(f"\n[Test 2] Integration: Out-of-Order Replication Handling...")
    key = "ordering_test_key"
    val_latest = "LATEST_VALUE"
    val_stale = "STALE_VALUE"

    async with httpx.AsyncClient() as client:
        try:
            await client.get(f"{FOLLOWER_1_URL}/health")
        except httpx.ConnectError:
            print("  ✗ Error: Cannot connect to Follower 1 (check ports).")
            return False

        # 1. Send Version 2 (Latest) FIRST
        await client.post(
            f"{FOLLOWER_1_URL}/replicate",
            json={"key": key, "value": val_latest, "version": 2}
        )

        # 2. Send Version 1 (Stale) LATER
        await client.post(
            f"{FOLLOWER_1_URL}/replicate",
            json={"key": key, "value": val_stale, "version": 1}
        )

        # 3. Check Final State
        resp = await client.get(f"{FOLLOWER_1_URL}/read/{key}")
        data = resp.json()

        if data["value"] == val_latest and data["version"] == 2:
            print("  ✓ Follower maintained correct state (version 2) despite out-of-order delivery.")
            return True
        else:
            print(f"  ✗ Failed: Follower has value '{data['value']}' (version {data['version']})")
            return False


async def main():
    print("Running Integration Tests...")

    results = [
        await test_leader_versioning(),
        await test_replication_ordering()
    ]

    print("\n" + "=" * 30)
    if all(results):
        print("✓ All integration tests passed.")
        sys.exit(0)
    else:
        print("✗ Some tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())