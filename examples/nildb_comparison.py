"""nilDB Benchmark: Write, Read, and Aggregate Web3 Survey Data"""
import asyncio
import random
import sys
import time
import numpy

from secretvaults import SecretVaultWrapper, OperationType
from org_config import org_config

# Update schema ID with your own value
SCHEMA_ID = "167cabf7-2003-4445-9531-9bd3f152042c"
QUERY_ID = "46d7f6f4-52df-4778-8a8c-ac51bf5a1826"

# $allot signals that the value will be encrypted to one $share per node before writing to the collection
def generate_web3_experience_survey_data(num_entries=3):
    data = []

    for _ in range(num_entries):
        entry = {
            "years_in_web3": {"$allot": random.randint(1, 10)},
            "responses": [
                {
                    "rating": random.randint(1, 5),
                    "question_number": random.randint(1, 10)
                } for _ in range(random.randint(3, 10))
            ]
        }
        data.append(entry)

    return data


# Generate 1000 entries
data_sample = generate_web3_experience_survey_data(1000)


async def main():
    write_times = []
    read_times = []
    sum_times = []

    for _ in range(100):
        try:
            collection = SecretVaultWrapper(
                org_config["nodes"],
                org_config["org_credentials"],
                SCHEMA_ID,
                operation=OperationType.SUM.value,
            )
            await collection.init()

            # Nuke the data
            await collection.flush_data()

            # Measure time for writing data
            start_write = time.time()
            await collection.write_to_nodes(data_sample)
            end_write = time.time()
            write_times.append(end_write - start_write)

            # Measure time for reading data
            start_read = time.time()
            await collection.read_from_nodes()
            end_read = time.time()
            read_times.append(end_read - start_read)

            # Measure time for aggregation on encrypted data (sum + avg)
            start_sum = time.time()
            result = await collection.query_execute_on_nodes({
                "id": QUERY_ID,
                "variables": {},
            })
            avg_years_in_web3 = result["sum_years_in_web3"]/result["user_count"]
            end_sum = time.time()
            sum_times.append(end_sum - start_sum)

        except RuntimeError as error:
            print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
            sys.exit(1)

    print(
        f"📝 Write to nodes - Avg: {numpy.mean(write_times):.4f}s, P99: {numpy.percentile(write_times, 99):.4f}s, P90: {numpy.percentile(write_times, 90):.4f}s")
    print(
        f"📚 Read from nodes - Avg: {numpy.mean(read_times):.4f}s, P99: {numpy.percentile(read_times, 99):.4f}s, P90: {numpy.percentile(read_times, 90):.4f}s")
    print(
        f"🗳️ Aggregation on nodes - Avg: {numpy.mean(sum_times):.4f}s, P99: {numpy.percentile(sum_times, 99):.4f}s, P90: {numpy.percentile(sum_times, 90):.4f}s")


if __name__ == "__main__":
    asyncio.run(main())
