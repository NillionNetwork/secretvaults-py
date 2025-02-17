"""MongoDB Benchmark: Write, Read, and Aggregate Web3 Survey Data"""
import asyncio
import random
import sys
import time
import numpy as np
from pymongo import MongoClient

# MongoDB Connection
MONGO_URI = ""
DB_NAME = ""
COLLECTION_NAME = "167cabf7-2003-4445-9531-9bd3f152042c"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]


def generate_web3_experience_survey_data(num_entries=1000):
    """Generate synthetic survey data."""
    data = []
    for _ in range(num_entries):
        entry = {
            "years_in_web3": random.randint(1, 10),
            "responses": [
                {
                    "rating": random.randint(1, 5),
                    "question_number": random.randint(1, 10)
                }
                for _ in range(random.randint(3, 10))
            ]
        }
        data.append(entry)
    return data


async def benchmark():
    """Runs the benchmark: Write, Read, and Aggregate operations."""
    write_times, read_times, sum_times = [], [], []

    for _ in range(100):
        try:
            # Step 1: Flush Collection
            collection.delete_many({})

            # Step 2: Measure Write Performance
            data_sample = generate_web3_experience_survey_data(1000)
            start_write = time.time()
            collection.insert_many(data_sample)
            end_write = time.time()
            write_times.append(end_write - start_write)

            # Step 3: Measure Read Performance
            start_read = time.time()
            list(collection.find({}, {"_id": 0}))
            end_read = time.time()
            read_times.append(end_read - start_read)

            # Step 4: Measure Aggregation Performance (Sum and Average)
            start_sum = time.time()
            aggregation_result = collection.aggregate([
                {"$group": {"_id": None, "sum_years_in_web3": {"$sum": "$years_in_web3"}, "user_count": {"$sum": 1}}},
                {"$project": {"_id": 0, "avg_years_in_web3": {"$divide": ["$sum_years_in_web3", "$user_count"]}}}
            ])
            result = list(aggregation_result)[0]
            avg_years_in_web3 = result["avg_years_in_web3"]
            end_sum = time.time()
            sum_times.append(end_sum - start_sum)

        except Exception as error:
            print(f"❌ Error during MongoDB benchmark: {str(error)}")
            sys.exit(1)

    # Print Benchmark Results
    print(f"📝 Write to MongoDB - Avg: {np.mean(write_times):.4f}s, P99: {np.percentile(write_times, 99):.4f}s, P90: {np.percentile(write_times, 90):.4f}s")
    print(f"📚 Read from MongoDB - Avg: {np.mean(read_times):.4f}s, P99: {np.percentile(read_times, 99):.4f}s, P90: {np.percentile(read_times, 90):.4f}s")
    print(f"🗳️ Aggregation in MongoDB - Avg: {np.mean(sum_times):.4f}s, P99: {np.percentile(sum_times, 99):.4f}s, P90: {np.percentile(sum_times, 90):.4f}s")


if __name__ == "__main__":
    asyncio.run(benchmark())
