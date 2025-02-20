"""Data Create and Read example using the SecretVault wrapper"""

import asyncio
import json
import random
import sys

from secretvaults import SecretVaultWrapper, OperationType
from org_config import org_config

# Update schema ID with your own value
SCHEMA_ID = "51dba4eb-b5e7-4c54-9059-867ff592d1ae"


# %allot signals that the value will be encrypted to one %share per node before writing to the collection
def generate_web3_experience_survey_data(num_entries=10):
    data = []

    for _ in range(num_entries):
        entry = {
            "years_in_web3": {"%allot": random.randint(1, 10)},
            "responses": [
                {
                    "rating": random.randint(1, 5),
                    "question_number": random.randint(1, 10),
                }
                for _ in range(random.randint(3, 10))
            ],
        }
        data.append(entry)

    return data


# Generate some entries
web3_experience_survey_data = generate_web3_experience_survey_data(10)


async def main():
    """
    Main function to demonstrate writing to and reading from nodes using the SecretVaultWrapper.
    """
    try:
        # Initialize the SecretVaultWrapper instance with the org configuration and schema ID
        collection = SecretVaultWrapper(
            org_config["nodes"],
            org_config["org_credentials"],
            SCHEMA_ID,
            operation=OperationType.STORE.value,
        )
        await collection.init()

        # Write data to nodes
        data_written = await collection.write_to_nodes(web3_experience_survey_data)

        # Extract unique created IDs from the results
        new_ids = list(
            {
                created_id
                for item in data_written
                if item.get("result")
                for created_id in item["result"]["data"]["created"]
            }
        )
        print("🔏 Created IDs:")
        print("\n".join(new_ids))

        # Read data from nodes
        data_read = await collection.read_from_nodes()
        print("📚 Read new records:", (json.dumps(data_read[: len(web3_experience_survey_data)], indent=2)))

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
