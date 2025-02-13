"""Data Create and Read example using the SecretVault wrapper"""
import asyncio
import json
import sys

from nilsvwrappers import SecretVaultWrapper
from org_config import org_config

# Update schema ID with your own value
SCHEMA_ID = "ac0e37ec-de37-4a96-840c-8277dc472e2d"

# $allot signals that the value will be encrypted to one $share per node before writing to the collection
web3_experience_survey_data = [
    {
        "years_in_web3": {"$allot": 4},
        "responses": [
            {"rating": 5, "question_number": 1},
            {"rating": 3, "question_number": 2},
        ],
    },
    {
        "years_in_web3": {"$allot": 1},
        "responses": [
            {"rating": 2, "question_number": 1},
            {"rating": 4, "question_number": 2},
        ],
    },
]


async def main():
    """
    Main function to demonstrate writing to and reading from nodes using the SecretVaultWrapper.
    """
    try:
        # Initialize the SecretVaultWrapper instance with the org configuration and schema ID
        collection = SecretVaultWrapper(org_config["nodes"], org_config["org_credentials"], SCHEMA_ID)
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
        data_read = await collection.read_from_nodes({})
        print("📚 Read new records:", (json.dumps(data_read[: len(web3_experience_survey_data)], indent=2)))

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
