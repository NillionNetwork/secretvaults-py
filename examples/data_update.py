"""Data Update example using the SecretVault wrapper"""
import asyncio
import json
import sys

from secretvault import SecretVaultWrapper
from org_config import org_config


# Update schema ID and record ID to update with your own values
SCHEMA_ID = "167cabf7-2003-4445-9531-9bd3f152042c"
RECORD_ID = "0f27cd38-4696-434c-a4d2-1a3823efe864"

# Record update with $allot fields
record_update = {
    "years_in_web3": {"$allot": 15},
    "responses": [
        {"rating": 5, "question_number": 1},
        {"rating": 5, "question_number": 2},
    ],
}


async def main():
    """
    Main function to read, update, then read back a record using SecretVaultWrapper.
    """
    try:
        # Initialize the SecretVaultWrapper instance
        collection = SecretVaultWrapper(org_config["nodes"], org_config["org_credentials"], SCHEMA_ID)
        await collection.init()

        # Filter to find the record by ID
        filter_by_id = {"_id": RECORD_ID}

        # Read the original record
        print("\n📚 Reading original record...")
        read_original_record = await collection.read_from_nodes(filter_by_id)
        print(f"📚 Read original record: {json.dumps(read_original_record, indent=2)}")

        if read_original_record:
            # Update the record using the provided recordUpdate data
            print("\n🔄 Updating record in nodes...")
            updated_data = await collection.update_data_to_nodes(record_update, filter_by_id)
            updated_data_summary = [node["result"]["data"] for node in updated_data if node.get("result")]
            print("📚 Updated nodes with new record data:", json.dumps(updated_data_summary, indent=2))

            # Read the updated record to confirm the changes
            print("\n📚 Reading updated record...")
            read_updated_record = await collection.read_from_nodes(filter_by_id)
            print(f"📚 Read updated record: {json.dumps(read_updated_record, indent=2)}")
        else:
            print("❌ Nothing to update")

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
