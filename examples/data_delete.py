"""Data Delete and Flush examples using the SecretVault wrapper"""
import asyncio
import json
import sys

from nilsvwrappers import SecretVaultWrapper  # Adjust import if needed
from org_config import org_config


# Update schema ID and record ID to delete with your own values
SCHEMA_ID = "167cabf7-2003-4445-9531-9bd3f152042c"
RECORD_ID = "a1eafb86-baa7-4d84-8ef5-ff2eb1ff06f8"


async def main():
    """
    Main function to read, delete, and optionally flush data using SecretVaultWrapper.

    Steps:
    1. Initialize the collection.
    2. Read the original record.
    3. Delete the record using `delete_data_from_nodes`.
    4. Optionally flush all data from the collection.
    """
    try:
        # Initialize the SecretVaultWrapper instance
        collection = SecretVaultWrapper(org_config["nodes"], org_config["org_credentials"], SCHEMA_ID)
        await collection.init()

        # Filter to find the record by ID
        filter_by_id = {"_id": RECORD_ID}

        # Read the original record before deletion
        print("\n📚 Reading original record...")
        read_original_record = await collection.read_from_nodes(filter_by_id)
        print(f"📚 Read original record: {json.dumps(read_original_record, indent=2)}")

        if read_original_record:
            # Delete the record using the provided filter
            print("\n🗑️ Deleting record from nodes...")
            deleted_data = await collection.delete_data_from_nodes(filter_by_id)
            print(f"📚 Deleted record from all nodes: {json.dumps(deleted_data, indent=2)}")
        else:
            print("❌ Nothing to delete")

        # Optional: Flush all data from the collection
        # await collection.flush_data()

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
