"""File Create and Read example using the SecretVault wrapper"""

import asyncio
import sys

from secretvaults import SecretVaultWrapper, OperationType
from org_config import org_config

# Update schema ID with your own value
SCHEMA_ID = "c0f48b9f-8702-4364-996c-f53440a2b34f"
FILENAME = "cubes.png"
NEW_FILENAME = "cubes_decoded.png"


async def main():
    """
    Main function to demonstrate encrypting files (JPG, PDF, etc), then storing and retrieving from SecretVault.
    """
    try:
        # Initialize the SecretVaultWrapper instance with the org configuration and schema ID
        collection = SecretVaultWrapper(
            org_config["nodes"],
            org_config["org_credentials"],
            SCHEMA_ID,
            operation=OperationType.STORE,
        )
        await collection.init()

        # Split the file into chunks and setup the appropriate number of records
        file_chunks: list = collection.allot_into_chunks(collection.encode_file_to_str(FILENAME))
        data = [
            {
                "file": chunk,
                "file_name": FILENAME,
                "file_sequence": idx,
            }
            for idx, chunk in enumerate(file_chunks)
        ]

        # Write data to nodes
        data_written = await collection.write_to_nodes(data)

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
        data_read = await collection.read_from_nodes(data_filter={"file_name": FILENAME})
        print("📚 Read new records, decode and store file...")

        # Sort by file_sequence to ensure correct order
        data_read_sorted = sorted(data_read, key=lambda x: x["file_sequence"])

        # Merge file pieces together
        decoded_file_chunks = "".join(chunk for record in data_read_sorted for chunk in record["file"])

        if collection.decode_file_from_str(decoded_file_chunks, NEW_FILENAME):
            print("📚 Stored on : ", NEW_FILENAME)

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
