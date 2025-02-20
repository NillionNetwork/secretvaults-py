"""Schema Create example using the SecretVault wrapper"""

import asyncio
import json
import sys

from secretvaults import SecretVaultWrapper
from org_config import org_config

# Load the schema from schema_match.json
with open("schema_store.json", "r", encoding="utf8") as schema_file:
    schema = json.load(schema_file)


async def main():
    """
    Main function to initialize the SecretVaultWrapper and create a new schema.
    """
    try:
        # Initialize the SecretVaultWrapper instance with the org configuration
        org = SecretVaultWrapper(org_config["nodes"], org_config["org_credentials"])
        await org.init()

        # Create a new schema
        new_schema = await org.create_schema(schema, "Web3 Experience Survey")
        print("📚 New Schema:", new_schema)

        # Optional: Delete the schema
        # await org.delete_schema(new_schema)

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
