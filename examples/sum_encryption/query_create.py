"""Schema Create example using the SecretVault wrapper"""

import asyncio
import json
import sys

from secretvaults import SecretVaultWrapper
from org_config import org_config

# Update schema ID with your own value
SCHEMA_ID = "d412699f-6cda-44fe-84f0-b9498315c9ad"

# Load the query
with open("query_sum_with_vars.json", "r", encoding="utf8") as query_file:
    query = json.load(query_file)


async def main():
    """
    Main function to initialize the SecretVaultWrapper and create a new schema.
    """
    try:
        # Initialize the SecretVaultWrapper instance with the org configuration
        org = SecretVaultWrapper(org_config["nodes"], org_config["org_credentials"])
        await org.init()

        # Create a new schema
        new_query = await org.create_query(
            query,
            SCHEMA_ID,
            "Returns sum of years_in_web3 and count of users that have answered question X",
        )
        print("📚 New Query:", new_query)

        # Optional: Delete the query
        # await org.delete_query(new_query)

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
