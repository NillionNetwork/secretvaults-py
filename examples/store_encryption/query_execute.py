"""Query Execute example using the SecretVault wrapper"""

import asyncio
import json
import sys

from secretvaults import SecretVaultWrapper, OperationType
from org_config import org_config

# Update QUERY ID with your own value
QUERY_ID = "8bc937fd-aca8-485b-89a9-886e507ce64b"

# Define payload variables. In this example we are not using any.
variables = {}


async def main():
    """
    Main function to initialize the SecretVaultWrapper and execute a query.
    """
    try:
        # Initialize the SecretVaultWrapper instance with the org configuration
        org = SecretVaultWrapper(
            org_config["nodes"],
            org_config["org_credentials"],
            operation=OperationType.STORE,
        )
        await org.init()

        # Define the query payload
        query_payload = {
            "id": QUERY_ID,
            "variables": variables or {},
        }

        # Execute the query
        query_result = await org.query_execute_on_nodes(query_payload)
        print("📚 Query Result:", json.dumps(query_result, indent=2))

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
