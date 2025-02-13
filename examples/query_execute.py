"""Query Execute example using the SecretVault wrapper"""
import asyncio
import json
import sys

from secretvault import SecretVaultWrapper, OperationType
from org_config import org_config

# Update QUERY ID with your own value
QUERY_ID = "738906f8-1cb9-4c30-bb9d-54b977a0bc16"

# Define payload variables. In this example we are targeting users who have answered question number X.
variables = {
    "question_number": 1,  # feel free to change this and experiment
}


async def main():
    """
    Main function to initialize the SecretVaultWrapper and execute a query.
    """
    try:
        # Initialize the SecretVaultWrapper instance with the org configuration
        org = SecretVaultWrapper(
            org_config["nodes"],
            org_config["org_credentials"],
            operation=OperationType.SUM.value,  # we'll be doing a sum operation on encrypted values
        )
        await org.init()

        # Define the query payload
        query_payload = {
            "id": QUERY_ID,
            "variables": variables or {},
        }

        # Execute the query
        query_result = await org.query_execute_on_nodes(query_payload)
        # Even though years_in_web3 entries are encrypted, we can get the sum without individually decrypting them
        print("📚 Query Result:", json.dumps(query_result, indent=2))

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
