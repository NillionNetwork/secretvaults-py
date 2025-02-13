"""Generating API tokens example using the SecretVault wrapper"""
import asyncio
import json
import sys

from secretvault import SecretVaultWrapper
from org_config import org_config


async def main():
    """
    Main function to print the org config, initialize the SecretVaultWrapper,
    and generate API tokens for all nodes.
    """
    try:
        # Initialize the SecretVaultWrapper instance with the org configuration
        org = SecretVaultWrapper(org_config["nodes"], org_config["org_credentials"])
        await org.init()

        # Generate API tokens for all nodes
        api_tokens = await org.generate_tokens_for_all_nodes()
        print("🪙 API Tokens:", json.dumps(api_tokens, indent=2))

    except RuntimeError as error:
        print(f"❌ Failed to use SecretVaultWrapper: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
