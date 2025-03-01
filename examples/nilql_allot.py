"""nilQL encryption example with allot using the nilQL wrapper"""

import asyncio
import json
import sys

from secretvaults import NilQLWrapper


async def main():
    """
    This is a standalone example of using NilQLWrapper to encrypt and decrypt data.
    It is useful for testing and understanding the basic functionality of NilQLWrapper.
    """

    # Example data to encrypt
    data = {
        "name": {"%allot": "Steph"},
        "years_in_web3": {"%allot": 5},
        "test_nested": {
            "test_nested_2": {
                "test_nested_3": {"%allot": "nested 3 levels down"},
                "test_nested_4": None,
            },
        },
        "test_list": [
            {"%allot": "list item 1"},
            {"%allot": "list item 2"},
        ],
        "rating_of_product": 8,
        "test_null": None,
    }

    # The cluster config just needs an array of nodes for NilQLWrapper
    # - When using NilQLWrapper alone: nodes can be empty dictionaries or contain any fields
    # - When using with SecretVaultWrapper: nodes must contain URL and DID fields
    cluster = {"nodes": [{}, {}, {}]}

    try:
        # Initialize wrapper with cluster config
        print("🔄 Initializing NilQLWrapper...")
        encryption_wrapper = NilQLWrapper(cluster)
        print("✅ NilQLWrapper initialized successfully")

        # Prepare and allot the data
        print("\n📚 Preparing and allotting data...")
        allotted = await encryption_wrapper.prepare_and_allot(data)
        print(f"📚 Allot: {json.dumps(allotted, indent=2)}")

        # Unify the allotted data
        print("\n🔍 Unifying allotted data...")
        unified = await encryption_wrapper.unify(allotted)
        print(f"📚 Unify: {json.dumps(unified, indent=2)}")

    except RuntimeError as error:
        print(f"❌ Error: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
