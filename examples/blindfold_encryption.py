"""blindfold encryption example using the BlindfoldWrapper"""

import asyncio
import sys

from secretvaults import BlindfoldWrapper


async def main():
    """
    This is a standalone example of using BlindfoldWrapper to encrypt and decrypt data.
    It is useful for testing and understanding the basic functionality of BlindfoldWrapper.
    """

    # Example data to encrypt
    secret_data = 4269

    # The cluster config just needs an array of nodes for BlindfoldWrapper
    # - When using BlindfoldWrapper alone: nodes can be empty dictionaries or contain any fields
    # - When using with SecretVaultWrapper: nodes must contain URL and DID fields
    cluster = {"nodes": [{}, {}, {}]}

    try:
        # Initialize wrapper with cluster config
        print("🔄 Initializing BlindfoldWrapper...")
        encryption_wrapper = BlindfoldWrapper(cluster)
        print("✅ BlindfoldWrapper initialized successfully")

        print(f"\n🔢 Original data: {secret_data}")

        # Encrypt data into multiple shares
        print("\n🔒 Encrypting data...")
        shares = await encryption_wrapper.encrypt(secret_data)
        print(f"🛠️ Data encrypted into shares: {shares}")

        # Decrypt shares back into original data
        print("\n🔓 Decrypting shares...")
        decrypted_data = await encryption_wrapper.decrypt(shares)

        # Convert decrypted string back to number if necessary
        decrypted_data_as_number = int(decrypted_data)
        print(f"✅ Decrypted data: {decrypted_data_as_number}")

    except RuntimeError as error:
        print(f"❌ Error: {str(error)}")
        sys.exit(1)


# Run the async main function
if __name__ == "__main__":
    asyncio.run(main())
