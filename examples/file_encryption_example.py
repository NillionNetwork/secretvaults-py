"""
File Encryption Demo for SecretVaults

This example demonstrates:
1. Creating a collection for file storage
2. Encrypting and splitting a file into chunks
3. Storing file chunks in SecretVaults
4. Retrieving and reassembling the file
"""

import base64
import traceback
import asyncio
import json
import os
import uuid
from typing import List, Dict, Any
from dotenv import load_dotenv

from secretvaults.dto.builders import RegisterBuilderRequest
from secretvaults.dto.common import Name

from secretvaults.builder import SecretVaultBuilderClient
from secretvaults.common.blindfold import BlindfoldFactoryConfig, BlindfoldOperation
from secretvaults.common.keypair import Keypair
from secretvaults.dto.collections import CreateCollectionRequest
from secretvaults.dto.data import CreateStandardDataRequest, FindDataRequest

# Load .env file
load_dotenv()

# Configuration
SCHEMA_ID = str(uuid.uuid4())  # Generate a new UUID for each run
FILENAME = "examples/data/cubes.png"
NEW_FILENAME = "examples/data/cubes_decoded.png"


# Load collection schema from JSON file
def load_collection_schema():
    """Load collection schema from examples/data/collection_file.json"""
    schema_file = "examples/data/collection_file.json"
    try:
        with open(schema_file, "r", encoding="utf-8") as f:
            schema_data = json.load(f)
        return schema_data
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"❌ Failed to load schema from {schema_file}: {e}")
        raise


# Collection configuration for file storage
COLLECTION_CONFIG = {"id": SCHEMA_ID, "type": "standard", "name": "file_storage", "schema": load_collection_schema()}


def encode_file_to_str(filename: str) -> str:
    """Encode a file to base64 string."""
    with open(filename, "rb") as file:
        file_data = file.read()
        return base64.b64encode(file_data).decode("utf-8")


def decode_file_from_str(encoded_data: str, output_filename: str) -> bool:
    """Decode a base64 string back to a file."""
    try:
        file_data = base64.b64decode(encoded_data)
        with open(output_filename, "wb") as file:
            file.write(file_data)
        return True
    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"❌ Failed to decode file: {e}")
        return False


def allot_into_chunks(data: str, chunk_size: int = 1000) -> List[str]:
    """Split data into chunks of specified size."""
    return [data[i : i + chunk_size] for i in range(0, len(data), chunk_size)]


async def create_file_collection(builder_client: SecretVaultBuilderClient) -> str:
    """Create a collection for file storage."""
    print("📁 Creating file storage collection...")

    # Create collection request
    collection_request = CreateCollectionRequest(
        id=COLLECTION_CONFIG["id"],
        type=COLLECTION_CONFIG["type"],
        name=COLLECTION_CONFIG["name"],
        schema=COLLECTION_CONFIG["schema"],
    )

    try:
        await builder_client.create_collection(collection_request)
        print(f"✅ Collection '{COLLECTION_CONFIG['name']}' created successfully!")
        return COLLECTION_CONFIG["id"]
    except Exception as e:
        print(f"❌ Failed to create collection: {e}")
        raise


async def write_file_chunks(
    builder_client: SecretVaultBuilderClient, collection_id: str, file_chunks: List[str], filename: str
) -> List[str]:
    """Write file chunks to SecretVaults."""
    print("📝 Writing file chunks to SecretVaults...")

    # Prepare data records - file field should be array of objects with %share
    data = [
        {
            "_id": str(uuid.uuid4()),
            "file": [{"%allot": chunk}],
            "file_name": os.path.basename(filename),
            "file_sequence": idx,
        }
        for idx, chunk in enumerate(file_chunks)
    ]

    # Create data request
    create_request = CreateStandardDataRequest(collection=collection_id, data=data)

    try:
        response = await builder_client.create_standard_data(create_request)

        # Extract created IDs
        created_ids = []
        if response and hasattr(response, "root"):
            for node_response in response.root.values():
                if hasattr(node_response, "data") and hasattr(node_response.data, "created"):
                    created_ids.extend(node_response.data.created)

        print(f"✅ Wrote {len(data)} chunks, created {len(created_ids)} records")
        return created_ids
    except Exception as e:
        print(f"❌ Failed to write file chunks: {e}")
        raise


async def read_file_chunks(
    builder_client: SecretVaultBuilderClient, collection_id: str, filename: str
) -> List[Dict[str, Any]]:
    """Read file chunks from SecretVaults."""
    print("📖 Reading file chunks from SecretVaults...")

    # Find data with filter
    find_request = FindDataRequest(collection=collection_id, filter={"file_name": filename})

    try:
        response = await builder_client.find_data(find_request)

        if response:
            return response
        print("❌ No data found")
        return []
    except Exception as e:
        print(f"❌ Failed to read file chunks: {e}")
        raise


def check_environment():
    """Check if all required environment variables are present"""
    required_vars = ["BUILDER_PRIVATE_KEY", "NILCHAIN_URL", "NILAUTH_URL", "NILDB_NODES"]

    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)

    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\n📝 Please copy .env.example to .env and add your private key:")
        print("   cp .env.example .env")
        print("\nThen edit .env with your configuration values.")
        return False

    return True


async def main():  # pylint: disable=too-many-locals,too-many-branches,too-many-statements
    """Main function to demonstrate file encryption and storage."""
    if not check_environment():
        return

    # Check if file exists
    if not os.path.exists(FILENAME):
        print(f"❌ File '{FILENAME}' not found. Please ensure the file exists in the current directory.")
        return

    try:
        # Load configuration from environment
        config = {
            "BUILDER_PRIVATE_KEY": os.getenv("BUILDER_PRIVATE_KEY"),
            "NILCHAIN_URL": os.getenv("NILCHAIN_URL"),
            "NILAUTH_URL": os.getenv("NILAUTH_URL"),
            "NILDB_NODES": os.getenv("NILDB_NODES", "").split(","),
        }

        # Create keypair from private key
        keypair = Keypair.from_hex(config["BUILDER_PRIVATE_KEY"])

        # Prepare URLs for the builder client
        urls = {"chain": [config["NILCHAIN_URL"]], "auth": config["NILAUTH_URL"], "dbs": config["NILDB_NODES"]}

        # Create SecretVaultBuilderClient
        async with await SecretVaultBuilderClient.from_options(
            keypair=keypair,
            urls=urls,
            blindfold=BlindfoldFactoryConfig(operation=BlindfoldOperation.STORE, use_cluster_key=True),
        ) as builder_client:

            # Register builder before any operations
            print("\n=== Registering builder ===")
            try:
                register_request = RegisterBuilderRequest(
                    did=keypair.to_did_string(), name=Name("file-encryption-example-builder")
                )
                register_response = await builder_client.register(register_request)
                if hasattr(register_response, "root"):
                    has_errors = False
                    for node_id, response in register_response.root.items():  # pylint: disable=unused-variable
                        if hasattr(response, "status") and response.status != 201:
                            has_errors = True
                            break
                    if has_errors:
                        print("ℹ️  Builder appears to already be registered.")
                        print("   This is normal if the builder was previously registered.")
                    else:
                        print("✅ Builder registered successfully!")
                else:
                    print("✅ Builder registered successfully!")
            except Exception as e:  # pylint: disable=broad-exception-caught
                print(f"ℹ️  Builder appears to already be registered: {e}")

            # Get root token
            await builder_client.refresh_root_token()

            print("🔏 SecretVaults File Encryption Demo")
            print("=" * 50)

            # Step 1: Create collection
            collection_id = await create_file_collection(builder_client)

            # Step 2: Encode and split file
            print(f"\n📄 Encoding file '{FILENAME}'...")
            encoded_file = encode_file_to_str(FILENAME)
            file_chunks = allot_into_chunks(encoded_file)
            print(f"✅ File encoded and split into {len(file_chunks)} chunks")

            # Step 3: Write file chunks
            created_ids = await write_file_chunks(builder_client, collection_id, file_chunks, FILENAME)
            print(f"🔏 Created {len(created_ids)} records")

            # Step 4: Read file chunks
            data_read = await read_file_chunks(builder_client, collection_id, os.path.basename(FILENAME))

            if data_read:
                # Step 5: Reassemble file
                print("📚 Reassembling file...")

                # Sort by file_sequence to ensure correct order
                data_read_sorted = sorted(data_read, key=lambda x: x.get("file_sequence", 0))

                # Merge file pieces together
                decoded_file_chunks = "".join(chunk for record in data_read_sorted for chunk in record.get("file", ""))

                # Decode and save file
                if decode_file_from_str(decoded_file_chunks, NEW_FILENAME):
                    print(f"✅ File successfully decoded and saved as '{NEW_FILENAME}'")

                    # Verify file sizes
                    original_size = os.path.getsize(FILENAME)
                    decoded_size = os.path.getsize(NEW_FILENAME)
                    print(f"📊 Original file size: {original_size} bytes")
                    print(f"📊 Decoded file size: {decoded_size} bytes")

                    if original_size == decoded_size:
                        print("✅ File sizes match - encryption/decryption successful!")
                    else:
                        print("❌ File sizes don't match - there may be an issue")
                else:
                    print("❌ Failed to decode file")
            else:
                print("❌ No data retrieved to reassemble file")

    except Exception as e:  # pylint: disable=broad-exception-caught
        print(f"❌ Demo failed: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(main())
