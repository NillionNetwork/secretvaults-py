"""
Owned Data SecretVault Example

This script demonstrates the owned data workflow for creating a SecretVault builder,
registering it, creating owned collections, and managing user-owned data with blindfold encryption.
"""

from datetime import datetime
import json
import os
import asyncio
import uuid
from dotenv import load_dotenv
from nuc.builder import NucTokenBuilder
from nuc.token import Command

from secretvaults.builder import SecretVaultBuilderClient
from secretvaults.common.nuc_cmd import NucCmd
from secretvaults.common.utils import into_seconds_from_now
from secretvaults.user import SecretVaultUserClient
from secretvaults.common.keypair import Keypair
from secretvaults.common.blindfold import BlindfoldFactoryConfig, BlindfoldOperation
from secretvaults.dto.collections import CreateCollectionRequest
from secretvaults.dto.data import CreateOwnedDataRequest
from secretvaults.dto.users import GrantAccessToDataRequest, RevokeAccessToDataRequest, AclDto
from secretvaults.dto.builders import RegisterBuilderRequest
from secretvaults.dto.common import Name

# Load .env file
load_dotenv()


def check_environment():
    """Check if all required environment variables are present"""
    required_vars = [
        "BUILDER_PRIVATE_KEY",
        "NILCHAIN_URL",
        "NILAUTH_URL",
        "NILDB_NODES",
    ]

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


# Hardcoded grantee DID for grant/revoke
GRANTEE_DID = "did:nil:03d2312cb24fb2664a2a770fd72e7d83ffbc5a96d6b2308b1b0801ccbc5d20ecb5"


async def main():  # pylint: disable=too-many-locals,too-many-statements
    """Owned data SecretVault builder workflow example"""
    if not check_environment():
        return
    # Load config
    config = {
        "BUILDER_PRIVATE_KEY": os.getenv("BUILDER_PRIVATE_KEY"),
        "NILCHAIN_URL": os.getenv("NILCHAIN_URL"),
        "NILAUTH_URL": os.getenv("NILAUTH_URL"),
        "NILDB_NODES": os.getenv("NILDB_NODES", "").split(","),
    }

    # Step 1: Register builder
    print("\n=== Step 1: Register Builder ===")
    builder_keypair = Keypair.from_hex(config["BUILDER_PRIVATE_KEY"])
    urls = {
        "chain": [config["NILCHAIN_URL"]],
        "auth": config["NILAUTH_URL"],
        "dbs": config["NILDB_NODES"],
    }

    with open("examples/data/collection.json", "r", encoding="utf-8") as f:  # Load the standard collection schema
        schema_data = json.load(f)

    async with await SecretVaultBuilderClient.from_options(
        keypair=builder_keypair,
        urls=urls,
        blindfold=BlindfoldFactoryConfig(
            operation=BlindfoldOperation.STORE,
            use_cluster_key=True,
        ),
    ) as builder_client:
        await builder_client.refresh_root_token()
        try:
            register_request = RegisterBuilderRequest(
                did=builder_client.keypair.to_did_string(), name=Name("owned-data-example-builder")
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

        # Step 2: Create owned collection as builder
        print("\n=== Step 2: Create Owned Collection ===")
        collection_id = str(uuid.uuid4())
        collection_request = CreateCollectionRequest(
            id=collection_id,
            type="owned",
            name="owned_collection_demo",
            schema=schema_data["schema"],
        )
        await builder_client.create_collection(collection_request)
        print(f"✅ Owned collection created: {collection_id}")

        # Step 2: Setup user
        print("\n=== Step 2: Setup User ===")
        user_keypair = Keypair.generate()
        user_client = await SecretVaultUserClient.from_options(
            keypair=user_keypair,
            base_urls=config["NILDB_NODES"],
            blindfold=BlindfoldFactoryConfig(
                operation=BlindfoldOperation.STORE,
                use_cluster_key=True,
            ),
        )
        try:
            print(f"✅ User DID: {user_keypair.to_did_string()}")

            # Step 3: Create owned data in the owned collection as the user
            print("\n=== Step 3: Create Owned Data ===")
            # Get builder's root token
            await builder_client.refresh_root_token()
            root_token_envelope = builder_client.root_token

            # Mint a delegation token from builder to user for this collection
            delegation_token = (
                NucTokenBuilder.extending(root_token_envelope)
                .command(Command(NucCmd.NIL_DB_DATA_CREATE.value.split(".")))
                .audience(user_client.id)
                .expires_at(datetime.fromtimestamp(into_seconds_from_now(60)))
                .build(builder_client.keypair.private_key())
            )

            # Create user data
            user_data = {"name": "User owned Item 1", "country_code": {"%allot": "US"}}

            print("\n📝 Creating data entry")

            # Upload data using delegation token
            print("\n📤 Uploading data with delegation token...")

            # Create the data with ACL included
            create_data_request = CreateOwnedDataRequest(
                collection=collection_id,
                owner=user_client.id,
                data=[user_data],
                acl=AclDto(
                    grantee=builder_client.keypair.to_did_string(),
                    read=True,
                    write=False,
                    execute=True,
                ),
            )
            create_response = await user_client.create_data(
                delegation=delegation_token,
                body=create_data_request,
            )
            # Get the document ID
            doc_id = None
            for node_result in create_response.values():
                if hasattr(node_result, "data") and hasattr(node_result.data, "created") and node_result.data.created:
                    doc_id = node_result.data.created[0]
                    break
            if not doc_id:
                print("❌ Failed to create owned data.")
                return
            print(f"✅ Owned data created: {doc_id}")

            # Step 4: Grant access to the data to the hardcoded grantee DID
            print("\n=== Step 4: Grant Access ===")
            acl = AclDto(grantee=GRANTEE_DID, read=True, write=True, execute=True)
            grant_request = GrantAccessToDataRequest(
                collection=collection_id,
                document=doc_id,
                acl=acl,
            )
            await user_client.grant_access(grant_request)
            print(f"✅ Granted access to {GRANTEE_DID}")

            # Step 5: Revoke access from the same grantee DID
            print("\n=== Step 5: Revoke Access ===")
            revoke_request = RevokeAccessToDataRequest(
                grantee=GRANTEE_DID,
                collection=collection_id,
                document=doc_id,
            )
            await user_client.revoke_access(revoke_request)
            print(f"✅ Revoked access from {GRANTEE_DID}")
        finally:
            await user_client.close()


if __name__ == "__main__":
    asyncio.run(main())
