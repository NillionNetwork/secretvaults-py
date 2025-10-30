"""
Interactive SecretVault Demo

This script provides an interactive CLI for demonstrating the full capabilities of the SecretVaults Python client,
including builder and user flows, collection and data management, blindfold encryption, and access control.
"""

# Standard library imports
import os
import asyncio
import json
import uuid
import traceback
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

# Third-party imports
from dotenv import load_dotenv
from nuc.builder import NucTokenBuilder
from nuc.token import Command

# First-party imports
from secretvaults.common.keypair import Keypair
from secretvaults.builder import SecretVaultBuilderClient
from secretvaults.dto.users import (
    AclDto,
    UpdateUserDataRequest,
    ReadDataRequestParams,
    DeleteDocumentRequestParams,
    GrantAccessToDataRequest,
    RevokeAccessToDataRequest,
)
from secretvaults.user import SecretVaultUserClient
from secretvaults.common.nuc_cmd import NucCmd
from secretvaults.dto.builders import RegisterBuilderRequest
from secretvaults.dto.collections import CreateCollectionRequest, CreateCollectionIndexRequest
from secretvaults.dto.data import (
    CreateStandardDataRequest,
    CreateOwnedDataRequest,
    FindDataRequest,
    UpdateDataRequest,
    DeleteDataRequest,
)
from secretvaults.dto.common import Name
from secretvaults.common.blindfold import BlindfoldFactoryConfig, BlindfoldOperation, reveal
from secretvaults.dto.queries import CreateQueryRequest, RunQueryRequest
from secretvaults.common.utils import into_seconds_from_now

# Load .env file
load_dotenv()


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


config = {
    "NILCHAIN_URL": os.getenv("NILCHAIN_URL"),
    "NILAUTH_URL": os.getenv("NILAUTH_URL"),
    "NILDB_NODES": os.getenv("NILDB_NODES", "").split(","),
    "BUILDER_PRIVATE_KEY": os.getenv("BUILDER_PRIVATE_KEY"),
}


async def read_cluster_info(builder_client: SecretVaultBuilderClient) -> None:
    """Read cluster information"""
    print("\n=== Reading Cluster Info ===")
    try:
        cluster_info = await builder_client.read_cluster_info()

        if not cluster_info:
            print("No cluster info available.")
            return

        print(f"\nCluster Status: {len(cluster_info)} nodes found")
        print("=" * 60)

        for node_id, node_info in cluster_info.items():
            print(f"\n🔗 Node: {node_id}")
            print(f"   URL: {node_info.url}")
            print(f"   Public Key: {node_info.public_key}")
            print(f"   Version: {node_info.build.version}")
            print(f"   Commit: {node_info.build.commit[:8]}...")
            print(f"   Started: {node_info.started.strftime('%Y-%m-%d %H:%M:%S UTC')}")
            print(f"   Build Time: {node_info.build.time.strftime('%Y-%m-%d %H:%M:%S UTC')}")

            if node_info.maintenance.active:
                print(
                    f"   ⚠️Maintenance: ON (since {node_info.maintenance.started_at.strftime('%Y-%m-%d %H:%M:%S UTC')})"
                )
            else:
                print("   ✅ Maintenance: Inactive")
            print("-" * 40)

        print(f"\n✅ Cluster is operational with {len(cluster_info)} nodes")

    except Exception as e:
        print(f"Failed to read cluster info: {e}")


async def check_subscription_status(builder_client: SecretVaultBuilderClient) -> Optional[Dict[str, Any]]:
    """Check subscription status"""
    print("\n=== Checking Subscription Status ===")

    try:
        # Get subscription status
        subscription_status = await builder_client.subscription_status()

        if not subscription_status:
            print("No subscription information available.")
            return None

        print("\n📋 Subscription Details")
        print("=" * 40)

        # Main subscription status
        if subscription_status.subscribed:
            print("✅ Status: ACTIVE")
        else:
            print("❌ Status: INACTIVE")

        # Subscription details
        if hasattr(subscription_status, "details") and subscription_status.details:
            details = subscription_status.details

            if hasattr(details, "expires_at") and details.expires_at:
                expires_date = details.expires_at.strftime("%Y-%m-%d %H:%M:%S UTC")
                print(f"📅 Expires: {expires_date}")

                # Calculate days remaining
                now = datetime.now(timezone.utc)
                days_remaining = (details.expires_at - now).days

                if days_remaining > 30:
                    print(f"⏰ Days Remaining: {days_remaining} days")
                elif days_remaining > 7:
                    print(f"⚠️  Days Remaining: {days_remaining} days")
                elif days_remaining > 0:
                    print(f"🚨 Days Remaining: {days_remaining} days (EXPIRING SOON!)")
                else:
                    print(f"❌ EXPIRED {abs(days_remaining)} days ago")

            if hasattr(details, "renewable_at") and details.renewable_at:
                renewable_date = details.renewable_at.strftime("%Y-%m-%d %H:%M:%S UTC")
                print(f"🔄 Renewable: {renewable_date}")

        print("=" * 40)

        return subscription_status

    except Exception as e:
        print(f"Failed to check subscription status: {e}")
        return None


async def get_root_token(builder_client: SecretVaultBuilderClient) -> Optional[str]:
    """Get root token for the builder"""
    print("\n=== Getting Root Token ===")

    # Get root token for the builder
    await builder_client.refresh_root_token()
    root_token = builder_client.root_token
    print("Root token:", root_token)

    return root_token


async def read_profile(builder_client: SecretVaultBuilderClient) -> Optional[Dict[str, Any]]:
    """Read builder profile"""
    print("\n=== Reading Builder Profile ===")
    try:
        profile = await builder_client.read_profile()

        if not profile:
            print("No profile information available.")
            return None

        print("\n👤 Builder Profile")
        print("=" * 50)

        # Check if profile has data attribute (wrapped response)
        if hasattr(profile, "data") and profile.data:
            profile_data = profile.data
        else:
            profile_data = profile

        # Basic profile info
        if hasattr(profile_data, "id"):
            print(f"🆔 ID: {profile_data.id}")

        if hasattr(profile_data, "name"):
            print(f"📝 Name: {profile_data.name}")

        # Timestamps
        if hasattr(profile_data, "created"):
            created_date = profile_data.created.strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"📅 Created: {created_date}")

        if hasattr(profile_data, "updated"):
            updated_date = profile_data.updated.strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"🔄 Updated: {updated_date}")

        # Collections
        if hasattr(profile_data, "collections") and profile_data.collections:
            print(f"\n📚 Collections ({len(profile_data.collections)}):")
            for i, collection_id in enumerate(profile_data.collections, 1):
                print(f"   {i}. {collection_id}")
        else:
            print("\n📚 Collections: None")

        # Queries
        if hasattr(profile_data, "queries") and profile_data.queries:
            print(f"\n🔍 Queries ({len(profile_data.queries)}):")
            for i, query_id in enumerate(profile_data.queries, 1):
                print(f"   {i}. {query_id}")
        else:
            print("\n🔍 Queries: None")

        print("=" * 50)

        return profile

    except Exception as e:
        print(f"Failed to read profile: {e}")
        traceback.print_exc()
        return None


async def register_builder(builder_client: SecretVaultBuilderClient) -> None:
    """Register the builder"""
    print("\n=== Register Builder ===")

    register_request = RegisterBuilderRequest(did=builder_client.keypair.to_did_string(), name=Name("test-builder"))
    print(f"Registration request: {register_request.model_dump()}")

    try:
        register_response = await builder_client.register(register_request)

        # Check if the response contains errors (any non-201 status codes)
        if hasattr(register_response, "root"):
            has_errors = False
            for node_id, response in register_response.root.items():
                if hasattr(response, "status") and response.status != 201:
                    has_errors = True
                    break

            if has_errors:
                print("ℹ️  Builder appears to already be registered.")
                print("   This is normal if the builder was previously registered.")
                print("   You can verify by checking the builder profile.")
            else:
                print("✅ Registration successful!")
                print("Register response:", register_response)
        else:
            print("✅ Registration successful!")
            print("Register response:", register_response)

    except Exception as e:
        print(f"❌ Registration failed: {e}")
        print("💡 User is probably already registered")
        # Try to get more details about the error
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def list_collections(builder_client: SecretVaultBuilderClient) -> Optional[List[Dict[str, Any]]]:
    """List all collections"""
    print("\n=== Listing Collections ===")
    try:
        collections = await builder_client.read_collections()

        if not collections:
            print("No collections found.")
            return None

        # Check if collections is wrapped in a response object
        if hasattr(collections, "data") and collections.data:
            collections_data = collections.data
        else:
            collections_data = collections

        print(f"\n📚 Collections ({len(collections_data)} found)")
        print("=" * 60)

        for i, collection in enumerate(collections_data, 1):
            print(f"\n{i}. Collection Details:")

            if hasattr(collection, "id"):
                print(f"   🆔 ID: {collection.id}")

            if hasattr(collection, "name"):
                print(f"   📝 Name: {collection.name}")

            if hasattr(collection, "type"):
                print(f"   🏷️  Type: {collection.type}")

            # Add more fields if they exist
            if hasattr(collection, "created"):
                created_date = collection.created.strftime("%Y-%m-%d %H:%M:%S UTC")
                print(f"   📅 Created: {created_date}")

            if hasattr(collection, "updated"):
                updated_date = collection.updated.strftime("%Y-%m-%d %H:%M:%S UTC")
                print(f"   🔄 Updated: {updated_date}")

            print("-" * 40)

        print(f"\n✅ Found {len(collections_data)} collection(s)")
        return collections

    except Exception as e:
        print(f"Failed to list collections: {e}")
        return None


async def create_collection(builder_client: SecretVaultBuilderClient, collection_type: str = "standard") -> None:
    """Create a new collection with specified type"""
    type_display = "Standard" if collection_type == "standard" else "Owned"
    print(f"\n=== Create {type_display} Collection ===")

    try:
        # Load the standard collection schema
        with open("examples/data/collection.json", "r", encoding="utf-8") as f:
            schema_data = json.load(f)

        # Get collection name from user
        default_name = f"{collection_type}-collection"
        collection_name = input(f"\nEnter collection name (or press Enter for '{default_name}'): ").strip()
        if not collection_name:
            collection_name = default_name

        print(f"\n🔧 Creating {collection_type} collection: {collection_name}")

        # Create collection request
        collection_request = CreateCollectionRequest(
            id=str(uuid.uuid4()), type=collection_type, name=collection_name, schema=schema_data["schema"]
        )

        collection_response = await builder_client.create_collection(collection_request)
        print(f"✅ {type_display} collection created successfully!")
        print(f"   ID: {collection_request.id}")
        print(f"   Type: {collection_type}")
        print(f"   Name: {collection_name}")

        # List collections to verify
        print("\n📚 Updated collections list:")
        await list_collections(builder_client)

    except Exception as e:
        print(f"❌ Failed to create {collection_type} collection: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def read_collection_metadata(
    builder_client: SecretVaultBuilderClient,
    collection_id: Optional[str] = None,
) -> None:
    """Read a specific collection by ID. If ID is provided, skip prompt."""
    print("\n=== Read Collection ===")

    if not collection_id:
        # First list collections to show available ones
        print("Available collections:")
        collections_response = await builder_client.read_collections()
        if not collections_response:
            print("No collections available to read.")
            return

        # Extract collections data from response
        if hasattr(collections_response, "data") and collections_response.data:
            collections_data = collections_response.data
        else:
            collections_data = collections_response

        if not collections_data:
            print("No collections available to read.")
            return

        # Display collections in a simple format for selection
        print(f"\n📚 Found {len(collections_data)} collection(s):")
        for i, collection in enumerate(collections_data, 1):
            _cid = None
            if hasattr(collection, "id"):
                _cid = collection.id
            elif hasattr(collection, "_id"):
                _cid = collection._id
            elif isinstance(collection, dict):
                _cid = collection.get("id") or collection.get("_id")

            collection_name = "Unknown"
            if hasattr(collection, "name"):
                collection_name = collection.name
            elif isinstance(collection, dict) and "name" in collection:
                collection_name = collection["name"]

            print(f"   {i}. {collection_name} (ID: {_cid})")

        # Get collection ID from user
        print("\nEnter collection ID to read:")
        collection_id = input().strip()

        if not collection_id:
            print("No collection ID provided.")
            return

    try:
        print(f"\n📖 Reading collection: {collection_id}")
        collection_response = await builder_client.read_collection(collection_id)

        if not collection_response:
            print("❌ No collection found with that ID.")
            return

        # Display collection details nicely
        print("\n📋 Collection Metadata:")
        print("=" * 60)

        # Check if response is wrapped in a data attribute
        if hasattr(collection_response, "data") and collection_response.data:
            collection_data = collection_response.data
        else:
            collection_data = collection_response

        # Basic collection info
        if hasattr(collection_data, "id"):
            print(f"🆔 ID: {collection_data.id}")

        # Collection metadata
        if hasattr(collection_data, "count"):
            print(f"📊 Document Count: {collection_data.count}")

        if hasattr(collection_data, "size"):
            print(f"💾 Size: {collection_data.size} bytes")

        # Timestamps
        if hasattr(collection_data, "first_write"):
            first_write_date = collection_data.first_write.strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"📅 First Write: {first_write_date}")

        if hasattr(collection_data, "last_write"):
            last_write_date = collection_data.last_write.strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"🔄 Last Write: {last_write_date}")

        # Indexes
        if hasattr(collection_data, "indexes") and collection_data.indexes:
            print(f"\n📋 Indexes ({len(collection_data.indexes)}):")
            for i, index in enumerate(collection_data.indexes, 1):
                index_name = getattr(index, "name", "Unknown")
                index_version = getattr(index, "v", "Unknown")
                index_key = getattr(index, "key", {})
                index_unique = getattr(index, "unique", False)

                # Format the key dictionary
                key_str = ", ".join([f"{k}: {v}" for k, v in index_key.items()])
                unique_str = "unique" if index_unique else "non-unique"

                print(f"   {i}. {index_name} (v{index_version}, {key_str}, {unique_str})")
        else:
            print(f"\n📋 Indexes: None")

        print("=" * 60)

    except Exception as e:
        print(f"❌ Failed to read collection: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def delete_collections(builder_client: SecretVaultBuilderClient) -> None:
    """Delete collections by ID"""
    print("\n=== Delete Collections ===")

    # First list collections to show available ones
    print("Available collections:")
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available to delete.")
        return

    # Extract collections data from response
    if hasattr(collections_response, "data") and collections_response.data:
        collections_data = collections_response.data
    else:
        collections_data = collections_response

    if not collections_data:
        print("No collections available to delete.")
        return

    # Display collections in a simple format for deletion
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        collection_id = None
        if hasattr(collection, "id"):
            collection_id = collection.id
        elif hasattr(collection, "_id"):
            collection_id = collection._id
        elif isinstance(collection, dict):
            collection_id = collection.get("id") or collection.get("_id")

        collection_name = "Unknown"
        if hasattr(collection, "name"):
            collection_name = collection.name
        elif isinstance(collection, dict) and "name" in collection:
            collection_name = collection["name"]

        print(f"   {i}. {collection_name} (ID: {collection_id})")

    # Get collection IDs to delete from user
    print("\nEnter collection IDs to delete (comma-separated, or 'all' for all):")
    user_input = input().strip()

    if user_input.lower() == "all":
        collection_ids_to_delete = []
        for coll in collections_data:
            # Handle different collection data structures
            if hasattr(coll, "id"):
                collection_ids_to_delete.append(coll.id)
            elif hasattr(coll, "_id"):
                collection_ids_to_delete.append(coll._id)
            elif isinstance(coll, dict) and ("id" in coll or "_id" in coll):
                collection_ids_to_delete.append(coll.get("id") or coll.get("_id"))
    else:
        collection_ids_to_delete = [id.strip() for id in user_input.split(",") if id.strip()]

    if not collection_ids_to_delete:
        print("No collection IDs provided for deletion.")
        return

    print(f"\nDeleting {len(collection_ids_to_delete)} collections...")
    for collection_id in collection_ids_to_delete:
        try:
            print(f"Deleting collection: {collection_id}")
            delete_response = await builder_client.delete_collection(collection_id)
            print(f"Delete response for {collection_id}:", delete_response)
            if delete_response:
                print(f"Successfully deleted collection {collection_id} from {len(delete_response)} nodes")
            else:
                print(f"Failed to delete collection {collection_id}")
        except Exception as e:
            print(f"Failed to delete collection {collection_id}: {e}")
            if hasattr(e, "status"):
                print(f"HTTP Status: {e.status}")
            if hasattr(e, "headers"):
                print(f"Response Headers: {e.headers}")

    # List collections again to verify deletion
    print("\nListing collections after deletion:")
    await list_collections(builder_client)


async def create_standard_data(builder_client: SecretVaultBuilderClient) -> None:
    """Create standard data for a collection"""
    print("\n=== Create Standard Data ===")

    # First show available collections
    print("Available collections:")
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available.")
        return

    # Extract collections data from response
    if hasattr(collections_response, "data") and collections_response.data:
        collections_data = collections_response.data
    else:
        collections_data = collections_response

    if not collections_data:
        print("No collections available.")
        return

    # Display collections
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        collection_id = None
        if hasattr(collection, "id"):
            collection_id = collection.id
        elif hasattr(collection, "_id"):
            collection_id = collection._id
        elif isinstance(collection, dict):
            collection_id = collection.get("id") or collection.get("_id")

        collection_name = "Unknown"
        if hasattr(collection, "name"):
            collection_name = collection.name
        elif isinstance(collection, dict) and "name" in collection:
            collection_name = collection["name"]

        print(f"   {i}. {collection_name} (ID: {collection_id})")

    # Get collection ID from user
    print("\nEnter collection ID to create data for:")
    collection_id = input().strip()

    if not collection_id:
        print("No collection ID provided.")
        return

    try:
        # Sample data that matches the standard schema
        sample_data = [
            {"_id": str(uuid.uuid4()), "name": "Sample Item 1", "country_code": {"%allot": "US"}},
            {"_id": str(uuid.uuid4()), "name": "Sample Item 2", "country_code": {"%allot": "GB"}},
            {"_id": str(uuid.uuid4()), "name": "Sample Item 3", "country_code": {"%allot": "AU"}},
        ]

        create_data_request = CreateStandardDataRequest(collection=collection_id, data=sample_data)

        print(f"Creating {len(sample_data)} data records...")
        data_response = await builder_client.create_standard_data(create_data_request)

        # Display data creation response nicely
        print("\n✅ Data Creation Results:")
        print("=" * 50)

        if hasattr(data_response, "root"):
            total_created = 0
            total_errors = 0

            for node_id, response in data_response.root.items():
                if hasattr(response, "data"):
                    created_count = len(response.data.created) if response.data.created else 0
                    error_count = len(response.data.errors) if response.data.errors else 0
                    total_created += created_count
                    total_errors += error_count

                    print(f"🔗 Node {node_id[:20]}...:")
                    print(f"   ✅ Created: {created_count} records")
                    if error_count > 0:
                        print(f"   ❌ Errors: {error_count} records")

            print(f"\n📊 Summary: {total_created} records created, {total_errors} errors")
        else:
            print("Data creation response:", data_response)

    except Exception as e:
        print(f"Failed to create standard data: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def add_collection_index(builder_client: SecretVaultBuilderClient) -> None:
    """Add an index to a collection"""
    print("\n=== Add Collection Index ===")

    # List collections to help choose
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available.")
        return

    collections_data = collections_response.data if hasattr(collections_response, "data") else collections_response
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        coll_id = getattr(collection, "id", getattr(collection, "_id", None))
        coll_name = getattr(collection, "name", "Unknown")
        print(f"   {i}. {coll_name} (ID: {coll_id})")

    collection_id = input("\nEnter collection ID: ").strip()
    if not collection_id:
        print("No collection ID provided.")
        return

    # Preset index configuration
    index_name = "new_index_1"
    keys_list = [{"name": 1}]
    unique = True
    ttl_value: float = 0

    try:
        req = CreateCollectionIndexRequest(
            collection=collection_id,
            name=index_name,
            keys=keys_list,
            unique=unique,
            ttl=ttl_value,
        )
        result = await builder_client.create_collection_index(collection_id, req)
        # Detect per-node failures (supports ByNodeName RootModel and dict)
        node_errors = None
        try:
            if hasattr(result, "items"):
                node_errors = {k: v for k, v in result.items() if isinstance(v, Exception)}
            elif isinstance(result, dict):
                node_errors = {k: v for k, v in result.items() if isinstance(v, Exception)}
        except Exception:
            node_errors = None
        if node_errors:
            print("❌ Failed to create index on one or more nodes.")
            return
        print("✅ Index created successfully.")

        # Show updated metadata for the same collection without reprompting
        await read_collection_metadata(builder_client, collection_id)
    except Exception as e:
        print(f"❌ Failed to create index: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def drop_collection_index(builder_client: SecretVaultBuilderClient) -> None:
    """Drop an index from a collection"""
    print("\n=== Drop Collection Index ===")

    # List collections and their indexes to help choose
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available.")
        return

    collections_data = collections_response.data if hasattr(collections_response, "data") else collections_response
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        coll_id = getattr(collection, "id", getattr(collection, "_id", None))
        coll_name = getattr(collection, "name", "Unknown")
        print(f"   {i}. {coll_name} (ID: {coll_id})")

    collection_id = input("\nEnter collection ID: ").strip()
    if not collection_id:
        print("No collection ID provided.")
        return

    try:
        metadata_response = await builder_client.read_collection(collection_id)
        metadata = metadata_response.data if hasattr(metadata_response, "data") else metadata_response
        indexes = getattr(metadata, "indexes", [])
        if indexes:
            print(f"\n📋 Indexes ({len(indexes)}):")
            for idx in indexes:
                name = getattr(idx, "name", "")
                key = getattr(idx, "key", {})
                unique = getattr(idx, "unique", False)
                key_str = ", ".join([f"{k}: {v}" for k, v in key.items()])
                print(f"   - {name} ({key_str}) {'[unique]' if unique else ''}")
        else:
            print("\n📋 Indexes: None")
    except Exception:
        # Non-fatal for dropping by name
        pass

    index_name = input("Enter index name to drop: ").strip()
    if not index_name:
        print("Index name is required.")
        return

    try:
        result = await builder_client.drop_collection_index(collection_id, index_name)
        # Detect per-node failures (supports ByNodeName RootModel and dict)
        node_errors = None
        try:
            if hasattr(result, "items"):
                node_errors = {k: v for k, v in result.items() if isinstance(v, Exception)}
            elif isinstance(result, dict):
                node_errors = {k: v for k, v in result.items() if isinstance(v, Exception)}
        except Exception:
            node_errors = None
        if node_errors:
            print("❌ Failed to drop index on one or more nodes.")
            return
        print("✅ Index dropped successfully.")

        # Show updated metadata for the same collection without reprompting
        await read_collection_metadata(builder_client, collection_id)
    except Exception as e:
        print(f"❌ Failed to drop index: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def find_data_all(builder_client: SecretVaultBuilderClient) -> None:
    """Find all data in a collection"""
    print("\n=== Find Data (All) ===")

    # First show available collections
    print("Available collections:")
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available.")
        return

    # Extract collections data from response
    if hasattr(collections_response, "data") and collections_response.data:
        collections_data = collections_response.data
    else:
        collections_data = collections_response

    if not collections_data:
        print("No collections available.")
        return

    # Display collections
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        collection_id = None
        if hasattr(collection, "id"):
            collection_id = collection.id
        elif hasattr(collection, "_id"):
            collection_id = collection._id
        elif isinstance(collection, dict):
            collection_id = collection.get("id") or collection.get("_id")

        collection_name = "Unknown"
        if hasattr(collection, "name"):
            collection_name = collection.name
        elif isinstance(collection, dict) and "name" in collection:
            collection_name = collection["name"]

        print(f"   {i}. {collection_name} (ID: {collection_id})")

    # Get collection ID from user
    print("\nEnter collection ID to find data in:")
    collection_id = input().strip()

    if not collection_id:
        print("No collection ID provided.")
        return

    try:
        # Find all data in the collection
        find_request = FindDataRequest(collection=collection_id, filter={})
        find_response = await builder_client.find_data(find_request)

        # Display find response nicely
        if find_response:
            data_records = find_response
            print(f"\n📋 Found {len(data_records)} data records:")
            print("=" * 60)

            for i, record in enumerate(data_records, 1):
                print(f"\n{i}. Data Record:")

                # Generic display of all key-value pairs
                for key, value in record.items():
                    print(f"   {key}: {value}")

                print("-" * 40)

            print(f"\n✅ Successfully found {len(data_records)} data records")
        else:
            print("Find response:", find_response)

    except Exception as e:
        print(f"Failed to find data: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def find_data_with_filter(builder_client: SecretVaultBuilderClient) -> None:
    """Find data in a collection with a specific filter"""
    print("\n=== Find Data (with filter) ===")

    # First show available collections
    print("Available collections:")
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available.")
        return

    # Extract collections data from response
    if hasattr(collections_response, "data") and collections_response.data:
        collections_data = collections_response.data
    else:
        collections_data = collections_response

    if not collections_data:
        print("No collections available.")
        return

    # Display collections
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        collection_id = None
        if hasattr(collection, "id"):
            collection_id = collection.id
        elif hasattr(collection, "_id"):
            collection_id = collection._id
        elif isinstance(collection, dict):
            collection_id = collection.get("id") or collection.get("_id")

        collection_name = "Unknown"
        if hasattr(collection, "name"):
            collection_name = collection.name
        elif isinstance(collection, dict) and "name" in collection:
            collection_name = collection["name"]

        print(f"   {i}. {collection_name} (ID: {collection_id})")

    # Get collection ID from user
    print("\nEnter collection ID to find data in:")
    collection_id = input().strip()

    if not collection_id:
        print("No collection ID provided.")
        return

    try:
        # Use the specified filter
        filter_data = {"name": "Sample Item 3"}
        print(f"\n🔍 Using filter: {filter_data}")

        # Find data with the filter
        find_request = FindDataRequest(collection=collection_id, filter=filter_data)
        find_response = await builder_client.find_data(find_request)

        # Display find response nicely
        if find_response:
            data_records = find_response
            print(f"\n📋 Found {len(data_records)} data records matching filter:")
            print("=" * 60)

            for i, record in enumerate(data_records, 1):
                print(f"\n{i}. Data Record:")

                # Generic display of all key-value pairs
                for key, value in record.items():
                    print(f"   {key}: {value}")

                print("-" * 40)

            print(f"\n✅ Successfully found {len(data_records)} data records matching filter")
        else:
            print("Find response:", find_response)

    except Exception as e:
        print(f"Failed to find data: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def update_data_with_filter(builder_client: SecretVaultBuilderClient) -> None:
    """Update data in a collection with a specific filter"""
    print("\n=== Update Data (with filter) ===")

    # First show available collections
    print("Available collections:")
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available.")
        return

    # Extract collections data from response
    if hasattr(collections_response, "data") and collections_response.data:
        collections_data = collections_response.data
    else:
        collections_data = collections_response

    if not collections_data:
        print("No collections available.")
        return

    # Display collections
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        collection_id = None
        if hasattr(collection, "id"):
            collection_id = collection.id
        elif hasattr(collection, "_id"):
            collection_id = collection._id
        elif isinstance(collection, dict):
            collection_id = collection.get("id") or collection.get("_id")

        collection_name = "Unknown"
        if hasattr(collection, "name"):
            collection_name = collection.name
        elif isinstance(collection, dict) and "name" in collection:
            collection_name = collection["name"]

        print(f"   {i}. {collection_name} (ID: {collection_id})")

    # Get collection ID from user
    print("\nEnter collection ID to update data in:")
    collection_id = input().strip()

    if not collection_id:
        print("No collection ID provided.")
        return

    try:
        # Use the specified filter and update
        filter_data = {
            "_id": "dd512d0f-31d4-453c-8a07-e84828270984",
        }
        update_data = {
            "$set": {
                "utm": {
                    "utm_source": "chatgpt.com",
                }
            }
        }

        print(f"\n🔍 Using filter to find records: {filter_data}")
        print(f"📝 Update data: {update_data}")
        print("⚠️  This will update all records matching the filter!")

        # Confirm update
        confirm = input("\nAre you sure you want to proceed? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("❌ Update cancelled.")
            return

        update_request = UpdateDataRequest(collection=collection_id, filter=filter_data, update=update_data)

        try:
            update_response = await builder_client.update_data(update_request)
            print("✅ Update completed successfully!")

            # Format the response nicely
            print("\n📊 Update Results:")
            print("=" * 60)

            if hasattr(update_response, "root") and update_response.root:
                for node_id, response in update_response.root.items():
                    print(f"\n🔗 Node: {node_id}")

                    if hasattr(response, "data") and response.data:
                        data = response.data
                        if hasattr(data, "acknowledged"):
                            status = "✅ Acknowledged" if data.acknowledged else "❌ Not Acknowledged"
                            print(f"   Status: {status}")

                        if hasattr(data, "matched"):
                            print(f"   Records Matched: {data.matched}")

                        if hasattr(data, "modified"):
                            print(f"   Records Modified: {data.modified}")

                        if hasattr(data, "upserted"):
                            print(f"   Records Upserted: {data.upserted}")

                        if hasattr(data, "upserted_id") and data.upserted_id:
                            print(f"   Upserted ID: {data.upserted_id}")
                    else:
                        print("   ❌ No response data")

                    print("-" * 40)

                # Summary
                total_nodes = len(update_response.root)
                print(f"\n📈 Summary: Update completed across {total_nodes} node(s)")
            else:
                print("❌ No response data received")

        except Exception as e:
            print(f"❌ Update failed: {e}")
            if hasattr(e, "status"):
                print(f"HTTP Status: {e.status}")
            if hasattr(e, "headers"):
                print(f"Response Headers: {e.headers}")

    except Exception as e:
        print(f"Failed to update data: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def delete_data_with_filter(builder_client: SecretVaultBuilderClient) -> None:
    """Delete data in a collection with a specific filter"""
    print("\n=== Delete Data (with filter) ===")

    # First show available collections
    print("Available collections:")
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available.")
        return

    # Extract collections data from response
    if hasattr(collections_response, "data") and collections_response.data:
        collections_data = collections_response.data
    else:
        collections_data = collections_response

    if not collections_data:
        print("No collections available.")
        return

    # Display collections
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        collection_id = None
        if hasattr(collection, "id"):
            collection_id = collection.id
        elif hasattr(collection, "_id"):
            collection_id = collection._id
        elif isinstance(collection, dict):
            collection_id = collection.get("id") or collection.get("_id")

        collection_name = "Unknown"
        if hasattr(collection, "name"):
            collection_name = collection.name
        elif isinstance(collection, dict) and "name" in collection:
            collection_name = collection["name"]

        print(f"   {i}. {collection_name} (ID: {collection_id})")

    # Get collection ID from user
    print("\nEnter collection ID to delete data from:")
    collection_id = input().strip()

    if not collection_id:
        print("No collection ID provided.")
        return

    try:
        # Use the specified filter for Sample Item 2
        filter_data = {
            "created_at": {
                "$gte": "2025-10-27T14:18:00Z",
                "$lte": "2025-10-28T07:00:00Z",
            },
        }

        print(f"\n🗑️  Using filter to delete: {filter_data}")
        print("⚠️  This will delete all records matching the filter!")

        # Confirm deletion
        confirm = input("\nAre you sure you want to proceed? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("❌ Deletion cancelled.")
            return

        # Use the actual delete_data_by_filter method
        delete_request = DeleteDataRequest(collection=collection_id, filter=filter_data)

        try:
            delete_response = await builder_client.delete_data(delete_request)
            print("✅ Deletion completed successfully!")

            # Format the response nicely
            print("\n📊 Delete Results:")
            print("=" * 60)

            if hasattr(delete_response, "root") and delete_response.root:
                for node_id, response in delete_response.root.items():
                    print(f"\n🔗 Node: {node_id}")

                    if hasattr(response, "data") and response.data:
                        data = response.data
                        if hasattr(data, "acknowledged"):
                            status = "✅ Acknowledged" if data.acknowledged else "❌ Not Acknowledged"
                            print(f"   Status: {status}")

                        if hasattr(data, "deletedCount"):
                            print(f"   Records Deleted: {data.deletedCount}")
                    else:
                        print("   ❌ No response data")

                    print("-" * 40)

                # Summary
                total_nodes = len(delete_response.root)
                print(f"\n📈 Summary: Deletion completed across {total_nodes} node(s)")
            else:
                print("❌ No response data received")

        except Exception as e:
            print(f"❌ Deletion failed: {e}")
            if hasattr(e, "status"):
                print(f"HTTP Status: {e.status}")
            if hasattr(e, "headers"):
                print(f"Response Headers: {e.headers}")

    except Exception as e:
        print(f"Failed to delete data: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def get_queries(builder_client: SecretVaultBuilderClient) -> None:
    """Get all queries"""
    print("\n=== Get Queries ===")

    try:
        queries_response = await builder_client.get_queries()

        # Display queries nicely
        if hasattr(queries_response, "root") and queries_response.root:
            # Get queries from the first node (they should be the same across all nodes)
            first_node_response = list(queries_response.root.values())[0]
            if hasattr(first_node_response, "data") and first_node_response.data:
                queries_data = first_node_response.data
                print(f"\n📋 Found {len(queries_data)} query(s):")
                print("=" * 60)

                for i, query in enumerate(queries_data, 1):
                    print(f"\n{i}. Query:")

                    # Handle QueryDocumentResponse objects
                    if hasattr(query, "id"):
                        print(f"   🆔 ID: {query.id}")
                    if hasattr(query, "name"):
                        print(f"   📝 Name: {query.name}")
                    if hasattr(query, "collection"):
                        print(f"   📚 Collection: {query.collection}")

                    print("-" * 40)

                print(f"\n✅ Successfully found {len(queries_data)} queries")
            else:
                print("No queries found in node response.")
        else:
            print("No queries found.")

    except Exception as e:
        print(f"Failed to get queries: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def create_query(builder_client: SecretVaultBuilderClient) -> None:
    """Create a new query using standard.query.json"""
    print("\n=== Create Query ===")

    # First show available collections
    print("Available collections:")
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available.")
        return

    # Extract collections data from response
    if hasattr(collections_response, "data") and collections_response.data:
        collections_data = collections_response.data
    else:
        collections_data = collections_response

    if not collections_data:
        print("No collections available.")
        return

    # Display collections
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        collection_id = None
        if hasattr(collection, "id"):
            collection_id = collection.id
        elif hasattr(collection, "_id"):
            collection_id = collection._id
        elif isinstance(collection, dict):
            collection_id = collection.get("id") or collection.get("_id")

        collection_name = "Unknown"
        if hasattr(collection, "name"):
            collection_name = collection.name
        elif isinstance(collection, dict) and "name" in collection:
            collection_name = collection["name"]

        print(f"   {i}. {collection_name} (ID: {collection_id})")

    # Get collection ID from user
    print("\nEnter collection ID to create query for:")
    collection_id = input().strip()

    if not collection_id:
        print("No collection ID provided.")
        return

    try:
        # Load the standard query schema
        with open("examples/data/query.json", "r", encoding="utf-8") as f:
            query_data = json.load(f)

        print(f"Query data keys: {list(query_data.keys())}")

        # Create the query request
        create_query_request = CreateQueryRequest(
            id=str(uuid.uuid4()),
            collection=collection_id,
            name="test-query",
            pipeline=query_data.get("pipeline", []),
            variables=query_data.get("variables", {}),
        )

        print(f"Creating query: {create_query_request.name}")
        query_response = await builder_client.create_query(create_query_request)

        # Display query creation response nicely
        print("\n✅ Query Creation Results:")
        print("=" * 50)

        if hasattr(query_response, "root"):
            total_created = 0

            for node_id, response in query_response.root.items():
                # Check if response is empty (success) or has error
                if response is None or (hasattr(response, "data") and not response.data):
                    total_created += 1
                    print(f"🔗 Node {node_id[:20]}...:")
                    print(f"   ✅ Created: 1 query")
                else:
                    print(f"🔗 Node {node_id[:20]}...:")
                    print(f"   ❌ Failed to create query")

            print(f"\n📊 Summary: {total_created} query created")
        else:
            print("✅ Query created successfully!")

    except Exception as e:
        print(f"Failed to create query: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def delete_query(builder_client: SecretVaultBuilderClient) -> None:
    """Delete a query"""
    print("\n=== Delete Query ===")

    try:
        # First get available queries
        print("Available queries:")
        queries_response = await builder_client.get_queries()

        if not queries_response:
            print("No queries available.")
            return

        # Extract queries data from response (same structure as get_queries)
        if hasattr(queries_response, "root") and queries_response.root:
            # Get queries from the first node (they should be the same across all nodes)
            first_node_response = list(queries_response.root.values())[0]
            if hasattr(first_node_response, "data") and first_node_response.data:
                queries_data = first_node_response.data
            else:
                print("No queries found in node response.")
                return
        else:
            print("No queries available.")
            return

        # Display queries
        print(f"\n📋 Found {len(queries_data)} query(s):")
        for i, query in enumerate(queries_data, 1):
            query_id = None
            if hasattr(query, "id"):
                query_id = query.id
            elif hasattr(query, "_id"):
                query_id = query._id
            elif isinstance(query, dict):
                query_id = query.get("id") or query.get("_id")

            query_name = "Unknown"
            if hasattr(query, "name"):
                query_name = query.name
            elif isinstance(query, dict) and "name" in query:
                query_name = query["name"]

            print(f"   {i}. {query_name} (ID: {query_id})")

        # Get query ID from user
        print("\nEnter query ID to delete (or 'all' to delete all):")
        user_input = input().strip()

        if user_input.lower() == "all":
            query_ids_to_delete = []
            for query in queries_data:
                # Handle different query data structures
                if hasattr(query, "id"):
                    query_ids_to_delete.append(query.id)
                elif hasattr(query, "_id"):
                    query_ids_to_delete.append(query._id)
                elif isinstance(query, dict) and ("id" in query or "_id" in query):
                    query_ids_to_delete.append(query.get("id") or query.get("_id"))
        else:
            query_ids_to_delete = [user_input] if user_input else []

        if not query_ids_to_delete:
            print("No query IDs provided for deletion.")
            return

        print(f"\nDeleting {len(query_ids_to_delete)} query(ies)...")
        for query_id in query_ids_to_delete:
            try:
                print(f"Deleting query: {query_id}")
                delete_response = await builder_client.delete_query(query_id)
                print(f"✅ Successfully deleted query {query_id}")
            except Exception as e:
                print(f"❌ Failed to delete query {query_id}: {e}")
                if hasattr(e, "status"):
                    print(f"HTTP Status: {e.status}")
                if hasattr(e, "headers"):
                    print(f"Response Headers: {e.headers}")

        # List queries to verify
        print("\nListing queries to verify:")
        await get_queries(builder_client)

    except Exception as e:
        print(f"Failed to delete query: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def run_query(builder_client: SecretVaultBuilderClient) -> None:
    """Run a query with retry logic until it's ready"""
    print("\n=== Run Query ===")

    # First show available queries
    print("Available queries:")
    queries_response = await builder_client.get_queries()

    if not queries_response:
        print("No queries available.")
        return

    # Extract queries data from response (same structure as get_queries)
    if hasattr(queries_response, "root") and queries_response.root:
        # Get queries from the first node (they should be the same across all nodes)
        first_node_response = list(queries_response.root.values())[0]
        if hasattr(first_node_response, "data") and first_node_response.data:
            queries_data = first_node_response.data
        else:
            print("No queries found in node response.")
            return
    else:
        print("No queries available.")
        return

    # Display queries
    print(f"\n📋 Found {len(queries_data)} query(s):")
    for i, query in enumerate(queries_data, 1):
        query_id = None
        if hasattr(query, "id"):
            query_id = query.id
        elif hasattr(query, "_id"):
            query_id = query._id
        elif isinstance(query, dict):
            query_id = query.get("id") or query.get("_id")

        query_name = "Unknown"
        if hasattr(query, "name"):
            query_name = query.name
        elif isinstance(query, dict) and "name" in query:
            query_name = query["name"]

        print(f"   {i}. {query_name} (ID: {query_id})")

    # Get query ID from user
    print("\nEnter query ID to run:")
    query_id = input().strip()

    if not query_id:
        print("No query ID provided.")
        return

    try:
        print(f"\n🚀 Running query: {query_id}")

        # Create RunQueryRequest
        run_query_request = RunQueryRequest(_id=query_id, variables={"name": "Sample Item 1"})

        # First, run the query
        run_response = await builder_client.run_query(run_query_request)
        print("✅ Query execution started!")

        # Extract the run IDs from all nodes
        run_ids = {}
        if hasattr(run_response, "root"):
            for node_id, response in run_response.root.items():
                if hasattr(response, "data") and response.data:
                    run_ids[node_id] = response.data
        elif hasattr(run_response, "data"):
            # If it's a direct response, we need to get the node IDs from the response
            if hasattr(run_response, "root") and run_response.root:
                # Get the first node ID and assign the run ID to it
                first_node_id = list(run_response.root.keys())[0]
                run_ids[first_node_id] = run_response.data
            else:
                print("❌ Cannot determine node mapping for run ID.")
                return

        if not run_ids:
            print("❌ No run IDs received from query execution.")
            return

        print(f"🆔 Run IDs from nodes:")
        for node_id, run_id in run_ids.items():
            print(f"   {node_id[:20]}...: {run_id}")

        # Now poll for results with retry logic
        print("\n⏳ Waiting for query results...")

        # Retry intervals: 3, 5, 10, 30, then 60 seconds
        retry_intervals = [3, 5, 10, 30, 60]
        current_interval_index = 0
        attempt = 1

        while True:
            try:
                print(f"\n📊 Attempt {attempt}: Checking query status...")

                # Check each node with their specific run ID
                all_completed = True
                all_failed = True
                completed_results = []

                for node_id, run_id in run_ids.items():
                    try:
                        # Call the specific node directly with their run ID
                        # We need to find the client for this specific node
                        node_client = None
                        for client in builder_client.nodes:
                            if client.id == node_id:
                                node_client = client
                                break

                        if not node_client:
                            print(f"❌ Node {node_id[:20]}...: Client not found")
                            all_completed = False
                            all_failed = False
                            continue

                        # Call this specific node with their specific run ID
                        read_response = await node_client.read_query_run_results(
                            builder_client._mint_root_invocation(
                                audience=node_client.id,
                                command=NucCmd.NIL_DB_QUERIES_READ,
                            ),
                            run_id,
                        )

                        # Check if this node has results
                        if read_response and hasattr(read_response, "data") and read_response.data:
                            # The data might be a single ReadQueryRunByIdDto object or a list
                            run_results = read_response.data

                            # Handle both single object and list cases
                            if isinstance(run_results, list):
                                if run_results and len(run_results) > 0:
                                    run_result = run_results[0]
                                else:
                                    print(f"⏳ Node {node_id[:20]}...: Still processing")
                                    all_completed = False
                                    all_failed = False
                                    continue
                            else:
                                # Single object case
                                run_result = run_results

                            status = run_result.status

                            if status == "complete":
                                print(f"✅ Node {node_id[:20]}...: Query completed")
                                if run_result.result:
                                    completed_results.append(run_result.result)
                                all_failed = False
                            elif status == "error":
                                errors = run_result.errors or []
                                print(f"❌ Node {node_id[:20]}...: Query failed - {errors}")
                                all_completed = False
                            else:
                                print(f"⏳ Node {node_id[:20]}...: Status {status}")
                                all_completed = False
                                all_failed = False
                        else:
                            print(f"⏳ Node {node_id[:20]}...: Still processing")
                            all_completed = False
                            all_failed = False
                    except Exception as e:
                        print(f"❌ Node {node_id[:20]}...: Error checking status - {e}")
                        all_completed = False
                        all_failed = False

                # Check if all nodes are complete or failed
                if all_completed and completed_results:
                    print("\n✅ Query completed on all nodes! Results:")
                    print("=" * 60)

                    # Check if we need to unify concealed data
                    if builder_client._options.key:
                        print("🔐 Unifying concealed data from all nodes...")
                        try:
                            # Group results by _id so we can unify each document's shares
                            documents_by_id = {}

                            for result in completed_results:
                                if isinstance(result, list):
                                    # If it's a list, process each item
                                    for item in result:
                                        if isinstance(item, dict) and "_id" in item:
                                            doc_id = item["_id"]
                                            if doc_id not in documents_by_id:
                                                documents_by_id[doc_id] = []
                                            documents_by_id[doc_id].append(item)
                                else:
                                    # If it's already a dictionary
                                    if isinstance(result, dict) and "_id" in result:
                                        doc_id = result["_id"]
                                        if doc_id not in documents_by_id:
                                            documents_by_id[doc_id] = []
                                        documents_by_id[doc_id].append(result)

                            # Unify each document's shares
                            unified_results = []
                            for doc_id, shares in documents_by_id.items():
                                unified_doc = await reveal(builder_client._options.key, shares)
                                unified_results.append(unified_doc)

                            # Display unified results
                            for i, item in enumerate(unified_results, 1):
                                print(f"\n{i}. Unified Result:")
                                for key, value in item.items():
                                    print(f"   {key}: {value}")
                                print("-" * 40)

                            print(f"\n📊 Total unified results: {len(unified_results)}")

                        except Exception as e:
                            print(f"❌ Error unifying concealed data: {e}")
                            print("Displaying raw results instead:")

                            # Fallback to displaying raw results
                            all_results = []
                            for result in completed_results:
                                if isinstance(result, list):
                                    all_results.extend(result)
                                else:
                                    all_results.append(result)

                            # Display combined results
                            for i, item in enumerate(all_results, 1):
                                print(f"\n{i}. Result:")
                                for key, value in item.items():
                                    print(f"   {key}: {value}")
                                print("-" * 40)

                            print(f"\n📊 Total results: {len(all_results)}")
                    else:
                        # For non-concealed data, just combine results
                        all_results = []
                        for result in completed_results:
                            if isinstance(result, list):
                                all_results.extend(result)
                            else:
                                all_results.append(result)

                        # Display combined results
                        for i, item in enumerate(all_results, 1):
                            print(f"\n{i}. Result:")
                            for key, value in item.items():
                                print(f"   {key}: {value}")
                            print("-" * 40)

                        print(f"\n📊 Total results: {len(all_results)}")
                    break
                elif all_failed:
                    print("\n❌ Query failed on all nodes")
                    break
                else:
                    # Calculate wait time
                    if current_interval_index < len(retry_intervals):
                        wait_time = retry_intervals[current_interval_index]
                        current_interval_index += 1
                    else:
                        wait_time = 60  # After all intervals, wait 60 seconds

                    print(f"\n⏰ Waiting {wait_time} seconds before next check...")
                    await asyncio.sleep(wait_time)
                    attempt += 1

            except Exception as e:
                print(f"❌ Error checking query status: {e}")

                # Calculate wait time for error case
                if current_interval_index < len(retry_intervals):
                    wait_time = retry_intervals[current_interval_index]
                    current_interval_index += 1
                else:
                    wait_time = 60

                print(f"⏰ Waiting {wait_time} seconds before retry...")
                await asyncio.sleep(wait_time)
                attempt += 1

    except Exception as e:
        print(f"Failed to run query: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def flush_data_with_collection(builder_client: SecretVaultBuilderClient) -> None:
    """Flush all data from a collection"""
    print("\n=== Flush Data (Remove All) ===")

    # First show available collections
    print("Available collections:")
    collections_response = await builder_client.read_collections()
    if not collections_response:
        print("No collections available.")
        return

    # Extract collections data from response
    if hasattr(collections_response, "data") and collections_response.data:
        collections_data = collections_response.data
    else:
        collections_data = collections_response

    if not collections_data:
        print("No collections available.")
        return

    # Display collections
    print(f"\n📚 Found {len(collections_data)} collection(s):")
    for i, collection in enumerate(collections_data, 1):
        collection_id = None
        if hasattr(collection, "id"):
            collection_id = collection.id
        elif hasattr(collection, "_id"):
            collection_id = collection._id
        elif isinstance(collection, dict):
            collection_id = collection.get("id") or collection.get("_id")

        collection_name = "Unknown"
        if hasattr(collection, "name"):
            collection_name = collection.name
        elif isinstance(collection, dict) and "name" in collection:
            collection_name = collection["name"]

        print(f"   {i}. {collection_name} (ID: {collection_id})")

    # Get collection ID from user
    print("\nEnter collection ID to flush all data from:")
    collection_id = input().strip()

    if not collection_id:
        print("No collection ID provided.")
        return

    try:
        print(f"\n🗑️  Flushing ALL data from collection: {collection_id}")
        print("⚠️  This will remove ALL records from the collection!")

        # Confirm deletion
        confirm = input("\nAre you sure you want to proceed? (yes/no): ").strip().lower()
        if confirm != "yes":
            print("❌ Flush cancelled.")
            return

        # Perform the flush
        print(f"\n🗑️  Flushing all data...")

        try:
            flush_response = await builder_client.flush_data(collection_id)
            print("✅ Flush completed successfully!")

            # Format the response nicely
            print("\n📊 Flush Results:")
            print("=" * 60)

            if hasattr(flush_response, "root") and flush_response.root:
                for node_id, response in flush_response.root.items():
                    print(f"\n🔗 Node: {node_id}")

                    if response is None:
                        print("   ✅ Flush completed (no response data)")
                    else:
                        print(f"   Response: {response}")

                    print("-" * 40)

                # Summary
                total_nodes = len(flush_response.root)
                print(f"\n📈 Summary: Flush completed across {total_nodes} node(s)")
            else:
                print("❌ No response data received")

        except Exception as e:
            print(f"❌ Flush failed: {e}")
            if hasattr(e, "status"):
                print(f"HTTP Status: {e.status}")
            if hasattr(e, "headers"):
                print(f"Response Headers: {e.headers}")

    except Exception as e:
        print(f"Failed to flush data: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def setup_user() -> Optional[SecretVaultUserClient]:
    """Setup user keypair and client"""
    print("\n=== Setup User ===")

    try:
        # Generate a new user keypair
        print("🔑 Generating new user keypair...")
        user_keypair = Keypair.generate()
        print(f"✅ User keypair generated")
        print(f"   DID: {user_keypair.to_did_string()}")
        print(f"   Private Key: {user_keypair.private_key_hex()}")

        # Create user client
        print("\n👤 Creating SecretVaultUserClient...")
        user_client = await SecretVaultUserClient.from_options(
            keypair=user_keypair,
            base_urls=config["NILDB_NODES"],
            blindfold=BlindfoldFactoryConfig(operation=BlindfoldOperation.STORE, use_cluster_key=True),
        )

        print("✅ User client created successfully!")
        print(f"   User DID: {user_client.id}")
        print(f"   Connected to {len(user_client.nodes)} node(s)")

        return user_client

    except Exception as e:
        print(f"❌ Failed to setup user: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")
        return None


async def create_owned_data(builder_client: SecretVaultBuilderClient, user_client: SecretVaultUserClient) -> None:
    """Create data with delegation token from builder to user"""
    print("\n=== Create Data with Delegation Token ===")

    try:
        # First, get a collection to use
        print("📚 Getting available collections...")
        collections_response = await builder_client.read_collections()

        if not collections_response:
            print("❌ No collections available. Please create a collection first.")
            return

        # Extract collections data from response
        if hasattr(collections_response, "data") and collections_response.data:
            collections_data = collections_response.data
        else:
            collections_data = collections_response

        if not collections_data:
            print("❌ No collections available. Please create a collection first.")
            return

        # Display collections for selection
        print(f"\n📚 Found {len(collections_data)} collection(s):")
        for i, collection in enumerate(collections_data, 1):
            collection_id = None
            if hasattr(collection, "id"):
                collection_id = collection.id
            elif hasattr(collection, "_id"):
                collection_id = collection._id
            elif isinstance(collection, dict):
                collection_id = collection.get("id") or collection.get("_id")

            collection_name = "Unknown"
            if hasattr(collection, "name"):
                collection_name = collection.name
            elif isinstance(collection, dict) and "name" in collection:
                collection_name = collection["name"]

            print(f"   {i}. {collection_name} (ID: {collection_id})")

        # Get collection ID from user
        print("\nEnter collection ID to use:")
        collection_id = input().strip()

        if not collection_id:
            print("No collection ID provided.")
            return

        # Create delegation token
        print("\n🔑 Creating delegation token...")

        # Get builder's root token
        await builder_client.refresh_root_token()
        root_token_envelope = builder_client.root_token

        if not root_token_envelope:
            print("❌ No root token available. Cannot create delegation token.")
            return

        # Create delegation token extending the root token envelope
        delegation_token = (
            NucTokenBuilder.extending(root_token_envelope)
            .command(Command(NucCmd.NIL_DB_DATA_CREATE.value.split(".")))
            .audience(user_client.id)
            .expires_at(datetime.fromtimestamp(into_seconds_from_now(60)))
            .build(builder_client.keypair.private_key())
        )

        print(f"✅ Delegation token created successfully!")
        print(f"   Audience: {user_client.id}")
        print(f"   Expires: 1 minute from now")

        # Create user data
        user_data = {"_id": str(uuid.uuid4()), "name": "User owned Item 1", "country_code": {"%allot": "US"}}

        print(f"\n📝 Creating data entry")

        # Upload data using delegation token
        print("\n📤 Uploading data with delegation token...")

        # Create the data with ACL included
        create_data_request = CreateOwnedDataRequest(
            collection=collection_id,
            owner=user_client.id,
            data=[user_data],
            acl=AclDto(grantee=builder_client.keypair.to_did_string(), read=True, write=False, execute=True),
        )

        data_response = await user_client.create_data(delegation=delegation_token, body=create_data_request)

        # Display data creation response nicely
        print("\n✅ Data Creation Results:")
        print("=" * 50)

        if hasattr(data_response, "root"):
            total_created = 0
            total_errors = 0

            for node_id, response in data_response.root.items():
                if hasattr(response, "data"):
                    created_count = len(response.data.created) if response.data.created else 0
                    error_count = len(response.data.errors) if response.data.errors else 0
                    total_created += created_count
                    total_errors += error_count

                    print(f"🔗 Node {node_id[:20]}...:")
                    print(f"   ✅ Created: {created_count} records")
                    if error_count > 0:
                        print(f"   ❌ Errors: {error_count} records")

            print(f"\n📊 Summary: {total_created} records created, {total_errors} errors")
        else:
            print("Data creation response:", data_response)

        print("=" * 50)
        print(f"   Owner: {user_client.id}")
        print(f"   Collection: {collection_id}")
        print(f"   ACL granted to builder: read=true, write=false, execute=true")

    except Exception as e:
        print(f"❌ Failed to create data: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")
        traceback.print_exc()


async def read_user_profile(user_client: SecretVaultUserClient) -> None:
    """Read user profile"""
    print("\n=== Read User Profile ===")

    try:
        print("📖 Reading user profile...")
        profile_response = await user_client.read_profile()

        if not profile_response:
            print("❌ No profile information available.")
            return

        print("\n👤 User Profile")
        print("=" * 50)

        # Check if profile has data attribute (wrapped response)
        if hasattr(profile_response, "data") and profile_response.data:
            profile_data = profile_response.data
        else:
            profile_data = profile_response

        # Basic profile info
        if hasattr(profile_data, "id"):
            print(f"🆔 ID: {profile_data.id}")

        # Timestamps
        if hasattr(profile_data, "created"):
            created_date = profile_data.created.strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"📅 Created: {created_date}")

        if hasattr(profile_data, "updated"):
            updated_date = profile_data.updated.strftime("%Y-%m-%d %H:%M:%S UTC")
            print(f"🔄 Updated: {updated_date}")

        # Logs
        if hasattr(profile_data, "logs") and profile_data.logs:
            print(f"\n📋 Activity Log ({len(profile_data.logs)} entries):")
            for i, log_entry in enumerate(profile_data.logs, 1):
                print(f"   {i}. Operation: {log_entry.op}")
                if hasattr(log_entry, "collection") and log_entry.collection:
                    print(f"      Collection: {log_entry.collection}")
                if hasattr(log_entry, "acl") and log_entry.acl:
                    print(f"      ACL: {log_entry.acl}")
                print()
        else:
            print("\n📋 Activity Log: No entries")

        print("=" * 50)

    except Exception as e:
        print(f"❌ Failed to read user profile: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def list_data_references(user_client: SecretVaultUserClient) -> None:
    """List all data references owned by the user"""
    print("\n=== List Data References ===")

    try:
        print("📋 Listing data references...")
        references_response = await user_client.list_data_references()
        if not references_response:
            print("❌ No data references available.")
            return

        print("\n📋 Data References")
        print("=" * 50)

        # The response is already the data we need
        references_data = references_response

        if not references_data or not hasattr(references_data, "data") or not references_data.data:
            print("📋 No data references found")
            return

        # Display references
        print(f"📋 Found {len(references_data.data)} data reference(s):")
        for i, reference in enumerate(references_data.data, 1):
            print(f"\n   {i}. Reference Details:")
            if hasattr(reference, "builder"):
                print(f"      Builder: {reference.builder}")
            if hasattr(reference, "collection"):
                print(f"      Collection: {reference.collection}")
            if hasattr(reference, "document"):
                print(f"      Document: {reference.document}")

        print("=" * 50)

    except Exception as e:
        print(f"❌ Failed to list data references: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def read_document(user_client: SecretVaultUserClient) -> None:
    """Read a specific document by collection and document ID"""
    print("\n=== Read Document ===")

    try:
        # First, get the list of available data references
        print("📋 Getting available data references...")
        references_response = await user_client.list_data_references()

        if not references_response or not hasattr(references_response, "data") or not references_response.data:
            print("❌ No data references available. Please create some data first.")
            return

        # Display available references for selection
        print(f"\n📋 Available Documents ({len(references_response.data)}):")
        print("=" * 60)

        for i, reference in enumerate(references_response.data, 1):
            print(f"{i:2d}. Collection: {reference.collection}")
            print(f"    Document: {reference.document}")
            print(f"    Builder: {reference.builder}")
            print()

        # Get user selection
        print("Enter the number of the document to read (or 0 to cancel):")
        try:
            selection = int(input().strip())
            if selection == 0:
                print("❌ Cancelled.")
                return
            if selection < 1 or selection > len(references_response.data):
                print(f"❌ Invalid selection. Please enter a number between 1 and {len(references_response.data)}.")
                return
        except ValueError:
            print("❌ Invalid input. Please enter a number.")
            return

        # Get the selected reference
        selected_reference = references_response.data[selection - 1]
        collection_id = selected_reference.collection
        document_id = selected_reference.document

        print(f"\n📖 Reading document {document_id} from collection {collection_id}...")

        # Create the request parameters
        read_params = ReadDataRequestParams(collection=collection_id, document=document_id)

        # Read the document
        document_response = await user_client.read_data(read_params)

        if not document_response:
            print("❌ No document data available.")
            return

        print("\n📄 Document Data")
        print("=" * 50)

        # Check if response has data attribute (wrapped response)
        if hasattr(document_response, "data") and document_response.data:
            document_data = document_response.data
        else:
            document_data = document_response

        if not document_data:
            print("📄 No document data found")
            return

        # Display document data with better formatting
        if document_data:
            print(f"📄 Document contains {len(document_data)} field(s):")
            print("-" * 50)

            # Separate DTO fields from data fields for better display
            dto_fields = ["id", "created", "updated", "owner", "acl"]
            dto_data = {}
            data_fields = {}

            for field_name, field_value in document_data.items():
                if field_name in dto_fields:
                    dto_data[field_name] = field_value
                else:
                    data_fields[field_name] = field_value

            # Display DTO fields (metadata)
            if dto_data:
                print("📋 Metadata (DTO Fields):")
                for field_name, field_value in dto_data.items():
                    print(f"   {field_name}: {field_value}")
                print()

            # Display data fields
            if data_fields:
                print("📄 Data Fields:")
                for field_name, field_value in data_fields.items():
                    # Check if field is encrypted
                    if isinstance(field_value, dict) and "%share" in field_value:
                        print(f"   {field_name}: 🔐 [ENCRYPTED] {field_value}")
                    else:
                        print(f"   {field_name}: {field_value}")
            else:
                print("📄 No data fields found")
        else:
            print("📄 No document data found")

        print("=" * 50)
        print(f"   Collection: {collection_id}")
        print(f"   Document: {document_id}")

    except Exception as e:
        print(f"❌ Failed to read document: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def delete_document(user_client: SecretVaultUserClient) -> None:
    """Delete a specific document by collection and document ID"""
    print("\n=== Delete Document ===")

    try:
        # First, get the list of available data references
        print("📋 Getting available data references...")
        references_response = await user_client.list_data_references()

        if not references_response or not hasattr(references_response, "data") or not references_response.data:
            print("❌ No data references available. Please create some data first.")
            return

        # Display available references for selection
        print(f"\n📋 Available Documents ({len(references_response.data)}):")
        print("=" * 60)

        for i, reference in enumerate(references_response.data, 1):
            print(f"{i:2d}. Collection: {reference.collection}")
            print(f"    Document: {reference.document}")
            print(f"    Builder: {reference.builder}")
            print()

        # Get user selection
        print("Enter the number of the document to delete:")
        print("  0 = Cancel")
        print("  all = Delete all documents")
        print("  (or enter a number 1-" + str(len(references_response.data)) + ")")

        try:
            user_input = input().strip().lower()

            if user_input == "0":
                print("❌ Cancelled.")
                return
            elif user_input == "all":
                # Confirm deletion of all documents
                print(f"\n⚠️  WARNING: You are about to delete ALL {len(references_response.data)} documents!")
                print("This action cannot be undone.")
                confirm = input("Type 'yes' to confirm deletion of all documents: ").strip().lower()

                if confirm != "yes":
                    print("❌ Deletion cancelled.")
                    return

                print(f"\n🗑️  Deleting all {len(references_response.data)} documents...")

                deleted_count = 0
                failed_count = 0

                for reference in references_response.data:
                    collection_id = reference.collection
                    document_id = reference.document

                    print(f"🗑️  Deleting {document_id} from {collection_id}...")

                    # Create the request parameters
                    delete_params = DeleteDocumentRequestParams(collection=collection_id, document=document_id)

                    try:
                        delete_response = await user_client.delete_data(delete_params)
                        if delete_response:
                            print(f"✅ Deleted {document_id} from {collection_id}")
                            deleted_count += 1
                        else:
                            print(f"❌ No response for {document_id} from {collection_id}")
                            failed_count += 1
                    except Exception as delete_error:
                        print(f"❌ Failed to delete {document_id} from {collection_id}: {delete_error}")
                        failed_count += 1

                print("\n" + "=" * 50)
                print("🗑️  BULK DELETE COMPLETE")
                print("=" * 50)
                print(f"✅ Successfully deleted: {deleted_count} document(s)")
                if failed_count > 0:
                    print(f"❌ Failed to delete: {failed_count} document(s)")
                print("=" * 50)
                return
            else:
                # Single document deletion
                selection = int(user_input)
                if selection < 1 or selection > len(references_response.data):
                    print(f"❌ Invalid selection. Please enter a number between 1 and {len(references_response.data)}.")
                    return

                # Get the selected reference
                selected_reference = references_response.data[selection - 1]
                collection_id = selected_reference.collection
                document_id = selected_reference.document

                print(f"\n🗑️  Deleting document {document_id} from collection {collection_id}...")

                # Create the request parameters
                delete_params = DeleteDocumentRequestParams(collection=collection_id, document=document_id)

                # Delete the document
                delete_response = await user_client.delete_data(delete_params)

                if delete_response:
                    print("\n✅ Document deleted successfully!")
                    print("=" * 50)
                    print(f"   Collection: {collection_id}")
                    print(f"   Document: {document_id}")
                    print(f"   Deleted from {len(delete_response)} node(s)")
                else:
                    print("❌ No response from delete operation.")

        except ValueError:
            print("❌ Invalid input. Please enter a number, 'all', or '0' to cancel.")
            return

    except Exception as e:
        print(f"❌ Failed to delete document: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def update_document(user_client: SecretVaultUserClient) -> None:
    """Update a specific document by collection and document ID"""
    print("\n=== Update Document ===")

    try:
        # First, ensure user profile exists by reading it
        print("📖 Checking user profile...")
        try:
            profile_response = await user_client.read_profile()
            print("✅ User profile exists")
        except Exception as profile_error:
            print(f"❌ User profile not found: {profile_error}")
            print("💡 The user may need to be properly initialized first.")
            print("   Try creating some data with the user to establish their profile.")
            return

        # Get the list of available data references
        print("📋 Getting available data references...")
        references_response = await user_client.list_data_references()

        if not references_response or not hasattr(references_response, "data") or not references_response.data:
            print("❌ No data references available. Please create some data first.")
            return

        # Display available references for selection
        print(f"\n📋 Available Documents ({len(references_response.data)}):")
        print("=" * 60)

        for i, reference in enumerate(references_response.data, 1):
            print(f"{i:2d}. Collection: {reference.collection}")
            print(f"    Document: {reference.document}")
            print(f"    Builder: {reference.builder}")
            print()

        # Get user selection
        print("Enter the number of the document to update (or 0 to cancel):")
        try:
            selection = int(input().strip())
            if selection == 0:
                print("❌ Cancelled.")
                return
            if selection < 1 or selection > len(references_response.data):
                print(f"❌ Invalid selection. Please enter a number between 1 and {len(references_response.data)}.")
                return
        except ValueError:
            print("❌ Invalid input. Please enter a number.")
            return

        # Get the selected reference
        selected_reference = references_response.data[selection - 1]
        collection_id = selected_reference.collection
        document_id = selected_reference.document

        print(f"\n📝 Updating document {document_id} from collection {collection_id}")
        print("=" * 60)

        # Read the current document to show what can be updated
        print("📖 Reading current document...")
        try:
            read_params = ReadDataRequestParams(collection=collection_id, document=document_id)
            current_document = await user_client.read_data(read_params)

            if current_document and hasattr(current_document, "data"):
                print("📋 Current document data:")
                print("=" * 40)
                for key, value in current_document.data.items():
                    if key not in ["id", "created", "updated", "owner", "acl"]:
                        print(f"   {key}: {value}")
                print("=" * 40)
            else:
                print("⚠️  Could not read current document data.")
        except Exception as read_error:
            print(f"⚠️  Could not read current document: {read_error}")
            print("   Proceeding with update anyway...")

        # Get update data from user
        print("\n📝 Using fixed update data...")
        update_data = {"$set": {"country_code": {"%allot": "MX"}}}

        # Confirm the update
        print(f"\n📝 Update Summary:")
        print(f"   Document: {document_id}")
        print(f"   Collection: {collection_id}")
        print(f"   Update: {update_data}")

        confirm = input("\nConfirm update? (y/n): ").strip().lower()
        if confirm not in ["y", "yes"]:
            print("❌ Update cancelled.")
            return

        # Create the update request
        update_request = UpdateUserDataRequest(collection=collection_id, document=document_id, update=update_data)

        print(f"\n📝 Updating document...")
        print(f"   Request details:")
        print(f"     Collection: {collection_id}")
        print(f"     Document: {document_id}")
        print(f"     Update: {update_data}")

        # Update the document
        update_response = await user_client.update_data(update_request)

        if update_response and hasattr(update_response, "root"):
            has_errors = False
            for node_id, response in update_response.root.items():
                if hasattr(response, "status") and response.status != 204:
                    has_errors = True
                    break

            if has_errors:
                print("❌ Update failed on some nodes.")
            else:
                print("\n✅ Document updated successfully!")
                print("=" * 50)
                print(f"   Collection: {collection_id}")
                print(f"   Document: {document_id}")
                print(f"   Updated on {len(update_response.root)} node(s)")
        else:
            print("❌ No response from update operation.")

    except Exception as e:
        print(f"❌ Failed to update document: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def grant_access(user_client: SecretVaultUserClient) -> None:
    """Grant access to a document for another user"""
    print("\n=== Grant Access ===")

    try:
        # First, ensure user profile exists by reading it
        print("📖 Checking user profile...")
        try:
            profile_response = await user_client.read_profile()
            print("✅ User profile exists")
        except Exception as profile_error:
            print(f"❌ User profile not found: {profile_error}")
            print("💡 The user may need to be properly initialized first.")
            print("   Try creating some data with the user to establish their profile.")
            return

        # First, get the list of available data references
        print("📋 Getting available data references...")
        references_response = await user_client.list_data_references()

        if not references_response or not hasattr(references_response, "data") or not references_response.data:
            print("❌ No data references available. Please create some data first.")
            return

        # Display available references for selection
        print(f"\n📋 Available Documents ({len(references_response.data)}):")
        print("=" * 60)

        for i, reference in enumerate(references_response.data, 1):
            print(f"{i:2d}. Collection: {reference.collection}")
            print(f"    Document: {reference.document}")
            print(f"    Builder: {reference.builder}")
            print()

        # Get user selection
        print("Enter the number of the document to grant access to (or 0 to cancel):")
        try:
            selection = int(input().strip())
            if selection == 0:
                print("❌ Cancelled.")
                return
            if selection < 1 or selection > len(references_response.data):
                print(f"❌ Invalid selection. Please enter a number between 1 and {len(references_response.data)}.")
                return
        except ValueError:
            print("❌ Invalid input. Please enter a number.")
            return

        # Get the selected reference
        selected_reference = references_response.data[selection - 1]
        collection_id = selected_reference.collection
        document_id = selected_reference.document

        print(f"\n🔐 Granting access to document {document_id} from collection {collection_id}")
        print("=" * 60)

        # Use the hardcoded DID as the grantee
        print("🔑 Using hardcoded DID as grantee...")
        grantee_did = "did:nil:03d2312cb24fb2664a2a770fd72e7d83ffbc5a96d6b2308b1b0801ccbc5d20ecb5"
        print(f"   DID: {grantee_did}")

        confirm = input("\nConfirm grant access? (y/n): ").strip().lower()
        if confirm not in ["y", "yes"]:
            print("❌ Access grant cancelled.")
            return

        # Create the ACL
        acl = AclDto(grantee=grantee_did, read=True, write=True, execute=True)

        # Create the grant request
        grant_request = GrantAccessToDataRequest(collection=collection_id, document=document_id, acl=acl)

        print(f"\n🔐 Granting access...")
        print(f"   Request details:")
        print(f"     Collection: {collection_id}")
        print(f"     Document: {document_id}")
        print(f"     ACL: {acl.model_dump()}")

        # Grant the access
        grant_response = await user_client.grant_access(grant_request)

        if grant_response and hasattr(grant_response, "root"):
            has_errors = False
            for node_id, response in grant_response.root.items():
                if hasattr(response, "status") and response.status != 204:
                    has_errors = True
                    break

            if has_errors:
                print("❌ Grant access failed on some nodes.")
            else:
                print("\n✅ Access granted successfully!")
                print("=" * 50)
                print(f"   Collection: {collection_id}")
                print(f"   Document: {document_id}")
                print(f"   Grantee: {grantee_did}")
                print(f"   Granted to {len(grant_response.root)} node(s)")
        else:
            print("❌ No response from grant access operation.")

    except Exception as e:
        print(f"❌ Failed to grant access: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


async def revoke_access(user_client: SecretVaultUserClient) -> None:
    """Revoke access to a document for another user"""
    print("\n=== Revoke Access ===")

    try:
        # First, ensure user profile exists by reading it
        print("📖 Checking user profile...")
        try:
            profile_response = await user_client.read_profile()
            print("✅ User profile exists")
        except Exception as profile_error:
            print(f"❌ User profile not found: {profile_error}")
            print("💡 The user may need to be properly initialized first.")
            print("   Try creating some data with the user to establish their profile.")
            return

        # First, get the list of available data references
        print("📋 Getting available data references...")
        references_response = await user_client.list_data_references()

        if not references_response or not hasattr(references_response, "data") or not references_response.data:
            print("❌ No data references available. Please create some data first.")
            return

        # Display available references for selection
        print(f"\n📋 Available Documents ({len(references_response.data)}):")
        print("=" * 60)

        for i, reference in enumerate(references_response.data, 1):
            print(f"{i:2d}. Collection: {reference.collection}")
            print(f"    Document: {reference.document}")
            print(f"    Builder: {reference.builder}")
            print()

        # Get user selection
        print("Enter the number of the document to revoke access from (or 0 to cancel):")
        try:
            selection = int(input().strip())
            if selection == 0:
                print("❌ Cancelled.")
                return
            if selection < 1 or selection > len(references_response.data):
                print(f"❌ Invalid selection. Please enter a number between 1 and {len(references_response.data)}.")
                return
        except ValueError:
            print("❌ Invalid input. Please enter a number.")
            return

        # Get the selected reference
        selected_reference = references_response.data[selection - 1]
        collection_id = selected_reference.collection
        document_id = selected_reference.document

        print(f"\n🚫 Revoking access to document {document_id} from collection {collection_id}")
        print("=" * 60)

        # Use the hardcoded DID as the grantee
        print("🔑 Using hardcoded DID as grantee...")
        grantee_did = "did:nil:03d2312cb24fb2664a2a770fd72e7d83ffbc5a96d6b2308b1b0801ccbc5d20ecb5"
        print(f"   DID: {grantee_did}")

        confirm = input("\nConfirm revoke access? (y/n): ").strip().lower()
        if confirm not in ["y", "yes"]:
            print("❌ Access revoke cancelled.")
            return

        # Create the revoke request
        revoke_request = RevokeAccessToDataRequest(grantee=grantee_did, collection=collection_id, document=document_id)

        print(f"\n🚫 Revoking access...")
        print(f"   Request details:")
        print(f"     Collection: {collection_id}")
        print(f"     Document: {document_id}")
        print(f"     Grantee: {grantee_did}")

        # Revoke the access
        revoke_response = await user_client.revoke_access(revoke_request)

        if revoke_response and hasattr(revoke_response, "root"):
            has_errors = False
            for node_id, response in revoke_response.root.items():
                if hasattr(response, "status") and response.status != 204:
                    has_errors = True
                    break

            if has_errors:
                print("❌ Revoke access failed on some nodes.")
            else:
                print("\n✅ Access revoked successfully!")
                print("=" * 50)
                print(f"   Collection: {collection_id}")
                print(f"   Document: {document_id}")
                print(f"   Grantee: {grantee_did}")
                print(f"   Revoked from {len(revoke_response.root)} node(s)")
        else:
            print("❌ No response from revoke access operation.")

    except Exception as e:
        print(f"❌ Failed to revoke access: {e}")
        if hasattr(e, "status"):
            print(f"HTTP Status: {e.status}")
        if hasattr(e, "headers"):
            print(f"Response Headers: {e.headers}")


def print_menu():
    """Print the main menu"""
    print("\n" + "=" * 50)
    print("           SECRETVAULTS DEMO MENU")
    print("=" * 50)
    print("BUILDERS:")
    print("1.  Read cluster info")
    print("2.  Check subscription status")
    print("3.  Get root token")
    print("4.  Register builder")
    print("5.  Read builder profile")
    print()
    print("COLLECTIONS:")
    print("6.  List collections")
    print("7.  Create standard collection")
    print("8.  Read collection metadata")
    print("9.  Delete collections")
    print("10. Add collection index")
    print("11. Drop collection index")
    print()
    print("DATA:")
    print("12. Create standard data")
    print("13. Find data (all)")
    print("14. Find data (with filter)")
    print("15. Update data (with filter)")
    print("16. Delete data (with filter)")
    print("17. Flush data (remove all)")
    print()
    print("QUERIES:")
    print("18. List queries")
    print("19. Create query")
    print("20. Delete query")
    print("21. Run query")
    print()
    print("USERS:")
    print("22. Create owned collection (builder)")
    print("23. Setup user (generate keypair & create client)")
    print("24. Create data (with delegation token)")
    print("25. Read user profile")
    print("26. List data references")
    print("27. Read document")
    print("28. Delete document")
    print("29. Update document")
    print("30. Grant access")
    print("31. Revoke access")
    print()
    print("UTILITIES:")
    print("0.  Exit")
    print("=" * 50)


async def main():
    # Check environment variables first
    if not check_environment():
        return

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

        # Get root token for use in other functions
        await builder_client.refresh_root_token()

        # User client (will be set when user is created)
        user_client = None

        while True:
            print_menu()
            choice = input("Enter your choice (0-31): ").strip()

            if choice == "0":
                print("Goodbye!")
                break
            elif choice == "1":
                await read_cluster_info(builder_client)
            elif choice == "2":
                await check_subscription_status(builder_client)
            elif choice == "3":
                await get_root_token(builder_client)
            elif choice == "4":
                await register_builder(builder_client)
            elif choice == "5":
                await read_profile(builder_client)
            elif choice == "6":
                await list_collections(builder_client)
            elif choice == "7":
                await create_collection(builder_client, "standard")
            elif choice == "8":
                await read_collection_metadata(builder_client)
            elif choice == "9":
                await delete_collections(builder_client)
            elif choice == "10":
                await add_collection_index(builder_client)
            elif choice == "11":
                await drop_collection_index(builder_client)
            elif choice == "12":
                await create_standard_data(builder_client)
            elif choice == "13":
                await find_data_all(builder_client)
            elif choice == "14":
                await find_data_with_filter(builder_client)
            elif choice == "15":
                await update_data_with_filter(builder_client)
            elif choice == "16":
                await delete_data_with_filter(builder_client)
            elif choice == "17":
                await flush_data_with_collection(builder_client)
            elif choice == "18":
                await get_queries(builder_client)
            elif choice == "19":
                await create_query(builder_client)
            elif choice == "20":
                await delete_query(builder_client)
            elif choice == "21":
                await run_query(builder_client)
            elif choice == "22":
                await create_collection(builder_client, "owned")
            elif choice == "23":
                user_client = await setup_user()
            elif choice == "24":
                if user_client is None:
                    print("❌ No user client available. Please setup a user first (option 23).")
                else:
                    await create_owned_data(builder_client, user_client)
            elif choice == "25":
                if user_client is None:
                    print("❌ No user client available. Please setup a user first (option 23).")
                else:
                    await read_user_profile(user_client)
            elif choice == "26":
                if user_client is None:
                    print("❌ No user client available. Please setup a user first (option 23).")
                else:
                    await list_data_references(user_client)
            elif choice == "27":
                if user_client is None:
                    print("❌ No user client available. Please setup a user first (option 23).")
                else:
                    await read_document(user_client)
            elif choice == "28":
                if user_client is None:
                    print("❌ No user client available. Please setup a user first (option 23).")
                else:
                    await delete_document(user_client)
            elif choice == "29":
                if user_client is None:
                    print("❌ No user client available. Please setup a user first (option 23).")
                else:
                    await update_document(user_client)
            elif choice == "30":
                if user_client is None:
                    print("❌ No user client available. Please setup a user first (option 23).")
                else:
                    await grant_access(user_client)
            elif choice == "31":
                if user_client is None:
                    print("❌ No user client available. Please setup a user first (option 23).")
                else:
                    await revoke_access(user_client)
            else:
                print("Invalid choice. Please enter a number between 0 and 31.")

            input("\nPress Enter to continue...")


if __name__ == "__main__":
    asyncio.run(main())
