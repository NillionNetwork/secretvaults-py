"""Test suite for SecretVaultWrapper"""

import base64

import pytest
import jwt
import time
from ecdsa import SigningKey, SECP256k1
from unittest.mock import AsyncMock
from secretvaults import SecretVaultWrapper, OperationType, NilQLWrapper

SEED = "my_seed"


@pytest.fixture
def test_nodes():
    """Returns a mock list of nodes for testing."""
    return [
        {"did": "did:node1", "url": "http://node1.example.com"},
        {"did": "did:node2", "url": "http://node2.example.com"},
    ]


@pytest.fixture
def test_credentials():
    """Returns mock credentials for testing."""
    private_key = SigningKey.generate(curve=SECP256k1)
    secret_key_hex = private_key.to_string().hex()
    return {"org_did": "did:org123", "secret_key": secret_key_hex}


@pytest.fixture
def wrapper(test_nodes, test_credentials):
    """Fixture to create a SecretVaultWrapper instance."""
    return SecretVaultWrapper(
        nodes=test_nodes,
        credentials=test_credentials,
        schema_id="test_schema",
        operation=OperationType.STORE,
    )


def test_initialization(wrapper, test_nodes, test_credentials):
    """Test SecretVaultWrapper initialization with valid parameters."""
    assert wrapper.nodes == test_nodes
    assert wrapper.credentials == test_credentials
    assert wrapper.schema_id == "test_schema"
    assert wrapper.operation == OperationType.STORE
    assert wrapper.token_expiry_seconds == 60  # default


@pytest.mark.asyncio
async def test_generate_node_token(wrapper):
    """Test JWT token generation for a node."""
    await wrapper.init()
    node_did = "did:node1"
    token = await wrapper.generate_node_token(node_did)
    decoded = jwt.decode(token, wrapper.signer.to_pem(), algorithms=["ES256K"], audience=node_did)

    assert decoded["iss"] == wrapper.credentials["org_did"]
    assert decoded["aud"] == node_did
    assert decoded["exp"] > int(time.time())


@pytest.mark.asyncio
async def test_init(wrapper):
    """Test SecretVaultWrapper initialization with NilQLWrapper."""
    nilql_wrapper = await wrapper.init()
    assert wrapper.nilql_wrapper is not None
    assert isinstance(nilql_wrapper, NilQLWrapper)


@pytest.mark.asyncio
async def test_generate_tokens_for_all_nodes(wrapper):
    """Test generating JWT tokens for all nodes."""
    await wrapper.init()
    tokens = await wrapper.generate_tokens_for_all_nodes()

    assert len(tokens) == len(wrapper.nodes)
    for token_entry in tokens:
        assert "node" in token_entry
        assert "token" in token_entry


@pytest.mark.asyncio
async def test_allot_data(wrapper):
    """Test encrypting and transforming data before storage."""
    wrapper.nilql_wrapper = AsyncMock()
    wrapper.nilql_wrapper.prepare_and_allot.return_value = [{"encrypted_data": "mock"}]

    data = [{"field": "sensitive_data"}]
    encrypted_data = await wrapper.allot_data(data)

    assert isinstance(encrypted_data, list)
    assert len(encrypted_data) > 0

    if isinstance(encrypted_data[0], list):
        encrypted_data = encrypted_data[0]

    assert encrypted_data == [{"encrypted_data": "mock"}]


@pytest.mark.asyncio
async def test_flush_data(wrapper):
    """Test flushing data across all nodes."""
    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock(return_value={"status": "flushed"})

    response = await wrapper.flush_data()
    assert isinstance(response, list)
    assert len(response) == len(wrapper.nodes)


@pytest.mark.asyncio
async def test_get_schemas():
    """Test retrieving schemas from the first node."""

    wrapper = SecretVaultWrapper(
        nodes=[{"did": "did:node1", "url": "http://node1.example.com"}],
        credentials={"org_did": "did:org123", "secret_key": "mock_secret"},
        schema_id="test_schema",
    )

    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock(return_value={"schemas": ["schema1", "schema2"]})

    result = await wrapper.get_schemas()

    assert "schemas" in result
    assert isinstance(result["schemas"], list)
    assert len(result["schemas"]) == 2
    assert "schema1" in result["schemas"]

    wrapper.generate_node_token.assert_called_once_with("did:node1")
    wrapper.make_request.assert_called_once_with(
        "http://node1.example.com",
        "schemas",
        "mock_token",
        {},
        method="GET",
    )


@pytest.mark.asyncio
async def test_create_schema(wrapper):
    """Test creating a schema on all nodes."""
    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock()

    schema = {"name": "TestSchema"}
    schema_name = "TestSchema"

    schema_id = await wrapper.create_schema(schema, schema_name)
    assert isinstance(schema_id, str)


@pytest.mark.asyncio
async def test_delete_schema(wrapper):
    """Test deleting a schema from all nodes."""
    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock()

    schema_id = "test_schema"
    await wrapper.delete_schema(schema_id)

    wrapper.make_request.assert_called()


@pytest.fixture
def sample_file(tmp_path):
    # Create a temporary sample file
    file = tmp_path / "sample.txt"
    file.write_text("Hello, world!")
    return str(file)


def test_encode_file_to_str_success(wrapper, sample_file):
    encoded_str = wrapper.encode_file_to_str(sample_file)

    assert isinstance(encoded_str, str)
    assert len(encoded_str) > 0

    # Verify decoding gives the original content
    decoded_content = base64.b64decode(encoded_str).decode("utf-8")
    assert decoded_content == "Hello, world!"


def test_encode_file_to_str_failure(wrapper):
    encoded_str = wrapper.encode_file_to_str("nonexistentfile.txt")

    assert encoded_str == ""


def test_decode_file_from_str_success(wrapper, tmp_path):
    original_content = "Hello, again!"
    encoded_content = base64.b64encode(original_content.encode("utf-8")).decode("utf-8")

    output_file = tmp_path / "output.txt"
    result = wrapper.decode_file_from_str(encoded_content, str(output_file))

    assert result is True
    assert output_file.read_text() == original_content


def test_decode_file_from_str_failure(wrapper):
    invalid_base64 = "!!!!thisisnotbase64!!!!"
    result = wrapper.decode_file_from_str(invalid_base64, "output.txt")

    assert result is False  # Should fail to decode and write


def test_allot_into_chunks_small_data(wrapper):
    small_data = "A" * 1000  # Smaller than 4096
    chunks = wrapper.allot_into_chunks(small_data)

    assert isinstance(chunks, list)
    assert len(chunks) == 1  # Only one list
    assert len(chunks[0]) == 1  # Only one chunk

    assert chunks[0][0]["%allot"] == "A" * 1000


def test_allot_into_chunks_large_data(wrapper):
    # Make data big enough to require multiple partitions
    large_data = "B" * (4096 * 50)  # 50 chunks

    chunks = wrapper.allot_into_chunks(large_data)

    assert isinstance(chunks, list)
    assert all(isinstance(batch, list) for batch in chunks)
    total_chunks = sum(len(batch) for batch in chunks)

    # Should have created 50 small chunks
    assert total_chunks == 50


def test_allot_into_chunks_edge_case_exact_size(wrapper):
    # Exactly matching MAX_RECORD_SIZE_BYTES (assume the constant is 15MB)
    data_size = 15 * 1024 * 1024  # 15MB
    fake_data = "X" * data_size

    chunks = wrapper.allot_into_chunks(fake_data)

    # Should fit into one batch
    assert len(chunks) == 1


@pytest.mark.asyncio
async def test_write_to_nodes(wrapper):
    """Test writing encrypted data to all nodes."""
    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock(return_value={"status": "success"})
    wrapper.allot_data = AsyncMock(return_value=[[{"encrypted_share_1": "data1"}, {"encrypted_share_2": "data2"}]])

    data = [{"field": "sensitive_data"}]

    results = await wrapper.write_to_nodes(data)

    assert isinstance(results, list)
    assert len(results) == len(wrapper.nodes)
    assert results[0]["result"] == {"status": "success"}


@pytest.mark.asyncio
async def test_read_from_nodes(wrapper):
    """Test reading data from all nodes."""
    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock(return_value={"data": [{"_id": "123", "value": "mock"}]})
    wrapper.nilql_wrapper = AsyncMock()
    wrapper.nilql_wrapper.unify.return_value = {"_id": "123", "value": "mock"}

    data = await wrapper.read_from_nodes()
    assert len(data) == 1
    assert data[0]["_id"] == "123"


@pytest.mark.asyncio
async def test_update_data_to_nodes(wrapper):
    """Test updating data across all nodes."""
    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock(return_value={"status": "updated"})
    wrapper.allot_data = AsyncMock(
        return_value=[[{"encrypted_update_1": "new_value"}, {"encrypted_update_2": "new_value"}]]
    )

    update_data = {"status": "inactive"}
    data_filter = {"_id": "12345"}

    results = await wrapper.update_data_to_nodes(update_data, data_filter)

    assert isinstance(results, list)
    assert len(results) == len(wrapper.nodes)
    assert results[0]["result"] == {"status": "updated"}


@pytest.mark.asyncio
async def test_delete_data_from_nodes_with_filter():
    """Test data deletion using a filter."""
    wrapper = SecretVaultWrapper(
        nodes=[{"did": "did:node1", "url": "http://node1.example.com"}],
        credentials={"org_did": "did:org123", "secret_key": "mock_secret"},
        schema_id="test_schema",
    )

    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock(return_value={"status": "deleted"})

    data_filter = {"_id": "12345"}

    results = await wrapper.delete_data_from_nodes(data_filter)

    assert len(results) == 1
    assert results[0]["result"] == {"status": "deleted"}

    wrapper.make_request.assert_called_once_with(
        "http://node1.example.com",
        "data/delete",
        "mock_token",
        {"schema": "test_schema", "filter": data_filter},
    )


@pytest.mark.asyncio
async def test_get_queries(wrapper):
    """Test retrieving queries from the first node."""
    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock(return_value={"queries": ["query1", "query2"]})

    result = await wrapper.get_queries()

    assert "queries" in result
    assert isinstance(result["queries"], list)
    assert len(result["queries"]) == 2


@pytest.mark.asyncio
async def test_create_query():
    """Test creating a query on all nodes."""

    wrapper = SecretVaultWrapper(
        nodes=[{"did": "did:node1", "url": "http://node1.example.com"}],
        credentials={"org_did": "did:org123", "secret_key": "mock_secret"},
        schema_id="test_schema",
    )

    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock()

    query = {"variables": {"var1": "value1"}, "pipeline": [{"stage": "match", "filter": {}}]}
    query_name = "TestQuery"
    schema_id = "test_schema"

    query_id = await wrapper.create_query(query, schema_id, query_name)

    assert isinstance(query_id, str)
    assert len(query_id) > 0

    wrapper.make_request.assert_called_once_with(
        "http://node1.example.com",
        "queries",
        "mock_token",
        {
            "_id": query_id,
            "name": query_name,
            "schema": schema_id,
            "variables": query["variables"],
            "pipeline": query["pipeline"],
        },
    )


@pytest.mark.asyncio
async def test_delete_query():
    """Test deleting a query from all nodes."""

    wrapper = SecretVaultWrapper(
        nodes=[{"did": "did:node1", "url": "http://node1.example.com"}],
        credentials={"org_did": "did:org123", "secret_key": "mock_secret"},
        schema_id="test_schema",
    )

    wrapper.generate_node_token = AsyncMock(return_value="mock_token")
    wrapper.make_request = AsyncMock()

    query_id = "test_query"

    await wrapper.delete_query(query_id)

    wrapper.make_request.assert_called_once_with(
        "http://node1.example.com", "queries", "mock_token", {"id": query_id}, method="DELETE"
    )


@pytest.mark.asyncio
async def test_query_execute_on_nodes():
    """Test executing a query on all nodes and unifying the results."""

    wrapper = SecretVaultWrapper(
        nodes=[
            {"did": "did:node1", "url": "http://node1.example.com"},
            {"did": "did:node2", "url": "http://node2.example.com"},
        ],
        credentials={"org_did": "did:org123", "secret_key": "mock_secret"},
        schema_id="test_schema",
    )

    wrapper.generate_node_token = AsyncMock(return_value="mock_token")

    wrapper.make_request = AsyncMock(
        side_effect=[
            {"data": [{"_id": "123", "value": "node1_result"}]},
            {"data": [{"_id": "123", "value": "node2_result"}]},
        ]
    )

    wrapper.nilql_wrapper = AsyncMock()
    wrapper.nilql_wrapper.unify.return_value = {"_id": "123", "value": "final_result"}

    query_payload = {"query_id": "test_query"}
    result = await wrapper.query_execute_on_nodes(query_payload)

    assert isinstance(result, list)
    assert len(result) == 1
    assert result[0]["_id"] == "123"
    assert result[0]["value"] == "final_result"

    assert wrapper.make_request.call_count == len(wrapper.nodes)
    wrapper.nilql_wrapper.unify.assert_called_once()
