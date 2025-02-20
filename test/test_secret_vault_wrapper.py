import pytest
from unittest import TestCase
from unittest.mock import AsyncMock, patch
import jwt
import time
from ecdsa import SigningKey, SECP256k1
from secretvaults import SecretVaultWrapper, OperationType

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


class TestSecretVaultWrapper(TestCase):
    """
    Test suite for SecretVaultWrapper functionalities.
    """

    def setUp(self):
        """Setup method to initialize SecretVaultWrapper instances before each test."""
        self.nodes = [
            {"did": "did:node1", "url": "http://node1.example.com"},
            {"did": "did:node2", "url": "http://node2.example.com"},
        ]
        self.credentials = {
            "org_did": "did:org123",
            "secret_key": SigningKey.generate(curve=SECP256k1).to_string().hex(),
        }
        self.wrapper = SecretVaultWrapper(
            nodes=self.nodes,
            credentials=self.credentials,
            schema_id="test_schema",
            operation=OperationType.STORE.value,
        )

    def test_initialization(self):
        """Test SecretVaultWrapper initialization with valid parameters."""
        assert self.wrapper.nodes == self.nodes
        assert self.wrapper.credentials == self.credentials
        assert self.wrapper.schema_id == "test_schema"
        assert self.wrapper.operation == OperationType.STORE.value
        assert self.wrapper.token_expiry_seconds == 60  # Default expiration time

    @pytest.mark.asyncio
    async def test_generate_node_token(self):
        """Test JWT token generation for a node."""
        node_did = "did:node1"
        token = await self.wrapper.generate_node_token(node_did)
        decoded = jwt.decode(token, self.wrapper.signer.to_pem(), algorithms=["ES256K"], audience=node_did)

        assert decoded["iss"] == self.credentials["org_did"]
        assert decoded["aud"] == node_did
        assert decoded["exp"] > int(time.time())  # Token should be valid in the future

    @pytest.mark.asyncio
    async def test_generate_tokens_for_all_nodes(self):
        """Test generating JWT tokens for all nodes."""
        tokens = await self.wrapper.generate_tokens_for_all_nodes()
        assert len(tokens) == len(self.nodes)
        for token_entry in tokens:
            assert "node" in token_entry
            assert "token" in token_entry

    @pytest.mark.asyncio
    async def test_init(self):
        """Test SecretVaultWrapper initialization with NilQLWrapper."""
        nilql_wrapper = await self.wrapper.init()
        assert self.wrapper.nilql_wrapper is not None
        assert isinstance(nilql_wrapper, SecretVaultWrapper)

    @pytest.mark.asyncio
    async def test_make_request_success(self):
        """Test making a successful HTTP request using aiohttp."""
        with patch("aiohttp.ClientSession.request", new_callable=AsyncMock) as mock_request:
            mock_response = AsyncMock()
            mock_response.status = 200
            mock_response.json.return_value = {"success": True}
            mock_request.return_value.__aenter__.return_value = mock_response

            response = await self.wrapper.make_request(
                "http://node1.example.com", "schemas", "mock_token", {}, method="GET"
            )

            assert response == {"success": True}

    @pytest.mark.asyncio
    async def test_make_request_failure(self):
        """Test handling an HTTP request failure."""
        with patch("aiohttp.ClientSession.request", new_callable=AsyncMock) as mock_request:
            mock_response = AsyncMock()
            mock_response.status = 500
            mock_response.text.return_value = "Internal Server Error"
            mock_request.return_value.__aenter__.return_value = mock_response

            with pytest.raises(ConnectionError, match="Error: 500"):
                await self.wrapper.make_request(
                    "http://node1.example.com",
                    "schemas",
                    "mock_token",
                    {},
                    method="GET",
                )

    @pytest.mark.asyncio
    async def test_allot_data(self):
        """Test encrypting and transforming data before storage."""
        self.wrapper.nilql_wrapper = AsyncMock()
        self.wrapper.nilql_wrapper.prepare_and_allot.return_value = [{"encrypted_data": "mock"}]

        data = [{"field": "sensitive_data"}]
        encrypted_data = await self.wrapper.allot_data(data)

        assert isinstance(encrypted_data, list)
        assert len(encrypted_data) == 1
        assert "encrypted_data" in encrypted_data[0]

    @pytest.mark.asyncio
    async def test_flush_data(self):
        """Test flushing data across all nodes."""
        self.wrapper.generate_node_token = AsyncMock(return_value="mock_token")
        self.wrapper.make_request = AsyncMock(return_value={"status": "flushed"})

        response = await self.wrapper.flush_data()
        assert isinstance(response, list)
        assert len(response) == len(self.nodes)

    @pytest.mark.asyncio
    async def test_create_schema(self):
        """Test creating a schema on all nodes."""
        self.wrapper.generate_node_token = AsyncMock(return_value="mock_token")
        self.wrapper.make_request = AsyncMock()

        schema = {"name": "TestSchema"}
        schema_name = "TestSchema"

        schema_id = await self.wrapper.create_schema(schema, schema_name)
        assert isinstance(schema_id, str)

    @pytest.mark.asyncio
    async def test_delete_schema(self):
        """Test deleting a schema from all nodes."""
        self.wrapper.generate_node_token = AsyncMock(return_value="mock_token")
        self.wrapper.make_request = AsyncMock()

        schema_id = "test_schema"
        await self.wrapper.delete_schema(schema_id)

        self.wrapper.make_request.assert_called()

    @pytest.mark.asyncio
    async def test_write_to_nodes(self):
        """Test writing encrypted data to all nodes."""
        self.generate_node_token = AsyncMock(return_value="mock_token")
        self.make_request = AsyncMock(return_value={"status": "success"})
        self.allot_data = AsyncMock(return_value=[[{"encrypted_share_1": "data1"}, {"encrypted_share_2": "data2"}]])

        data = [{"field": "sensitive_data"}]

        results = await self.write_to_nodes(data)

        assert isinstance(results, list)
        assert len(results) == len(self.nodes)
        assert results[0]["result"] == {"status": "success"}

        self.make_request.assert_called()

    @pytest.mark.asyncio
    async def test_write_to_nodes_with_missing_id(self):
        """Test writing data without an `_id`, ensuring it generates one."""
        self.generate_node_token = AsyncMock(return_value="mock_token")
        self.make_request = AsyncMock(return_value={"status": "success"})
        self.allot_data = AsyncMock(return_value=[[{"encrypted_share_1": "data1"}, {"encrypted_share_2": "data2"}]])

        data = [{"field": "sensitive_data"}]  # No _id provided

        results = await self.write_to_nodes(data)

        assert "_id" in data[0] or results
        assert isinstance(results, list)
        assert results[0]["result"] == {"status": "success"}

    @pytest.mark.asyncio
    async def test_read_from_nodes(self):
        """Test reading data from all nodes."""
        self.wrapper.generate_node_token = AsyncMock(return_value="mock_token")
        self.wrapper.make_request = AsyncMock(return_value={"data": [{"_id": "123", "value": "mock"}]})
        self.wrapper.nilql_wrapper = AsyncMock()
        self.wrapper.nilql_wrapper.unify.return_value = {"_id": "123", "value": "mock"}

        data = await self.wrapper.read_from_nodes()
        assert len(data) == 1
        assert data[0]["_id"] == "123"

    @pytest.mark.asyncio
    async def test_update_data_to_nodes(self):
        """Test updating data across all nodes."""
        self.generate_node_token = AsyncMock(return_value="mock_token")
        self.make_request = AsyncMock(return_value={"status": "updated"})
        self.allot_data = AsyncMock(
            return_value=[[{"encrypted_update_1": "new_value"}, {"encrypted_update_2": "new_value"}]]
        )

        update_data = {"status": "inactive"}
        data_filter = {"_id": "12345"}

        results = await self.update_data_to_nodes(update_data, data_filter)

        assert isinstance(results, list)
        assert len(results) == len(self.nodes)
        assert results[0]["result"] == {"status": "updated"}

        self.make_request.assert_called()

    @pytest.mark.asyncio
    async def test_update_data_to_nodes_no_filter(self):
        """Test updating data without a filter, ensuring it applies to all."""
        self.generate_node_token = AsyncMock(return_value="mock_token")
        self.make_request = AsyncMock(return_value={"status": "updated"})
        self.allot_data = AsyncMock(
            return_value=[[{"encrypted_update_1": "new_value"}, {"encrypted_update_2": "new_value"}]]
        )

        update_data = {"status": "inactive"}

        results = await self.update_data_to_nodes(update_data)

        assert isinstance(results, list)
        assert len(results) == len(self.nodes)
        assert results[0]["result"] == {"status": "updated"}

        self.make_request.assert_called()

    @pytest.mark.asyncio
    async def test_get_queries(self):
        """Test retrieving queries from the first node."""
        self.generate_node_token = AsyncMock(return_value="mock_token")
        self.make_request = AsyncMock(return_value={"queries": ["query1", "query2"]})

        result = await self.get_queries()

        assert "queries" in result
        assert isinstance(result["queries"], list)
        assert len(result["queries"]) == 2
        self.make_request.assert_called_once_with(
            "http://node1.example.com",
            "queries",
            "mock_token",
            {},
            method="GET",
        )

    @pytest.mark.asyncio
    async def test_create_query(self):
        """Test creating a query on all nodes."""
        self.generate_node_token = AsyncMock(return_value="mock_token")
        self.make_request = AsyncMock()

        query = {"variables": {"var1": "value1"}, "pipeline": [{"stage": "match", "filter": {}}]}
        schema_id = "test_schema"
        query_name = "TestQuery"

        query_id = await self.create_query(query, schema_id, query_name)

        assert isinstance(query_id, str)  # Should return a valid UUID
        assert len(query_id) > 0
        self.make_request.assert_called()

    @pytest.mark.asyncio
    async def test_delete_query(self):
        """Test deleting a query from all nodes."""
        self.generate_node_token = AsyncMock(return_value="mock_token")
        self.make_request = AsyncMock()

        query_id = "test_query"
        await self.delete_query(query_id)

        assert self.make_request.call_count == len(self.nodes)
        self.make_request.assert_called_with(
            "http://node2.example.com", "queries", "mock_token", {"id": query_id}, method="DELETE"
        )

    @pytest.mark.asyncio
    async def test_query_execute_on_nodes(self):
        """Test executing a query on all nodes and unifying the results."""
        self.generate_node_token = AsyncMock(return_value="mock_token")
        self.make_request = AsyncMock(
            side_effect=[
                {"data": [{"_id": "123", "value": "node1_result"}]},
                {"data": [{"_id": "123", "value": "node2_result"}]},
            ]
        )
        self.nilql_wrapper = AsyncMock()
        self.nilql_wrapper.unify.return_value = {"_id": "123", "value": "final_result"}

        query_payload = {"query_id": "test_query"}

        result = await self.query_execute_on_nodes(query_payload)

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["_id"] == "123"
        assert result[0]["value"] == "final_result"
        assert self.make_request.call_count == len(self.nodes)
        self.nilql_wrapper.unify.assert_called_once()
