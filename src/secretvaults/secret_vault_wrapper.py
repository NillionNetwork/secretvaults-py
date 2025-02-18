"""SecretVaultWrapper manages distributed data storage across multiple nodes"""

import asyncio
import uuid
import time
from http import HTTPMethod
from typing import List, Dict, Any, Union

import aiohttp
import jwt
from ecdsa import SigningKey, SECP256k1

from .nilql_wrapper import NilQLWrapper, OperationType


# pylint: disable=too-many-instance-attributes
class SecretVaultWrapper:
    """
    SecretVaultWrapper manages distributed data storage across multiple nodes.
    It handles node authentication, data distribution, and uses NilQLWrapper
    for field-level encryption. Provides CRUD operations with built-in
    security and error handling.

    Attributes:
        nodes (List[Dict[str, str]]): Node-related information.
        credentials (Dict[str, str]): Authentication credentials.
        schema_id (Optional[str]): An optional identifier for the schema being used.
        operation (str): The operation type for the data to be encrypted (default is "store").
        token_expiry_seconds (int): Expiration time for the authentication token, in seconds.
    """

    def __init__(
        self,
        nodes: List[Dict[str, str]],
        credentials: Dict[str, str],
        schema_id: str = None,
        operation: str = OperationType.STORE.value,
        token_expiry_seconds: int = 60,
    ):
        self.nodes = nodes
        self.nodes_jwt = None
        self.credentials = credentials
        self.schema_id = schema_id
        self.operation = operation
        self.token_expiry_seconds = token_expiry_seconds
        self.nilql_wrapper = None
        self.signer = None

    async def init(self) -> NilQLWrapper:
        """
        Initializes the SecretVaultWrapper:
         - Generates tokens for nodes.
         - Instantiates and initializes NilQLWrapper with cluster configuration.

        Returns:
            NilQLWrapper: The initialized NilQLWrapper instance, configured with the cluster information
                          and operation type, ready for use in encryption/decryption tasks.

        Raises:
            KeyError: If the required secret key is not found in the credentials.
            Exception: If there is an issue generating the node tokens or initializing the NilQLWrapper.

        Example:
            await org.init()
        """
        # Convert the secret key from hex to bytes
        private_key = bytes.fromhex(self.credentials["secret_key"])
        self.signer = SigningKey.from_string(private_key, curve=SECP256k1)

        node_configs = []
        for node in self.nodes:
            token = await self.generate_node_token(node["did"])
            node_configs.append({"url": node["url"], "jwt": token})
        self.nodes_jwt = node_configs

        # Initiate the NilQLWrapper
        self.nilql_wrapper = NilQLWrapper({"nodes": self.nodes}, self.operation)
        return self.nilql_wrapper

    async def generate_node_token(self, node_did: str) -> str:
        """
        Generates a JWT token for node authentication using ES256K.

        Args:
            node_did (str): The decentralized identifier (DID) of the node for which the token is generated.

        Returns:
            str: The generated JWT token as a string, signed using the ES256K algorithm.

        Raises:
            ValueError: If the `node_did` or `signer` is invalid or missing the required fields.

        Example:
            token = await wrapper.generate_node_token(node_did="did:node123", signer=signer)
            # The token can then be used to authenticate the node in subsequent operations.
        """

        payload = {
            "iss": self.credentials["org_did"],
            "aud": node_did,
            "exp": int(time.time()) + self.token_expiry_seconds,
        }

        # Create and sign the JWT
        token = jwt.encode(payload, self.signer.to_pem(), algorithm="ES256K")

        return token

    async def generate_tokens_for_all_nodes(self) -> List[Dict[str, str]]:
        """
        Generates tokens for all nodes.

        Returns:
            List[Dict[str, str]]: A list of dictionaries, where each dictionary contains:
                                  - "node" (str): The URL of the node.
                                  - "token" (str): The generated JWT token for the node.

        Raises:
            Exception: If there is an issue generating the tokens, such as missing or invalid credentials.

        Example:
            tokens = await org.generate_tokens_for_all_nodes()
            # tokens = [{"node": "http://node1.nillion.network", "token": "jwt_token_1"}, ...]
        """
        tokens = []
        for node in self.nodes:
            token = await self.generate_node_token(node["did"])
            tokens.append({"node": node["url"], "token": token})
        return tokens

    @staticmethod
    async def make_request(
        node_url: str,
        endpoint: str,
        token: str,
        payload: Dict[str, Any],
        method: Union[HTTPMethod.POST, HTTPMethod.GET, HTTPMethod.DELETE] = HTTPMethod.POST,
    ) -> Dict[str, Any]:
        """
        Makes an HTTP request to the node endpoint using aiohttp.

        Args:
            node_url (str): The base URL of the node.
            endpoint (str): The API endpoint to call.
            token (str): The authorization token for the request.
            payload (Dict[str, Any]): The data to be sent in the request body.
            method (Union[HTTPMethod.POST, HTTPMethod.GET]): The HTTP method to use. Default is "POST".

        Returns:
            Dict[str, Any]: The response data parsed as JSON.

        Raises:
            ConnectionError: If there is an error with the network connection or server.
        """
        url = f"{node_url}/api/v1/{endpoint}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

        async with aiohttp.ClientSession() as session:
            try:
                async with session.request(
                    method,
                    url,
                    headers=headers,
                    json=payload if method != HTTPMethod.GET else None,
                    params=payload if method == HTTPMethod.GET else None,
                ) as response:
                    if response.status >= 300:
                        raise ConnectionError(f"Error: {response.status}, body: {await response.text()}")
                    # Check if response contains a body
                    if response.content_type and "application/json" in response.content_type.lower():
                        return await response.json()
                    return {}

            except aiohttp.ClientConnectionError as e:
                raise ConnectionError(f"Connection error: {str(e)}") from e
            except aiohttp.ClientError as e:
                raise ConnectionError(f"Request failed: {str(e)}") from e

    async def allot_data(self, data: List[Dict[str, Any]]) -> List[Any]:
        """
        Transforms data by encrypting specified fields across all nodes

        Args:
            data (List[Dict[str, Any]]): The data to be checked for allotment.

        Returns:
            List[Any]: A list of transformed (e.g., encrypted) data items, ready for distribution.

        Raises:
            RuntimeError: If the `NilQLWrapper` has not been initialized or if there is any error
                          during the transformation or encryption process.

        Example:
            encrypted_data = await wrapper.allot_data(records)
        """
        encrypted_records = []
        for item in data:
            encrypted_item = await self.nilql_wrapper.prepare_and_allot(item)
            encrypted_records.append(encrypted_item)
        return encrypted_records

    async def flush_data(self) -> List[Dict[str, Any]]:
        """
        Clears data from all nodes for the current schema, executing all requests in parallel.

        Returns:
            List[Dict[str, Any]]: A list of results from each node, containing the node URL and
                                   the result of the flush operation.

        Example:
            results = await wrapper.flush_data()
        """

        async def flush_node(node: Dict[str, str]) -> Dict[str, Any]:
            jwt_token = await self.generate_node_token(node["did"])
            payload = {"schema": self.schema_id}
            result = await self.make_request(
                node["url"],
                "data/flush",
                jwt_token,
                payload,
            )
            return {"node": node["url"], "result": result}

        # Gather tasks for all nodes and execute them in parallel
        tasks = [flush_node(node) for node in self.nodes]
        results = await asyncio.gather(*tasks)

        return results

    async def get_schemas(self) -> Dict[str, Any]:
        """
        Lists schema from the first node (as there is parity between them).

        Returns:
            Dict[str, Any]: A dictionary containing the response data from the "schemas"
                            endpoint of the first node, which typically includes a list of
                            schemas or other related data.

        Raises:
            Exception: If there is an issue generating the JWT token or making the request
                       (e.g., connection issues, invalid response, etc.).

        Example:
            schemas = await wrapper.get_schemas()
        """
        jwt_token = await self.generate_node_token(self.nodes[0]["did"])
        result = await self.make_request(
            self.nodes[0]["url"],
            "schemas",
            jwt_token,
            {},
            method=HTTPMethod.GET,
        )

        return result

    async def create_schema(self, schema: Dict[str, Any], schema_name: str, schema_id: str = None) -> str:
        """
        Creates a new schema on all nodes in the cluster concurrently.

        Args:
            schema (Dict[str, Any]): The schema definition to be created.
            schema_name (str): The name of the schema to be created.
            schema_id (str, optional): A custom schema ID. If not provided, a new UUID is generated.

        Returns:
            str: The schema id..

        Raises:
            Exception: If there is an issue generating the JWT token or making the request.

        Example:
            results = await wrapper.create_schema(schema, "NewSchema")
        """
        if not schema_id:
            schema_id = str(uuid.uuid4())  # Generate a new schema ID if not provided

        # Construct the payload for schema creation
        schema_payload = {
            "_id": schema_id,
            "name": schema_name,
            "keys": ["_id"],  # _id is a required field for indexing
            "schema": schema,  # The actual schema definition
        }

        # Define an async function to handle the request for a single node
        async def create_schema_for_node(node: Dict[str, str]) -> None:
            jwt_token = await self.generate_node_token(node["did"])  # Generate token for the node
            await self.make_request(
                node["url"],  # Node URL
                "schemas",  # Endpoint for schema creation
                jwt_token,  # JWT token for authentication
                schema_payload,  # The schema creation payload
            )

        # Gather tasks for all nodes and execute them in parallel
        tasks = [create_schema_for_node(node) for node in self.nodes]
        await asyncio.gather(*tasks)

        return schema_id

    async def delete_schema(self, schema_id: str) -> None:
        """
        Removes a schema from all nodes in the cluster concurrently.

        Args:
            schema_id (str): The ID of the schema to be deleted from all nodes.

        Raises:
            Exception: If there is an issue generating the JWT token or making the request.

        Example:
            results = await wrapper.delete_schema("XXXXXXXX")
        """

        # Define an async function to handle the request for a single node
        async def delete_schema_from_node(node: Dict[str, str]) -> None:
            jwt_token = await self.generate_node_token(node["did"])  # Generate token for the node
            await self.make_request(
                node["url"],  # Node URL
                "schemas",  # Endpoint for schema deletion
                jwt_token,  # JWT token for authentication
                {"id": schema_id},  # Schema ID to delete
                method=HTTPMethod.DELETE,  # HTTP method for deletion
            )

        # Gather tasks for all nodes and execute them in parallel
        tasks = [delete_schema_from_node(node) for node in self.nodes]
        await asyncio.gather(*tasks)

    async def write_to_nodes(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Writes data to all nodes, applying field encryption if necessary.

        Args:
            data (List[Dict[str, Any]]): A list of records to be written to the nodes.

        Returns:
            List[Dict[str, Any]]: The response from the nodes.

        Raises:
            Exception: If there's an error while preparing the data, generating tokens, or writing to any node.

        Example:
            results = await wrapper.write_to_nodes(data)
        """
        # Adds an _id field to each record if it doesn't exist
        id_data = []
        for record in data:
            if "_id" not in record:
                new_record = record.copy()
                new_record["_id"] = str(uuid.uuid4())  # Generate a new unique ID
                id_data.append(new_record)
            else:
                id_data.append(record)

        # Encrypts and transforms the data before sending it to the nodes
        transformed_data = await self.allot_data(id_data)

        # Define the async function to handle writing to a single node
        async def write_to_node(i: int, node: Dict[str, str]) -> Dict[str, Any]:
            try:
                # Prepare the data for this specific node
                node_data = []
                for encrypted_shares in transformed_data:
                    if len(encrypted_shares) != len(self.nodes):
                        node_data.append(encrypted_shares[0])
                    else:
                        node_data.append(encrypted_shares[i])

                jwt_token = await self.generate_node_token(node["did"])
                payload = {
                    "schema": self.schema_id,
                    "data": node_data,
                }
                result = await self.make_request(
                    node["url"],
                    "data/create",
                    jwt_token,
                    payload,
                )
                return {"node": node["url"], "result": result}
            except RuntimeError as e:
                print(f"❌ Failed to write to {node['url']}: {str(e)}")
                return {"node": node["url"], "error": str(e)}

        # Use asyncio.gather to run all node writes concurrently
        tasks = [write_to_node(i, node) for i, node in enumerate(self.nodes)]
        results = await asyncio.gather(*tasks)

        return results

    async def read_from_nodes(self, data_filter: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Reads data from all nodes and then recombines the shares to form the original records.

        Args:
            data_filter (Dict[str, Any]): A filter to apply when reading data.

        Returns:
            List[Dict[str, Any]]: A list of recombined records. Each record is the result of combining shares from
                                   different nodes, forming a complete record.

        Example:
            records = await wrapper.read_from_nodes({"_id": "XXXXXXXXXXXXXXXXXXX"})
        """

        # Function to read from each node
        async def read_from_node(node: Dict[str, str]) -> Dict[str, Any]:
            try:
                jwt_token = await self.generate_node_token(node["did"])
                payload = {
                    "schema": self.schema_id,
                    "filter": data_filter or {},
                }
                result = await self.make_request(
                    node["url"],
                    "data/read",
                    jwt_token,
                    payload,
                )
                return {"node": node["url"], "data": result.get("data", [])}
            except RuntimeError as e:
                print(f"❌ Failed to read from {node['url']}: {str(e)}")
                return {"node": node["url"], "error": str(e)}

        # Run the node reading tasks in parallel using asyncio.gather
        tasks = [read_from_node(node) for node in self.nodes]
        results_from_all_nodes = await asyncio.gather(*tasks)

        # Groups records from different nodes by _id field
        record_groups = []
        for node_result in results_from_all_nodes:
            for record in node_result.get("data", []):
                # Find a group that already contains a record with the same _id
                group = next(
                    (g for g in record_groups if any(share.get("_id") == record.get("_id") for share in g["shares"])),
                    None,
                )
                if group:
                    group["shares"].append(record)
                else:
                    record_groups.append({"shares": [record], "record_index": record.get("_id")})

        # Recombine the shares to form the original records
        recombined_records = []
        for group in record_groups:
            recombined = await self.nilql_wrapper.unify(group["shares"])
            recombined_records.append(recombined)

        return recombined_records

    async def update_data_to_nodes(
        self, record_update: Dict[str, Any], data_filter: Dict[str, Any] = None
    ) -> List[Dict[str, Any]]:
        """
        Update data on all nodes, while also optionally encrypting them and applying a filter.

        Args:
            record_update (Dict[str, Any]): The record to be updated.
            data_filter (Dict[str, Any], optional): A filter to apply when updating data.

        Returns:
            List[Dict[str, Any]]: A list of results from each node.

        Example:
            update_results = await wrapper.update_data_to_nodes(
                record_update={"status": "inactive"},
                data_filter={"_id": "XXXXXXXXXXXX"}
            )
        """

        # Transform the record using the allotData method
        transformed_data = await self.allot_data([record_update])

        # Function to update data on a single node
        async def update_node(i: int, node: Dict[str, str], node_data: list) -> Dict[str, Any]:
            try:
                # Map the encrypted shares to the correct node's data
                node_data = node_data[i] if len(node_data) == len(self.nodes) else node_data[0]

                # Generate the JWT token for the node
                jwt_token = await self.generate_node_token(node["did"])

                # Prepare the payload for the update request
                payload = {
                    "schema": self.schema_id,
                    "update": {"$set": node_data},
                    "filter": data_filter or {},
                }

                # Make the request to the node's update endpoint
                result = await self.make_request(
                    node["url"],
                    "data/update",
                    jwt_token,
                    payload,
                )
                return {"node": node["url"], "result": result}

            except RuntimeError as e:
                print(f"❌ Failed to write to {node['url']}: {str(e)}")
                return {"node": node["url"], "error": str(e)}

        # Run the node update tasks in parallel using asyncio.gather
        tasks = [update_node(i, node, transformed_data[0]) for i, node in enumerate(self.nodes)]
        results = await asyncio.gather(*tasks)

        return results

    async def delete_data_from_nodes(self, data_filter: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Deletes data from all nodes based on the provided filter.

        Args:
            data_filter (Dict[str, Any], optional): The filter to apply when deleting data.

        Returns:
            List[Dict[str, Any]]: A list of results from each node.

        Example:
            delete_results = await wrapper.delete_data_from_nodes(
                data_filter={"_id": "XXXXXXXXXXXXXXXXXXXXXXxx"}
            )
        """

        # Function to delete data from a single node
        async def delete_node(node: Dict[str, str]) -> Dict[str, Any]:
            try:
                # Generate the JWT token for the node
                jwt_token = await self.generate_node_token(node["did"])

                # Prepare the payload for the delete request
                payload = {
                    "schema": self.schema_id,
                    "filter": data_filter or {},
                }
                # Make the request to the node's delete endpoint
                result = await self.make_request(
                    node["url"],
                    "data/delete",
                    jwt_token,
                    payload,
                )
                return {"node": node["url"], "result": result}

            except RuntimeError as e:
                print(f"❌ Failed to delete from {node['url']}: {str(e)}")
                return {"node": node["url"], "error": str(e)}

        # Run the node delete tasks in parallel using asyncio.gather
        tasks = [delete_node(node) for node in self.nodes]
        results = await asyncio.gather(*tasks)

        return results

    async def get_queries(self) -> Dict[str, Any]:
        """
        Lists queries from the first node (as there is parity between them).

        Returns:
            Dict[str, Any]: A dictionary containing the response data from the "queries"
                            endpoint of the first node, which typically includes a list of
                            queries or other related data.

        Raises:
            Exception: If there is an issue generating the JWT token or making the request.

        Example:
            queries = await wrapper.get_queries()
        """
        # Generate a token for the first node
        jwt_token = await self.generate_node_token(self.nodes[0]["did"])

        # Make a request to the queries endpoint of the first node
        result = await self.make_request(
            self.nodes[0]["url"],
            "queries",
            jwt_token,
            {},
            method=HTTPMethod.GET,
        )

        return result

    async def create_query(self, query: Dict[str, Any], schema_id: str, query_name: str, query_id: str = None) -> str:
        """
        Creates a new query on all nodes in the cluster concurrently.

        Args:
            query (Dict[str, Any]): The query definition to be created.
            query_name (str): The name of the query to be created.
            schema_id (str): The schema_id the query will be based on.
            query_id (str, optional): A custom query ID. If not provided, a new UUID is generated.

        Returns:
            str: The created schema id.

        Raises:
            Exception: If there is an issue generating the JWT token or making the request.

        Example:
            results = await wrapper.create_query(query, "schemaXXXXXXXXX", "NewQuery")
        """
        if not query_id:
            query_id = str(uuid.uuid4())  # Generate a new query ID if not provided

        # Construct the payload for query creation
        query_payload = {
            "_id": query_id,
            "name": query_name,
            "schema": schema_id,
            "variables": query["variables"],
            "pipeline": query["pipeline"],
        }

        # Define an async function to handle the request for a single node
        async def create_query_for_node(node: Dict[str, str]) -> None:
            jwt_token = await self.generate_node_token(node["did"])  # Generate token for the node
            await self.make_request(
                node["url"],
                "queries",
                jwt_token,
                query_payload,
            )

        # Gather tasks for all nodes and execute them in parallel
        tasks = [create_query_for_node(node) for node in self.nodes]
        await asyncio.gather(*tasks)

        return query_id

    async def delete_query(self, query_id: str) -> None:
        """
        Removes a query from all nodes in the cluster concurrently.

        Args:
            query_id (str): The ID of the query to be deleted from all nodes.

        Raises:
            Exception: If there is an issue generating the JWT token or making the request.

        Example:
            results = await wrapper.delete_query("XXXXXXXX")
        """

        # Define an async function to handle the request for a single node
        async def delete_query_from_node(node: Dict[str, str]) -> None:
            jwt_token = await self.generate_node_token(node["did"])  # Generate token for the node
            await self.make_request(
                node["url"],
                "queries",
                jwt_token,
                {"id": query_id},
                method=HTTPMethod.DELETE,
            )

        # Gather tasks for all nodes and execute them in parallel
        tasks = [delete_query_from_node(node) for node in self.nodes]
        await asyncio.gather(*tasks)

    async def query_execute_on_nodes(self, query_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Executes a query on all nodes and unifies the results.

        Args:
            query_payload (Dict[str, Any]): The query payload to execute.

        Returns:
            List[Dict[str, Any]]: A list of unified records resulting from executing the query across all nodes.
        """

        async def execute_query_on_node(node: Dict[str, str]) -> Dict[str, Any]:
            try:
                jwt_token = await self.generate_node_token(node["did"])
                result = await self.make_request(
                    node["url"],
                    "queries/execute",
                    jwt_token,
                    query_payload,
                )
                return {
                    "node": node["url"],
                    "data": result.get("data", []),
                }
            except RuntimeError as e:
                print(f"❌ Failed to execute query on {node['url']}: {str(e)}")
                return {"node": node["url"], "error": str(e)}

        # Execute queries in parallel on all nodes
        tasks = [execute_query_on_node(node) for node in self.nodes]
        results_from_all_nodes = await asyncio.gather(*tasks)

        # Collect all data from nodes
        all_shares = []
        for node_result in results_from_all_nodes:
            if "data" in node_result:
                all_shares.extend(node_result["data"])

        # Unify the results directly
        recombined_result = await self.nilql_wrapper.unify(all_shares)

        return recombined_result
