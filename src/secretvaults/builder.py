"""
SecretVault builder client for managing SecretVaults with automatic handling of concealed data.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from nuc.token import Did
from nuc.nilauth import NilauthClient
from nuc.builder import NucTokenEnvelope, NucTokenBuilder
from nuc.token import Command, InvocationBody

from .base import SecretVaultBaseClient, SecretVaultBaseOptions
from .common.blindfold import BlindfoldFactoryConfig, to_blindfold_key
from .common.keypair import Keypair
from .common.utils import into_seconds_from_now, inject_ids_into_records
from .common.cluster import (
    execute_on_cluster,
    prepare_concealed_request,
    prepare_plaintext_request,
    process_concealed_list_response,
    process_plaintext_response,
)
from .common.nuc_cmd import NucCmd
from .dto.builders import (
    ReadBuilderProfileResponse,
    RegisterBuilderRequest,
    UpdateBuilderProfileRequest,
)
from .dto.collections import (
    CreateCollectionIndexRequest,
    CreateCollectionRequest,
    ListCollectionsResponse,
    ReadCollectionMetadataResponse,
)
from .dto.data import (
    CreateDataResponse,
    CreateStandardDataRequest,
    DeleteDataRequest,
    FindDataRequest,
    FindDataResponse,
    TailDataResponse,
    UpdateDataRequest,
    UpdateDataResponse,
    DeleteDataResponse,
)
from .dto.queries import (
    CreateQueryRequest,
    ReadQueriesResponse,
    ReadQueryResponse,
    ReadQueryRunByIdResponse,
    RunQueryRequest,
    RunQueryResponse,
)
from .logger import Log
from .nildb import NilDbBuilderClient, create_nil_db_builder_client


class SecretVaultBuilderOptions(SecretVaultBaseOptions[NilDbBuilderClient]):  # pylint: disable=too-few-public-methods
    """Options for SecretVault builder client."""

    nilauth_client: NilauthClient


class SecretVaultBuilderClient(SecretVaultBaseClient[NilDbBuilderClient]):  # pylint: disable=too-many-public-methods
    """Client for builders to manage their SecretVaults with automatic handling of concealed data if configured."""

    def __init__(self, options: SecretVaultBuilderOptions):
        super().__init__(options)
        self._nilauth_client = options.nilauth_client
        self._root_token = None

    @classmethod
    async def from_options(
        cls,
        keypair: Keypair,
        urls: Dict[str, List[str]],
        blindfold: Optional[BlindfoldFactoryConfig] = None,
    ) -> "SecretVaultBuilderClient":
        """
        Creates and initializes a new SecretVaultBuilderClient instance.

        Args:
            keypair: The keypair for authentication
            urls: Dictionary with 'chain', 'auth', and 'dbs' URLs
            blindfold: Optional blindfold configuration for encryption

        Returns:
            SecretVaultBuilderClient instance
        """
        Log.debug(
            {
                "did": keypair.to_did_string(),
                "db_count": len(urls["dbs"]),
                "blindfold": blindfold is not None,
            },
            "Creating SecretVaultBuilderClient",
        )

        # Create payer builder
        nilauth_client = NilauthClient(str(urls["auth"]))

        # Create clients for each node
        client_promises = [create_nil_db_builder_client(base) for base in urls["dbs"]]
        clients = await asyncio.gather(*client_promises)

        # Create client with or without encryption
        if blindfold:
            if hasattr(blindfold, "key") and blindfold.key:
                # User provided a key
                client = cls(
                    SecretVaultBuilderOptions(
                        clients=clients,
                        keypair=keypair,
                        key=blindfold.key,
                        nilauth_client=nilauth_client,
                    )
                )
            else:
                # Create a new key
                key = await to_blindfold_key(blindfold, cluster_size=len(clients))
                client = cls(
                    SecretVaultBuilderOptions(
                        clients=clients,
                        keypair=keypair,
                        key=key,
                        nilauth_client=nilauth_client,
                    )
                )
        else:
            # No encryption
            client = cls(
                SecretVaultBuilderOptions(
                    clients=clients,
                    keypair=keypair,
                    nilauth_client=nilauth_client,
                )
            )

        Log.info(
            {
                "id": keypair.to_did_string()[-8:],
                "nodes": len(clients),
                "encryption": client._options.key.__class__.__name__ if client._options.key else "none",
            },
            "SecretVaultBuilderClient created",
        )

        return client

    @property
    def root_token(self) -> NucTokenEnvelope:
        """Get the root token."""
        if not self._root_token:
            raise ValueError("`refresh_root_token` must be called first")
        return self._root_token

    async def refresh_root_token(self) -> None:
        """Fetches a new root NUC token from the configured nilAuth server."""
        Log.debug("Refreshing root token")
        token_response = self._nilauth_client.request_token(self.keypair.private_key(), "nildb")
        self._root_token = NucTokenEnvelope.parse(token_response)
        Log.info({"builder": self.id}, "Root token refreshed")

    async def subscription_status(self) -> Dict[str, Any]:
        """Checks subscription status by the builder's Did."""
        return self._nilauth_client.subscription_status(self.keypair.public_key(), "nildb")

    async def register(self, body: RegisterBuilderRequest) -> Dict[Did, None]:
        """Registers the builder with all nodes in the cluster."""
        result = await execute_on_cluster(self.nodes, lambda c: c.register(body))
        Log.info({"builder": self.id}, "Builder registered")
        return result

    async def read_profile(self) -> ReadBuilderProfileResponse:
        """Reads the builder's profile from the cluster."""
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.read_profile(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_BUILDERS_READ,
                )
            ),
        )
        result = process_plaintext_response(results_by_node)
        Log.info({"builder": self.id}, "Builder profile read")
        return result

    async def update_builder_profile(self, body: UpdateBuilderProfileRequest) -> Dict[Did, None]:
        """Updates the builder's profile on all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.update_profile(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_BUILDERS_UPDATE,
                ),
                body,
            ),
        )
        Log.info({"builder": self.id}, "Builder profile updated")
        return result

    async def delete_builder(self) -> Dict[Did, None]:
        """Deletes the builder from all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.delete_builder(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_BUILDERS_DELETE,
                )
            ),
        )
        Log.info({"builder": self.id}, "Builder deleted")
        return result

    async def create_collection(self, body: CreateCollectionRequest) -> Dict[Did, None]:
        """Creates a collection on all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.create_collection(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_COLLECTIONS_CREATE,
                ),
                body,
            ),
        )
        Log.info({"builder": self.id, "collection": body.name}, "Collection created")
        return result

    async def read_collections(self) -> ListCollectionsResponse:
        """Reads collections from the cluster."""
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.read_collections(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_COLLECTIONS_READ,
                )
            ),
        )
        result = process_plaintext_response(results_by_node)
        Log.info({"builder": self.id}, "Collections read")
        return result

    async def read_collection(self, collection: str) -> ReadCollectionMetadataResponse:
        """Reads collection metadata from the cluster."""
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.read_collection(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_COLLECTIONS_READ,
                ),
                collection,
            ),
        )
        result = process_plaintext_response(results_by_node)
        Log.info({"builder": self.id, "collection": collection}, "Collection read")
        return result

    async def delete_collection(self, collection: str) -> Dict[Did, None]:
        """Deletes a collection from all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.delete_collection(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_COLLECTIONS_DELETE,
                ),
                collection,
            ),
        )
        Log.info({"builder": self.id, "collection": collection}, "Collection deleted")
        return result

    async def create_collection_index(self, collection: str, body: CreateCollectionIndexRequest) -> Dict[Did, None]:
        """Creates a collection index on all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.create_collection_index(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_COLLECTIONS_UPDATE,
                ),
                collection,
                body,
            ),
        )
        Log.info(
            {"builder": self.id, "collection": collection, "index": body},
            "Collection index created",
        )
        return result

    async def drop_collection_index(self, collection: str, index: str) -> Dict[Did, None]:
        """Drops a collection index from all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.drop_collection_index(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_COLLECTIONS_UPDATE,
                ),
                collection,
                index,
            ),
        )
        Log.info(
            {"builder": self.id, "collection": collection, "index": index},
            "Collection index dropped",
        )
        return result

    async def create_standard_data(
        self, body: CreateStandardDataRequest, delegation: Optional[str] = None
    ) -> Dict[Did, CreateDataResponse]:
        """Creates standard data on all nodes."""
        create_body = inject_ids_into_records(body)

        node_payloads = (
            await prepare_concealed_request({"key": self._options.key, "clients": self.nodes, "body": create_body})
            if self._options.key
            else prepare_plaintext_request({"clients": self.nodes, "body": create_body})
        )

        # Execute on all nodes
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.create_standard_data(
                (
                    delegation
                    if delegation is not None
                    else self._mint_root_invocation(
                        audience=client.id,
                        command=NucCmd.NIL_DB_DATA_CREATE,
                    )
                ),
                node_payloads[client.id],
            ),
        )

        Log.info(
            {
                "builder": self.id,
                "collection": body.collection,
                "documents": len(body.data),
                "concealed": self._options.key is not None,
            },
            "Standard data created",
        )
        return result

    async def get_queries(self) -> Dict[Did, ReadQueriesResponse]:
        """Gets queries from the cluster."""
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.get_queries(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_QUERIES_READ,
                )
            ),
        )
        Log.info({"builder": self.id}, "Queries retrieved")
        return results_by_node

    async def get_query(self, query: str) -> Dict[Did, ReadQueryResponse]:
        """Gets a specific query from the cluster."""
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.get_query(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_QUERIES_READ,
                ),
                query,
            ),
        )
        Log.info({"builder": self.id, "query": query}, "Query retrieved")
        return results_by_node

    async def create_query(self, body: CreateQueryRequest) -> Dict[Did, None]:
        """Creates a query on all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.create_query(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_QUERIES_CREATE,
                ),
                body,
            ),
        )
        Log.info({"builder": self.id, "query": body.name}, "Query created")
        return result

    async def delete_query(self, query: str) -> Dict[Did, None]:
        """Deletes a query from all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.delete_query(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_QUERIES_DELETE,
                ),
                query,
            ),
        )
        Log.info({"builder": self.id, "query": query}, "Query deleted")
        return result

    async def run_query(self, body: RunQueryRequest) -> Dict[Did, RunQueryResponse]:
        """Runs a query on all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.run_query(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_QUERIES_RUN,
                ),
                body,
            ),
        )
        Log.info({"builder": self.id, "query": body.id}, "Query run")
        return result

    async def read_query_run_results(self, run_id: str) -> Dict[Did, ReadQueryRunByIdResponse]:
        """Reads query run results from the cluster."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.read_query_run_results(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_QUERIES_READ,
                ),
                run_id,
            ),
        )
        Log.info({"builder": self.id, "run_id": run_id}, "Query run results read")
        return result

    async def find_data(self, body: FindDataRequest) -> FindDataResponse:
        """Finds data in the cluster."""
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.find_data(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_DATA_READ,
                ),
                body,
            ),
        )

        if self._options.key:
            result = await process_concealed_list_response({"key": self._options.key, "resultsByNode": results_by_node})
        else:
            result = process_plaintext_response(results_by_node)

        Log.info(
            {
                "builder": self.id,
                "collection": body.collection,
                "concealed": self._options.key is not None,
            },
            "Data found",
        )
        return result

    async def update_data(self, body: UpdateDataRequest) -> Dict[Did, UpdateDataResponse]:
        """Updates data on all nodes."""
        # Prepare request payloads
        node_payloads = (
            await prepare_concealed_request({"key": self._options.key, "clients": self.nodes, "body": body})
            if self._options.key
            else prepare_plaintext_request({"clients": self.nodes, "body": body})
        )

        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.update_data(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_DATA_UPDATE,
                ),
                node_payloads[client.id],
            ),
        )
        Log.info(
            {"builder": self.id, "collection": body.collection, "filter": body.filter, "update": body.update},
            "Data updated",
        )
        return result

    async def delete_data(self, body: DeleteDataRequest) -> Dict[Did, DeleteDataResponse]:
        """Deletes data from all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.delete_data(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_DATA_DELETE,
                ),
                body,
            ),
        )
        Log.info(
            {"builder": self.id, "collection": body.collection, "filter": body.filter},
            "Data deleted",
        )
        return result

    async def flush_data(self, collection: str) -> Dict[Did, None]:
        """Flushes data from all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.flush_data(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_DATA_DELETE,
                ),
                collection,
            ),
        )
        Log.info({"builder": self.id, "collection": collection}, "Data flushed")
        return result

    async def tail_data(self, collection: str, limit: int = 10) -> TailDataResponse:
        """Tails data from the cluster."""
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.tail_data(
                self._mint_root_invocation(
                    audience=client.id,
                    command=NucCmd.NIL_DB_DATA_TAIL,
                ),
                collection,
                limit,
            ),
        )
        result = process_plaintext_response(results_by_node)
        Log.info({"builder": self.id, "collection": collection}, "Data tailed")
        return result

    async def __aenter__(self):
        # Enter context for all node clients
        for node in self.nodes:
            if hasattr(node, "__aenter__"):
                await node.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        # Exit context for all node clients
        for node in self.nodes:
            if hasattr(node, "__aexit__"):
                await node.__aexit__(exc_type, exc_val, exc_tb)

    def _mint_root_invocation(self, audience: Did, command: NucCmd) -> str:
        """Mints a root invocation token."""
        # Create invocation token extending the root token
        token = (
            NucTokenBuilder.extending(self._root_token)
            .command(Command(command.value.split(".")))
            .body(InvocationBody({}))
            .expires_at(datetime.fromtimestamp(into_seconds_from_now(60)))
            .audience(audience)
            .build(self.keypair.private_key())
        )

        return token
