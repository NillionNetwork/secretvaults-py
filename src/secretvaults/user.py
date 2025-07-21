"""
SecretVault user client for managing owned documents.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional

from nuc.builder import NucTokenBuilder
from nuc.token import Command, Did, InvocationBody
from nuc.envelope import NucTokenEnvelope
from .common.keypair import Keypair
from .common.utils import into_seconds_from_now

from .base import SecretVaultBaseClient, SecretVaultBaseOptions
from .common.blindfold import BlindfoldFactoryConfig, to_blindfold_key
from .common.cluster import (
    execute_on_cluster,
    prepare_concealed_request,
    prepare_plaintext_request,
    process_concealed_object_response,
    process_plaintext_response,
)
from .common.nuc_cmd import NucCmd
from .dto.data import CreateDataResponse, CreateOwnedDataRequest
from .dto.users import (
    DeleteDocumentRequestParams,
    GrantAccessToDataRequest,
    ListDataReferencesResponse,
    ReadDataRequestParams,
    ReadDataResponse,
    ReadUserProfileResponse,
    RevokeAccessToDataRequest,
    UpdateUserDataRequest,
)
from .logger import Log
from .nildb import NilDbUserClient, create_nil_db_user_client


class SecretVaultUserOptions(SecretVaultBaseOptions[NilDbUserClient]):  # pylint: disable=too-few-public-methods
    """Options for SecretVault user client."""


class SecretVaultUserClient(SecretVaultBaseClient[NilDbUserClient]):
    """Client for users to manage owned-documents in SecretVaults."""

    @classmethod
    async def from_options(
        cls,
        keypair: Keypair,
        base_urls: List[str],
        blindfold: Optional[BlindfoldFactoryConfig] = None,
    ) -> "SecretVaultUserClient":
        """
        Creates and initializes a new SecretVaultUserClient instance.

        Args:
            keypair: The keypair for authentication
            base_urls: List of base URLs for the NIL DB services
            blindfold: Optional blindfold configuration for encryption

        Returns:
            SecretVaultUserClient instance
        """
        # Create clients
        client_promises = [create_nil_db_user_client(url) for url in base_urls]
        clients = await asyncio.gather(*client_promises)

        # Create client with or without encryption
        if blindfold:
            if hasattr(blindfold, "key") and blindfold.key:
                # User provided a key
                client = cls(
                    SecretVaultUserOptions(
                        clients=clients,
                        keypair=keypair,
                        key=blindfold.key,
                    )
                )
            else:
                # Create a new key
                key = await to_blindfold_key(blindfold, cluster_size=len(clients))
                client = cls(
                    SecretVaultUserOptions(
                        clients=clients,
                        keypair=keypair,
                        key=key,
                    )
                )
        else:
            # No encryption
            client = cls(
                SecretVaultUserOptions(
                    clients=clients,
                    keypair=keypair,
                )
            )

        Log.info(
            {
                "did": keypair.to_did_string()[-8:],
                "nodes": len(clients),
                "encryption": client._options.key.__class__.__name__ if client._options.key else "none",
            },
            "SecretVaultUserClient created",
        )

        return client

    async def read_profile(self) -> ReadUserProfileResponse:
        """Reads the user's profile information from the cluster."""
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.read_profile(
                self._mint_invocation(
                    command=NucCmd.NIL_DB_USERS_READ,
                    audience=client.id,
                )
            ),
        )
        result = process_plaintext_response(results_by_node)
        Log.info({"user": self.id}, "User profile read")
        return result

    async def create_data(self, delegation: str, body: CreateOwnedDataRequest) -> Dict[Did, CreateDataResponse]:
        """Creates one or more data documents owned by the user."""
        key = self._options.key
        clients = self.nodes

        # Prepare map of node-id to node-specific payload
        node_payloads = (
            await prepare_concealed_request({"key": key, "clients": clients, "body": body})
            if key
            else prepare_plaintext_request({"clients": clients, "body": body})
        )

        # Execute on all nodes
        def create_invocation_token(client):
            # Parse the delegation token envelope
            envelope = NucTokenEnvelope.parse(delegation)

            # Create invocation token builder that extends the delegation
            builder = NucTokenBuilder.extending(envelope)

            # Build the token with all required parameters
            token = (
                builder.command(Command(NucCmd.NIL_DB_DATA_CREATE.value.split(".")))
                .audience(client.id)  # Target node's DID
                .expires_at(datetime.fromtimestamp(into_seconds_from_now(60)))
                .body(InvocationBody({}))
                .build(self.keypair.private_key())
            )

            return token

        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.create_owned_data(
                create_invocation_token(client),
                node_payloads[client.id],
            ),
        )

        Log.info(
            {
                "user": self.id,
                "collection": body.collection,
                "documents": len(body.data),
                "concealed": key is not None,
            },
            "User data created",
        )
        return result

    async def list_data_references(self) -> ListDataReferencesResponse:
        """Lists references to all data documents owned by the user."""
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.list_data_references(
                self._mint_invocation(
                    command=NucCmd.NIL_DB_USERS_READ,
                    audience=client.id,
                )
            ),
        )
        result = process_plaintext_response(results_by_node)
        Log.info(
            {"user": self.id, "count": len(result.data) if result.data else 0},
            "User data references listed",
        )
        return result

    async def read_data(self, params: ReadDataRequestParams) -> ReadDataResponse:  # pylint: disable=too-many-locals,too-many-branches
        """Reads a single data document, automatically revealing concealed values if a key is configured."""
        # Fetch the raw data from all nodes
        results_by_node = await execute_on_cluster(
            self.nodes,
            lambda client: client.read_data(
                self._mint_invocation(
                    command=NucCmd.NIL_DB_USERS_READ,
                    audience=client.id,
                ),
                params,
            ),
        )

        key = self._options.key

        # Extract and process only the 5 DTO fields as plaintext
        # Use the actual field names (without underscores) as they appear in the data
        dto_fields = ["id", "created", "updated", "owner", "acl"]

        # Create a modified results_by_node with only DTO fields
        dto_results = {}
        # Create a new dict for non-DTO fields
        data_results = {}
        for node_id, response in results_by_node.items():

            if hasattr(response, "data") and response.data:
                # Extract only DTO fields
                dto_data = {}
                field_mapping = {
                    "id": "_id",
                    "created": "_created",
                    "updated": "_updated",
                    "owner": "_owner",
                    "acl": "_acl",
                }
                for field in dto_fields:
                    if hasattr(response.data, field):
                        aliased_field = field_mapping[field]
                        dto_data[aliased_field] = getattr(response.data, field)
                dto_results[node_id] = type(response)(data=dto_data)

                # Extract non-DTO fields
                data_fields = {}
                # Convert Pydantic model to dict
                if hasattr(response.data, "model_dump"):
                    response_dict = response.data.model_dump()
                else:
                    response_dict = dict(response.data)

                for field_name, field_value in response_dict.items():
                    if field_name not in dto_fields:
                        data_fields[field_name] = field_value
                data_results[node_id] = data_fields
            else:
                dto_results[node_id] = response
                data_results[node_id] = response

        # Process DTO fields as plaintext
        dto_result = process_plaintext_response(dto_results)

        # Process the data fields with key/no key logic
        if key:
            # Process with concealed response
            try:
                data_result = await process_concealed_object_response({"key": key, "resultsByNode": data_results})
            except Exception as e:  # pylint: disable=broad-exception-caught
                Log.warning("Concealed processing failed, using plaintext", error=str(e))
                data_result = process_plaintext_response(data_results)
        else:
            # Process as plaintext
            data_result = process_plaintext_response(data_results)

        # Merge DTO results and data results
        if hasattr(dto_result, "data") and dto_result.data and data_result:
            # Convert DTO result to dictionary
            if hasattr(dto_result.data, "model_dump"):
                dto_dict = dto_result.data.model_dump()
            else:
                dto_dict = dict(dto_result.data)

            merged_data = {**dto_dict, **data_result}
            result = merged_data
        else:
            result = dto_result

        Log.info(
            {
                "user": self.id,
                "collection": params.collection,
                "document": params.document,
            },
            "User data read",
        )
        return result

    async def delete_data(self, params: DeleteDocumentRequestParams) -> Dict[Did, None]:
        """Deletes a user-owned document from all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.delete_data(
                self._mint_invocation(
                    command=NucCmd.NIL_DB_USERS_DELETE,
                    audience=client.id,
                ),
                params,
            ),
        )
        Log.info(
            {
                "user": self.id,
                "collection": params.collection,
                "document": params.document,
            },
            "User data deleted",
        )
        return result

    async def grant_access(self, body: GrantAccessToDataRequest) -> Dict[Did, None]:
        """Grants access to data for a specific user."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.grant_access(
                self._mint_invocation(
                    command=NucCmd.NIL_DB_USERS_UPDATE,
                    audience=client.id,
                ),
                body,
            ),
        )
        Log.info(
            {
                "user": self.id,
                "collection": body.collection,
                "document": body.document,
                "grantee": body.acl.grantee,
            },
            "Access granted",
        )
        return result

    async def revoke_access(self, body: RevokeAccessToDataRequest) -> Dict[Did, None]:
        """Revokes access to data for a specific user."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.revoke_access(
                self._mint_invocation(
                    command=NucCmd.NIL_DB_USERS_UPDATE,
                    audience=client.id,
                ),
                body,
            ),
        )
        Log.info(
            {
                "user": self.id,
                "collection": body.collection,
                "document": body.document,
                "grantee": body.grantee,
            },
            "Access revoked",
        )
        return result

    async def update_data(self, body: UpdateUserDataRequest) -> Dict[Did, None]:
        """Updates a user-owned document on all nodes."""
        result = await execute_on_cluster(
            self.nodes,
            lambda client: client.update_data(
                self._mint_invocation(
                    command=NucCmd.NIL_DB_USERS_UPDATE,
                    audience=client.id,
                ),
                body,
            ),
        )
        Log.info(
            {
                "user": self.id,
                "collection": body.collection,
                "document": body.document,
            },
            "User data updated",
        )
        return result

    async def close(self):
        """Close all node connections."""
        for node in self.nodes:
            if hasattr(node, "close") and callable(getattr(node, "close")):
                await node.close()

    def _mint_invocation(self, command: NucCmd, audience: Did) -> str:
        """Mints an invocation token for user operations.

        Args:
            command: The NUC command to execute
            audience: The DID of the target node

        Returns:
            A signed invocation token
        """
        # Create invocation token builder
        builder = NucTokenBuilder.invocation({})

        # Build the token with all required parameters
        token = (
            builder.command(Command(command.value.split(".")))
            .subject(self.id)  # User's DID as subject
            .audience(audience)  # Target node's DID
            .expires_at(datetime.fromtimestamp(into_seconds_from_now(60)))
            .build(self.keypair.private_key())
        )

        return token
