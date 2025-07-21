"""
User NIL DB client implementation.
"""

import aiohttp

from nuc.envelope import NucTokenEnvelope

from ..common.paths import NilDbEndpoint
from ..dto.data import CreateDataResponse, CreateOwnedDataRequest
from ..dto.users import (
    DeleteDocumentRequestParams,
    GrantAccessToDataRequest,
    ListDataReferencesResponse,
    ReadDataRequestParams,
    ReadDataResponse,
    ReadUserProfileResponse,
    RevokeAccessToDataRequest,
    UpdateUserDataRequest,
)
from ..dto.system import ReadAboutNodeResponse
from .base_client import AuthenticatedRequestOptions, NilDbBaseClient, NilDbBaseClientOptions

from ..logger import Log


class NilDbUserClientOptions(NilDbBaseClientOptions):
    """Options for NIL DB user client."""


class NilDbUserClient(NilDbBaseClient):
    """User NIL DB client implementation."""

    def __init__(self, options: NilDbUserClientOptions):
        super().__init__(options)
        self._options = options

    async def read_profile(self, token: str) -> ReadUserProfileResponse:
        """Retrieve the authenticated user's profile information."""
        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.users.me, token=token), ReadUserProfileResponse
        )

    async def create_owned_data(self, token: str, body: CreateOwnedDataRequest) -> CreateDataResponse:
        """Create user-owned data in an owned collection."""
        # Handle both Pydantic models and dictionaries
        if hasattr(body, "model_dump") and not isinstance(body, dict):
            body_data = body.model_dump(by_alias=True)
        else:
            body_data = body

        Log.info(f"Creating owned data with body: {body_data}")

        # Parse token and get details safely
        try:
            token_envelope = NucTokenEnvelope.parse(token)
            token_data = token_envelope.token.token
            Log.info(f"Token JSON: {token_data.to_json()}")
        except Exception as e:  # pylint: disable=broad-exception-caught
            Log.warning(f"Could not parse token for debugging: {e}")

        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.data.createOwned, method="POST", body=body_data, token=token
            ),
            CreateDataResponse,
        )

    async def list_data_references(self, token: str) -> ListDataReferencesResponse:
        """List all data records owned by the authenticated user."""
        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.users.data.root, token=token), ListDataReferencesResponse
        )

    async def read_data(self, token: str, params: ReadDataRequestParams) -> ReadDataResponse:
        """Retrieve user-owned data by collection and document id."""
        path = NilDbEndpoint.v1.users.data.byId.replace(":collection", params.collection).replace(
            ":document", params.document
        )
        return await self.request(AuthenticatedRequestOptions(path=path, token=token), ReadDataResponse)

    async def delete_data(self, token: str, params: DeleteDocumentRequestParams) -> None:
        """Delete a user-owned data document."""
        path = NilDbEndpoint.v1.users.data.byId.replace(":collection", params.collection).replace(
            ":document", params.document
        )
        return await self.request(
            AuthenticatedRequestOptions(path=path, method="DELETE", token=token),
        )

    async def grant_access(self, token: str, body: GrantAccessToDataRequest) -> None:
        """Grant access to user-owned data."""
        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.users.data.acl.grant,
                method="POST",
                body=body.model_dump(by_alias=True),
                token=token,
            )
        )

    async def revoke_access(self, token: str, body: RevokeAccessToDataRequest) -> None:
        """Revoke access to user-owned data."""
        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.users.data.acl.revoke,
                method="POST",
                body=body.model_dump(by_alias=True),
                token=token,
            )
        )

    async def update_data(self, token: str, body: UpdateUserDataRequest) -> None:
        """Update a user-owned data document."""
        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.users.data.root, method="POST", body=body.model_dump(by_alias=True), token=token
            )
        )


async def create_nil_db_user_client(base_url: str) -> NilDbUserClient:
    """
    Create a NIL DB user client.

    Args:
        base_url: Base URL for the NIL DB service

    Returns:
        NIL DB user client
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{base_url}/about") as response:
            body = await response.json()
            about = ReadAboutNodeResponse.model_validate(body)

    options = NilDbUserClientOptions(about=about, base_url=base_url)

    return NilDbUserClient(options)
