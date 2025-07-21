"""
Builder NIL DB client implementation.
"""

import aiohttp

from ..common.paths import NilDbEndpoint
from ..dto.builders import (
    ReadBuilderProfileResponse,
    RegisterBuilderRequest,
    UpdateBuilderProfileRequest,
)
from ..dto.collections import (
    CreateCollectionIndexRequest,
    CreateCollectionRequest,
    ListCollectionsResponse,
    ReadCollectionMetadataResponse,
)
from ..dto.data import (
    CreateDataResponse,
    CreateStandardDataRequest,
    DeleteDataRequest,
    DeleteDataResponse,
    FindDataRequest,
    FindDataResponse,
    TailDataResponse,
    UpdateDataRequest,
    UpdateDataResponse,
)
from ..dto.queries import (
    CreateQueryRequest,
    ReadQueriesResponse,
    ReadQueryResponse,
    ReadQueryRunByIdResponse,
    RunQueryRequest,
    RunQueryResponse,
)
from ..dto.system import ReadAboutNodeResponse
from .base_client import AuthenticatedRequestOptions, NilDbBaseClient, NilDbBaseClientOptions


class NilDbBuilderClientOptions(NilDbBaseClientOptions):
    """Options for NIL DB builder client."""


class NilDbBuilderClient(NilDbBaseClient):  # pylint: disable=too-many-public-methods
    """Builder NIL DB client implementation."""

    def __init__(self, options: NilDbBuilderClientOptions):
        super().__init__(options)
        self._options = options

    async def register(self, body: RegisterBuilderRequest) -> None:
        """Register a new builder."""
        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.builders.register, method="POST", body=body.model_dump(by_alias=True)
            ),
        )

    async def read_profile(self, token: str) -> ReadBuilderProfileResponse:
        """Retrieve the authenticated builder's profile information."""
        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.builders.me, token=token), ReadBuilderProfileResponse
        )

    async def update_profile(self, token: str, body: UpdateBuilderProfileRequest) -> None:
        """Update the authenticated builder's profile information."""
        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.builders.me, method="POST", body=body.model_dump(by_alias=True), token=token
            ),
        )

    async def delete_builder(self, token: str) -> None:
        """Delete the authenticated builder and all associated resources."""
        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.builders.me, method="DELETE", token=token),
        )

    async def create_collection(self, token: str, body: CreateCollectionRequest) -> None:
        """Create a new collection for data validation."""
        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.collections.root, method="POST", body=body.model_dump(by_alias=True), token=token
            ),
        )

    async def read_collections(self, token: str) -> ListCollectionsResponse:
        """List all collections owned by the authenticated builder."""
        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.collections.root, method="GET", token=token),
            ListCollectionsResponse,
        )

    async def delete_collection(self, token: str, collection: str) -> None:
        """Delete a collection by id and all associated data."""
        path = NilDbEndpoint.v1.collections.byId.replace(":id", collection)
        return await self.request(
            AuthenticatedRequestOptions(path=path, method="DELETE", token=token),
        )

    async def read_collection(self, token: str, collection: str) -> ReadCollectionMetadataResponse:
        """Retrieve a collection by id including metadata."""
        path = NilDbEndpoint.v1.collections.byId.replace(":id", collection)
        return await self.request(
            AuthenticatedRequestOptions(path=path, method="GET", token=token), ReadCollectionMetadataResponse
        )

    async def create_collection_index(self, token: str, collection: str, body: CreateCollectionIndexRequest) -> None:
        """Create an index on a collection."""
        path = NilDbEndpoint.v1.collections.indexesById.replace(":id", collection)
        return await self.request(
            AuthenticatedRequestOptions(path=path, method="POST", body=body.model_dump(by_alias=True), token=token),
        )

    async def drop_collection_index(self, token: str, collection: str, index: str) -> None:
        """Drop an index from a collection."""
        path = NilDbEndpoint.v1.collections.indexesByNameById.replace(":id", collection).replace(":name", index)
        return await self.request(
            AuthenticatedRequestOptions(path=path, method="DELETE", token=token),
        )

    async def get_queries(self, token: str) -> ReadQueriesResponse:
        """List all queries owned by the authenticated builder."""
        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.queries.root, token=token), ReadQueriesResponse
        )

    async def get_query(self, token: str, query: str) -> ReadQueryResponse:
        """Retrieve a query by id."""
        path = NilDbEndpoint.v1.queries.byId.replace(":id", query)
        return await self.request(AuthenticatedRequestOptions(path=path, token=token), ReadQueryResponse)

    async def create_query(self, token: str, body: CreateQueryRequest) -> None:
        """Create a new MongoDB aggregation query with variable substitution."""
        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.queries.root, method="POST", body=body.model_dump(by_alias=True), token=token
            )
        )

    async def delete_query(self, token: str, query: str) -> None:
        """Delete a query by id."""
        path = NilDbEndpoint.v1.queries.byId.replace(":id", query)
        return await self.request(
            AuthenticatedRequestOptions(path=path, method="DELETE", token=token),
        )

    async def run_query(self, token: str, body: RunQueryRequest) -> RunQueryResponse:
        """Execute a query with variable substitution."""
        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.queries.run, method="POST", body=body.model_dump(by_alias=True), token=token
            ),
            RunQueryResponse,
        )

    async def read_query_run_results(self, token: str, run: str) -> ReadQueryRunByIdResponse:
        """Retrieve the status and results of a background query job."""
        path = NilDbEndpoint.v1.queries.runById.replace(":id", run)
        return await self.request(AuthenticatedRequestOptions(path=path, token=token), ReadQueryRunByIdResponse)

    async def create_standard_data(self, token: str, body: CreateStandardDataRequest) -> CreateDataResponse:
        """Upload standard data records to a schema-validated collection."""
        # Handle both Pydantic models and dictionaries
        if hasattr(body, "model_dump") and not isinstance(body, dict):
            body_data = body.model_dump(by_alias=True)
        else:
            body_data = body

        return await self.request(
            AuthenticatedRequestOptions(
                path=NilDbEndpoint.v1.data.createStandard, method="POST", body=body_data, token=token
            ),
            CreateDataResponse,
        )

    async def find_data(self, token: str, body: FindDataRequest) -> FindDataResponse:
        """Search for data matching the provided filter."""
        # Handle both Pydantic models and dictionaries
        if hasattr(body, "model_dump") and not isinstance(body, dict):
            body_data = body.model_dump(by_alias=True)
        else:
            body_data = body
        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.data.find, method="POST", body=body_data, token=token),
            FindDataResponse,
        )

    async def update_data(self, token: str, body: UpdateDataRequest) -> UpdateDataResponse:
        """Update data records matching the provided filter."""
        # Handle both Pydantic models and dictionaries
        if hasattr(body, "model_dump") and not isinstance(body, dict):
            body_data = body.model_dump(by_alias=True)
        else:
            body_data = body
        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.data.update, method="POST", body=body_data, token=token),
            UpdateDataResponse,
        )

    async def delete_data(self, token: str, body: DeleteDataRequest) -> DeleteDataResponse:
        """Delete data records matching the provided filter."""
        # Handle both Pydantic models and dictionaries
        if hasattr(body, "model_dump") and not isinstance(body, dict):
            body_data = body.model_dump(by_alias=True)
        else:
            body_data = body

        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.data.delete, method="POST", body=body_data, token=token),
            DeleteDataResponse,
        )

    async def flush_data(self, token: str, collection: str) -> None:
        """Remove all data from a collection."""
        path = NilDbEndpoint.v1.data.flushById.replace(":id", collection)
        return await self.request(
            AuthenticatedRequestOptions(path=path, method="DELETE", token=token),
        )

    async def tail_data(self, token: str, collection: str, limit: int = 10) -> TailDataResponse:
        """Retrieve the most recent data records from a collection."""
        path = f"{NilDbEndpoint.v1.data.tailById.replace(':id', collection)}?limit={limit}"
        return await self.request(AuthenticatedRequestOptions(path=path, method="GET", token=token), TailDataResponse)


async def create_nil_db_builder_client(base_url: str) -> NilDbBuilderClient:
    """
    Create a NIL DB builder client.

    Args:
        base_url: Base URL for the NIL DB service

    Returns:
        NIL DB builder client
    """
    async with aiohttp.ClientSession() as session:
        async with session.get(f"{base_url}/about") as response:
            body = await response.json()
            about = ReadAboutNodeResponse.model_validate(body)

    options = NilDbBuilderClientOptions(about=about, base_url=base_url)

    return NilDbBuilderClient(options)
