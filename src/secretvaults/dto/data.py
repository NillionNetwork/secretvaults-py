"""
Data-related DTOs for SecretVaults API.
"""

from typing import List, Dict, Optional, Any
from pydantic import BaseModel, Field
from ..common.types import Did, Uuid
from .users import AclDto


class CreateStandardDataRequest(BaseModel):
    """Request model for creating standard (non-owned) data in a collection."""
    collection: Uuid
    data: List[Dict[str, Any]]


class CreateOwnedDataRequest(BaseModel):
    """Request model for creating owned data in a collection, specifying owner and ACL."""
    collection: Uuid
    data: List[Dict[str, Any]]
    owner: Did
    acl: AclDto


class CreateDataError(BaseModel):
    """Error details for a failed data creation attempt."""
    error: str
    document: Any


class CreateDataResponseData(BaseModel):
    """Response data for a data creation request, including created IDs and errors."""
    created: List[Uuid]
    errors: List[CreateDataError]  # Array of errors for failed documents


class CreateDataResponse(BaseModel):
    """Response model for a data creation request."""
    data: CreateDataResponseData


class UpdateDataRequest(BaseModel):
    """Request model for updating data in a collection."""
    collection: Uuid
    filter: Dict[str, Any]
    update: Dict[str, Any]


class UpdateDataResponseData(BaseModel):
    """Response data for a data update request, including counts and upserted ID."""
    acknowledged: bool
    matched: int
    modified: int
    upserted: int
    upserted_id: Optional[str] = None


class UpdateDataResponse(BaseModel):
    """Response model for a data update request."""
    data: UpdateDataResponseData


class FindDataRequest(BaseModel):
    """Request model for finding data in a collection using a filter."""
    collection: Uuid
    filter: Dict[str, Any]


class FindDataResponse(BaseModel):
    """Response model for a data find request, returning a list of documents."""
    data: List[Dict[str, Any]]


class DeleteDataRequest(BaseModel):
    """Request model for deleting data in a collection using a filter."""
    collection: Uuid
    filter: Dict[str, Any]


class DeleteDataResponseData(BaseModel):
    """Response data for a data deletion request, including count of deleted documents."""
    acknowledged: bool
    deletedCount: int


class DeleteDataResponse(BaseModel):
    """Response model for a data deletion request."""
    data: DeleteDataResponseData


class FlushDataRequest(BaseModel):
    """Request model for flushing all data in a collection."""
    collection: Uuid


class DataSchemaByIdRequestParams(BaseModel):
    """Request parameters for retrieving a data schema by ID."""
    id: Uuid


class TailDataRequest(BaseModel):
    """Request model for tailing the last N documents in a collection."""
    id: Uuid
    limit: int = Field(le=1000, default=10)


class TailDataResponse(BaseModel):
    """Response model for a tail data request, returning a list of documents."""
    data: List[Dict[str, Any]]
