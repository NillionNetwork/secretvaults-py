"""
Collection-related DTOs for SecretVaults API.
"""

from typing import List, Dict, Optional, Union, Literal
from datetime import datetime
from pydantic import BaseModel, Field, constr, model_validator

from ..common.types import Uuid


class CollectionDocumentDto(BaseModel):
    """A summary of a collection, including its ID, type, and name."""

    id: Uuid
    type: Literal["standard", "owned"]
    name: str


class ListCollectionsResponse(BaseModel):
    """Response model for listing all collections."""

    data: List[CollectionDocumentDto]


class CreateCollectionRequest(BaseModel):
    """Request model for creating a new collection."""

    id: Uuid = Field(alias="_id")
    type: Literal["standard", "owned"]
    name: constr(min_length=1)
    schema_data: Dict[str, object] = Field(alias="schema")

    @model_validator(mode="before")
    @classmethod
    def allow_id_or__id(cls, data):
        """Allow both 'id' and '_id' fields for backward compatibility."""
        if "id" in data and "_id" not in data:
            data["_id"] = data["id"]
        return data


class DeleteCollectionRequestParams(BaseModel):
    """Request parameters for deleting a collection by ID."""

    id: Uuid


class CreateCollectionIndexRequest(BaseModel):
    """Request model for creating an index on a collection."""

    collection: Uuid
    name: constr(min_length=4)
    keys: List[Dict[str, Literal[1, -1]]]
    unique: bool
    ttl: Optional[float] = 0


class CollectionIndexDto(BaseModel):
    """Details of a collection index, including its name and uniqueness."""

    v: int
    key: Dict[str, Union[str, int, float]]
    name: str
    unique: bool


class DropCollectionIndexParams(BaseModel):
    """Request parameters for dropping a collection index by name."""

    id: Uuid
    name: constr(min_length=4, max_length=50)


class ReadCollectionMetadataRequestParams(BaseModel):
    """Request parameters for reading collection metadata by ID."""

    id: Uuid


class CollectionMetadataDto(BaseModel):
    """Metadata for a collection, including size, count, and indexes."""

    id: Uuid = Field(alias="_id")
    count: int
    size: int
    first_write: datetime
    last_write: datetime
    indexes: List[CollectionIndexDto]

    @model_validator(mode="before")
    @classmethod
    def allow_id_or__id(cls, data):
        """Allow both 'id' and '_id' fields for backward compatibility."""
        if "id" in data and "_id" not in data:
            data["_id"] = data["id"]
        return data


class ReadCollectionMetadataResponse(BaseModel):
    """Response model for reading collection metadata."""

    data: CollectionMetadataDto
