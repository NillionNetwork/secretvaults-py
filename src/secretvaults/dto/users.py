"""
User-related DTOs for SecretVaults API.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, Extra, model_validator
from ..common.types import Did, Uuid


class AclDto(BaseModel):
    """Access control list (ACL) entry specifying permissions for a grantee DID."""

    grantee: Did
    read: bool
    write: bool
    execute: bool


class UserProfileLogEntry(BaseModel):
    """A log entry for user profile operations, including collection and ACL."""

    op: str
    collection: Uuid
    acl: Optional[AclDto] = None


class UserProfileDto(BaseModel):
    """Profile information for a user, including logs and timestamps."""

    id: Did = Field(alias="_id")
    created: datetime = Field(alias="_created")
    updated: datetime = Field(alias="_updated")
    logs: List[UserProfileLogEntry]

    @model_validator(mode="before")
    @classmethod
    def allow_id_or__id(cls, data):
        """Allow both 'id' and '_id' fields for backward compatibility."""
        if "id" in data and "_id" not in data:
            data["_id"] = data["id"]
        return data


class ReadUserProfileResponse(BaseModel):
    """Response model for reading a user's profile."""

    data: UserProfileDto


class ReadDataRequestParams(BaseModel):
    """Request parameters for reading a user's data document."""

    collection: Uuid
    document: Uuid


class OwnedDataDto(BaseModel, extra=Extra.allow):
    """A data document owned by a user, including ACL and timestamps."""

    id: Uuid = Field(alias="_id")
    created: datetime = Field(alias="_created")
    updated: datetime = Field(alias="_updated")
    owner: Did = Field(alias="_owner")
    acl: List[AclDto] = Field(alias="_acl")


class ReadDataResponse(BaseModel):
    """Response model for reading a user's owned data document."""

    data: OwnedDataDto


class DataDocumentReference(BaseModel):
    """Reference to a data document, including builder, collection, and document IDs."""

    builder: Did
    collection: Uuid
    document: Uuid


class ListDataReferencesResponse(BaseModel):
    """Response model for listing all data document references for a user."""

    data: List[DataDocumentReference]


class GrantAccessToDataRequest(BaseModel):
    """Request model for granting access to a data document."""

    collection: Uuid
    document: Uuid
    acl: AclDto


class RevokeAccessToDataRequest(BaseModel):
    """Request model for revoking access from a data document."""

    grantee: Did
    collection: Uuid
    document: Uuid


class DeleteDocumentRequestParams(BaseModel):
    """Request parameters for deleting a user's data document."""

    collection: Uuid
    document: Uuid


class UpdateUserDataRequest(BaseModel):
    """Request model for updating a user's data document."""

    document: Uuid
    collection: Uuid
    update: Dict[str, Any]
