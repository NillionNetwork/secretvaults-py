"""
Builder-related DTOs for SecretVaults API.
"""

from typing import List
from datetime import datetime
from pydantic import BaseModel, Field, model_validator
from .common import Name
from ..common.types import Did


class RegisterBuilderRequest(BaseModel):
    """Request model for registering a new builder."""

    did: Did
    name: Name


class BuilderProfileDto(BaseModel):
    """Profile information for a builder, including collections and queries."""

    id: Did = Field(alias="_id")
    created: datetime = Field(alias="_created")
    updated: datetime = Field(alias="_updated")
    name: str
    collections: List[str]  # Changed from List[UUID] to List[str] for serialization
    queries: List[str]  # Changed from List[UUID] to List[str] for serialization

    @model_validator(mode="before")
    @classmethod
    def allow_id_or__id(cls, data):
        """Allow both 'id' and '_id' fields for backward compatibility."""
        if "id" in data and "_id" not in data:
            data["_id"] = data["id"]
        return data


class ReadBuilderProfileResponse(BaseModel):
    """Response model for reading a builder's profile."""

    success: bool = True
    data: BuilderProfileDto


class UpdateBuilderProfileRequest(BaseModel):
    """Request model for updating a builder's profile name."""

    name: Name
