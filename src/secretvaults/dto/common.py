"""
Common DTOs used across the SecretVaults API.
"""

from typing import List
from datetime import datetime
from pydantic import BaseModel, constr
from ..common.types import Uuid, Did

# Name: string with length constraints
Name = constr(min_length=1, max_length=255)


class ApiErrorResponse(BaseModel):
    """Represents an error response from the API, including a timestamp and a list of error messages."""

    ts: datetime
    errors: List[str]


class ByIdRequestParams(BaseModel):
    """Request parameters for operations that require an ID."""

    id: Uuid


class Acl(BaseModel):
    """Access control list (ACL) entry specifying permissions for a grantee DID."""

    grantee: Did
    read: bool
    write: bool
    execute: bool
