"""
Query-related DTOs for SecretVaults API.
"""

from enum import Enum
from typing import List, Dict, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field, constr, model_validator

from ..common.types import Uuid

# VariablePath: regex validation
PATH_EXPRESSION = r"^\$(\.[$a-zA-Z][a-zA-Z0-9-_]+(\[\d+])*)+$"
VariablePath = constr(pattern=PATH_EXPRESSION)


class QueryVariableValidator(BaseModel):
    """Validator for query variables, specifying the path and optional description."""
    path: VariablePath
    description: Optional[str] = None


class CreateQueryRequest(BaseModel):
    """Request model for creating a new query."""
    id: Uuid = Field(alias="_id")
    collection: Uuid
    name: str
    pipeline: List[Dict[str, Any]]
    variables: Dict[str, Any]

    @model_validator(mode="before")
    @classmethod
    def allow_id_or__id(cls, data):
        """Allow both 'id' and '_id' fields for backward compatibility."""
        if "id" in data and "_id" not in data:
            data["_id"] = data["id"]
        return data


class QueryDocumentResponse(BaseModel):
    """A summary of a query, including its ID, name, and collection."""
    id: Uuid = Field(alias="_id")
    name: str
    collection: Uuid


class ReadQueriesResponse(BaseModel):
    """Response model for listing all queries."""
    data: List[QueryDocumentResponse]


class ReadQueryResponse(BaseModel):
    """Response model for reading a single query."""
    data: QueryDocumentResponse


class DeleteQueryRequest(BaseModel):
    """Request model for deleting a query by ID."""
    id: Uuid


class RunQueryRequest(BaseModel):
    """Request model for running a query with variables."""
    id: Uuid = Field(alias="_id")
    variables: Dict[str, Any]


class RunQueryResponse(BaseModel):
    """Response model for running a query, returning the result ID."""
    data: Uuid


class RunQueryResultStatus(str, Enum):
    """Status values for a query run result."""
    pending = "pending"  # pylint: disable=invalid-name
    running = "running"  # pylint: disable=invalid-name
    complete = "complete"  # pylint: disable=invalid-name
    error = "error"  # pylint: disable=invalid-name


class ReadQueryRunByIdDto(BaseModel):
    """Details of a query run, including status, result, and errors."""
    id: Uuid = Field(alias="_id")
    query: Uuid
    status: RunQueryResultStatus
    started: Optional[datetime] = None
    completed: Optional[datetime] = None
    result: Optional[Any] = None
    errors: Optional[List[str]] = None


class ReadQueryRunByIdResponse(BaseModel):
    """Response model for reading a query run by ID."""
    data: ReadQueryRunByIdDto
