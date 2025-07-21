"""Data Transfer Objects for SecretVaults API."""

from .builders import *
from .collections import *
from .common import *
from .data import *
from .queries import *
from .system import *
from .users import *

__all__ = [
    # Common
    "Name",
    "ApiErrorResponse",
    "ByIdRequestParams",
    "Acl",
    # System
    "ReadAboutNodeResponse",
    "NodeHealthCheckResponse",
    "BuildInfo",
    "MaintenanceInfo",
    # Builders
    "RegisterBuilderRequest",
    "ReadBuilderProfileResponse",
    "UpdateBuilderProfileRequest",
    # Collections
    "CreateCollectionRequest",
    "ListCollectionsResponse",
    "DeleteCollectionRequestParams",
    "CreateCollectionIndexRequest",
    "DropCollectionIndexParams",
    "ReadCollectionMetadataRequestParams",
    "ReadCollectionMetadataResponse",
    # Data
    "CreateStandardDataRequest",
    "CreateOwnedDataRequest",
    "CreateDataResponse",
    "FindDataRequest",
    "FindDataResponse",
    "UpdateDataRequest",
    "UpdateDataResponse",
    "DeleteDataRequest",
    "DeleteDataResponse",
    "FlushDataRequest",
    "DataSchemaByIdRequestParams",
    "TailDataRequest",
    "TailDataResponse",
    # Queries
    "CreateQueryRequest",
    "QueryDocumentResponse",
    "ReadQueriesResponse",
    "ReadQueryResponse",
    "DeleteQueryRequest",
    "RunQueryRequest",
    "RunQueryResponse",
    "ReadQueryRunByIdResponse",
    # Users
    "ReadUserProfileResponse",
    "ReadDataRequestParams",
    "ReadDataResponse",
    "ListDataReferencesResponse",
    "GrantAccessToDataRequest",
    "RevokeAccessToDataRequest",
    "DeleteDocumentRequestParams",
    "UpdateUserDataRequest",
]
