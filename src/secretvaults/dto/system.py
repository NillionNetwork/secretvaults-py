"""
System-related DTOs for node information.
"""

from datetime import datetime
from typing import Literal
from pydantic import BaseModel, Field, HttpUrl


class BuildInfo(BaseModel):
    """Build information for a node, including time, commit, and version."""
    time: datetime
    commit: str
    version: str


class MaintenanceInfo(BaseModel):
    """Information about node maintenance status and start time."""
    active: bool
    started_at: datetime


class ReadAboutNodeResponse(BaseModel):
    """Response model for node information, including build, public key, and maintenance."""
    started: datetime
    build: BuildInfo
    public_key: str
    url: HttpUrl
    maintenance: MaintenanceInfo


class NodeHealthCheckResponse(BaseModel):
    """Response model for node health check status."""
    status: Literal["OK"] = Field("OK", description="Health check status should always be 'OK'")
