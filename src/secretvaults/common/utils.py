"""
Common utility functions.
"""

import time
import uuid
from typing import Union, Any, Dict


def into_seconds_from_now(seconds: int) -> int:
    """Convert seconds from now to Unix timestamp."""
    return int((time.time() + seconds))


def inject_ids_into_records(body: Union[Dict, Any]) -> Dict[str, Any]:
    """
    Ensures every record in 'data' has an '_id' field. If missing, a UUID is assigned.

    Args:
        body: A Pydantic model containing a 'data' key with a list of records.

    Returns:
        A dictionary version of the body with '_id' injected where missing.
    """
    create_body = body.model_dump()
    records = create_body.get("data", [])
    if not isinstance(records, list):
        raise ValueError(f"Expected 'data' to be a list, got: {type(records)}")

    for record in records:
        if not isinstance(record, dict):
            raise ValueError(f"Each record must be a dict, got: {type(record)} -> {record}")
        if "_id" not in record:
            record["_id"] = str(uuid.uuid4())

    return create_body
