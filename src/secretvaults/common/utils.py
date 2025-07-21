"""
Common utility functions.
"""

import time


def into_seconds_from_now(seconds: int) -> int:
    """Convert seconds from now to Unix timestamp."""
    return int((time.time() + seconds))
