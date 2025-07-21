"""
Common types used throughout the SecretVaults library.

This module provides centralized type definitions including Uuid, Did,
and ByNodeName for consistent type checking across the codebase.
"""

from typing import Dict, NewType, Any

from pydantic import RootModel

# Type aliases
Uuid = NewType("Uuid", str)
Did = NewType("Did", str)


class ByNodeName(RootModel[Dict[Did, Any]]):
    """A dictionary mapping node DIDs to values."""

    def __getitem__(self, key: Did) -> Any:
        return self.root[key]

    def __setitem__(self, key: Did, value: Any) -> None:
        self.root[key] = value

    def __len__(self) -> int:
        return len(self.root)

    def keys(self):
        """Return the keys of the underlying dictionary."""
        return self.root.keys()

    def values(self):
        """Return the values of the underlying dictionary."""
        return self.root.values()

    def items(self):
        """Return the items of the underlying dictionary."""
        return self.root.items()
