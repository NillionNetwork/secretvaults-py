"""
Common types used throughout the SecretVaults library.

This module provides centralized type definitions including Uuid, Did,
and ByNodeName for consistent type checking across the codebase.
"""

import warnings
from typing import Dict, TypeVar, NewType

from pydantic import RootModel, BaseModel, Field, field_validator

# Type aliases
Uuid = NewType("Uuid", str)

# Base58 alphabet used in multibase
BASE58_ALPHABET = "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz"


# Decode base58 string to bytes
def decode_base58(encoded: str) -> bytes:
    """Decode base58 string to bytes."""
    num = 0
    for char in encoded:
        if char not in BASE58_ALPHABET:
            raise ValueError(f"Invalid base58 character: {char}")
        num = num * 58 + BASE58_ALPHABET.index(char)

    # Convert number to bytes
    bytes_list = []
    while num > 0:
        bytes_list.insert(0, num & 0xFF)
        num >>= 8

    # Handle leading zeros (represented as '1' in base58)
    for char in encoded:
        if char != "1":
            break
        bytes_list.insert(0, 0)

    return bytes(bytes_list) if bytes_list else b"\x00"


# Convert bytes to hex string
def to_hex(data: bytes) -> str:
    """Convert bytes to hex string."""
    return data.hex()


# Helper to convert did:key to did:nil format
def convert_did_key_to_did_nil(did_key: str) -> str:
    """Convert did:key to did:nil format."""
    try:
        # did:key format: did:key:z<multibase-encoded-key>
        multibase_key = did_key[8:]  # Remove "did:key:"

        if not multibase_key.startswith("z"):
            raise ValueError("Expected multibase encoding type 'z' (base58)")

        # Decode the base58 key (skip the 'z' prefix)
        decoded_bytes = decode_base58(multibase_key[1:])

        # Skip the multicodec prefix (usually 2-3 bytes for key type)
        # For Ed25519: 0xed 0x01
        # For secp256k1: 0xe7 0x01
        public_key_hex = to_hex(decoded_bytes[2:])

        return f"did:nil:{public_key_hex}"
    except Exception as e:
        raise ValueError(f"Failed to convert did:key to did:nil: {e}") from e


class Did(BaseModel):
    """
    Decentralized Identifier (DID) for Nillion network.

    A branded type that loosely validates DIDs. Supports did:nil, did:key, and did:ethr formats.
    Automatically converts did:key to did:nil for backwards compatibility.
    """

    value: str = Field(..., min_length=1)

    @field_validator("value")
    @classmethod
    def validate_did(cls, v: str) -> str:
        """Validate and convert DID string."""
        if not v.startswith("did:"):
            raise ValueError("DID must start with 'did:'")

        if v.startswith("did:ethr:"):
            # Warn about ethr support but don't fail validation
            warnings.warn(
                "Received `did:ethr` which is not compatible with this version of secretvaults — upgrade to 1.0.0+.",
                UserWarning,
            )
            return v

        if v.startswith("did:nil:"):
            return v

        if v.startswith("did:key:"):
            # Validate format
            multibase_key = v[8:]  # Remove "did:key:"
            if not multibase_key or len(multibase_key) < 10:
                raise ValueError("Invalid did:key format - key portion too short")

            # Convert to did:nil
            try:
                return convert_did_key_to_did_nil(v)
            except Exception as e:
                raise ValueError(f"Invalid did:key format: {e}") from e

        # Unsupported DID method
        raise ValueError(f"Unsupported DID method. Expected did:nil, did:key, or did:ethr, but got: {v[:10]}...")

    def __str__(self) -> str:
        return self.value

    def __repr__(self) -> str:
        return f"Did(value='{self.value}')"

    def __hash__(self) -> int:
        return hash(self.value)

    def __eq__(self, other) -> bool:
        if isinstance(other, Did):
            return self.value == other.value
        if isinstance(other, str):
            return self.value == other
        return False


# Type variable for ByNodeName
T = TypeVar("T")


class ByNodeName(RootModel[Dict[Did, T]]):
    """
    Map type indexed by node DIDs.

    Used to store data associated with specific nodes in the network.
    """

    def __getitem__(self, key: Did) -> T:
        return self.root[key]

    def __setitem__(self, key: Did, value: T) -> None:
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

    def get(self, key: Did, default: T = None) -> T:
        """Get value by key with default fallback."""
        return self.root.get(key, default)

    def __contains__(self, key: Did) -> bool:
        """Check if key exists in the mapping."""
        return key in self.root
