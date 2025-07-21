"""
Cryptographic keypair utilities for SecretVaults.

This module provides secp256k1-based keypair functionality including
key generation, signing, and DID (Decentralized Identifier) creation.
"""

import os
import hashlib
from binascii import unhexlify
from secp256k1 import PrivateKey as Secp256k1PrivateKey, PublicKey as Secp256k1PublicKey
from nuc.token import Did


class Keypair:
    """A cryptographic keypair for SecretVaults operations.

    Provides secp256k1-based keypair functionality with methods for
    signing, DID generation, and key management.
    """

    def __init__(self, privkey_bytes: bytes):
        """Initialize a keypair from private key bytes.

        Args:
            privkey_bytes: 32-byte private key

        Raises:
            ValueError: If private key is not exactly 32 bytes
        """
        if len(privkey_bytes) != 32:
            raise ValueError("Private key must be 32 bytes")

        # Store as secp256k1.PrivateKey instance
        self._private_key = Secp256k1PrivateKey(privkey=privkey_bytes, raw=True)
        self._public_key = self._private_key.pubkey  # secp256k1.PublicKey instance

    @classmethod
    def from_hex(cls, hex_key: str) -> "Keypair":
        """Create a keypair from a hexadecimal private key string.

        Args:
            hex_key: Private key as hex string

        Returns:
            Keypair instance
        """
        return cls(unhexlify(hex_key))

    @classmethod
    def from_bytes(cls, key_bytes: bytes) -> "Keypair":
        """Create a keypair from private key bytes.

        Args:
            key_bytes: Private key as bytes

        Returns:
            Keypair instance
        """
        return cls(key_bytes)

    @classmethod
    def generate(cls) -> "Keypair":
        """Generate a new random keypair.

        Returns:
            Keypair instance with cryptographically secure random private key
        """
        return cls(os.urandom(32))

    def private_key(self) -> Secp256k1PrivateKey:
        """Get the secp256k1 private key object.

        Returns:
            secp256k1.PrivateKey instance
        """
        return self._private_key

    def public_key(self) -> Secp256k1PublicKey:
        """Get the secp256k1 public key object.

        Returns:
            secp256k1.PublicKey instance
        """
        return self._public_key

    def private_key_hex(self) -> str:
        """Get the private key as a hexadecimal string.

        Returns:
            Private key as hex string
        """
        return self._private_key.private_key.hex()

    def public_key_hex(self, compressed=False) -> str:
        """Get the public key as a hexadecimal string.

        Args:
            compressed: Whether to return compressed format

        Returns:
            Public key as hex string
        """
        return self._public_key.serialize(compressed=compressed).hex()

    def matches_public_key(self, pk: bytes | str) -> bool:
        """Check if this keypair's public key matches the given key.

        Args:
            pk: Public key as bytes or hex string

        Returns:
            True if public keys match, False otherwise
        """
        ref = pk.hex() if isinstance(pk, bytes) else pk
        return self.public_key_hex() == ref

    def to_did(self) -> Did:
        """Create a DID (Decentralized Identifier) from the public key.

        Returns:
            Did object representing this keypair's identity
        """
        pubkey_bytes = self._public_key.serialize(compressed=True)
        return Did(pubkey_bytes)

    def to_did_string(self) -> str:
        """Get the DID as a string representation.

        Returns:
            DID string in format 'did:nil:<pubkey_hex>'
        """
        return str(self.to_did())

    def sign(self, msg: str, fmt: str = "hex") -> bytes | str:
        """Sign a message using ECDSA with secp256k1.

        Args:
            msg: Message to sign
            fmt: Output format - "hex" for hex string, "bytes" for bytes

        Returns:
            Signature as hex string or bytes
        """
        msg_bytes = msg.encode()
        digest = hashlib.sha256(msg_bytes).digest()
        sig_obj = self._private_key.ecdsa_sign_recoverable(digest)
        sig_bytes, rec_id = self._private_key.ecdsa_recoverable_serialize(sig_obj)
        full_sig = sig_bytes + bytes([rec_id])
        return full_sig if fmt == "bytes" else full_sig.hex()
