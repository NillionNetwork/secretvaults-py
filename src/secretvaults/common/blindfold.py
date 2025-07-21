"""
Blindfold encryption utilities for SecretVaults API.
"""

from typing import Union, Optional, Dict, Any, List
from enum import Enum
from blindfold import allot, ClusterKey, encrypt as blindfold_encrypt, SecretKey, unify as blindfold_unify

from ..logger import Log


class BlindfoldOperation(str, Enum):
    """Valid blindfold operations."""

    STORE = "store"
    MATCH = "match"
    SUM = "sum"


# Type aliases for better readability
BlindfoldKey = Union[SecretKey, ClusterKey]


class BlindfoldFactoryConfig:  # pylint: disable=too-few-public-methods
    """
    Defines valid configurations for creating or using a Blindfold encryption key.

    This class represents the union type from TypeScript with different scenarios:
    - Scenario 1: Use a pre-existing key
    - Scenario 2: Generate a SecretKey (allows seed)
    - Scenario 3: Generate a ClusterKey (disallows seed)
    """

    def __init__(
        self,
        key: Optional[BlindfoldKey] = None,
        operation: Optional[BlindfoldOperation] = None,
        seed: Optional[Union[bytes, str]] = None,
        use_cluster_key: Optional[bool] = None,
        threshold: Optional[int] = None,
    ):  # pylint: disable=too-many-arguments
        # Validate configuration based on scenarios
        if key is not None:
            # Scenario 1: Use pre-existing key
            if any(param is not None for param in [operation, seed, use_cluster_key, threshold]):
                raise ValueError("When using existing key, other parameters must be None")
            self.key = key
            self.operation = None
            self.seed = None
            self.use_cluster_key = None
            self.threshold = None
        elif operation is not None:
            # Scenarios 2 & 3: Generate key
            if use_cluster_key:
                # Scenario 3: Generate ClusterKey (disallows seed)
                if seed is not None:
                    raise ValueError("ClusterKey generation does not allow seed")
                if operation == BlindfoldOperation.SUM and threshold is None:
                    raise ValueError("SUM operation requires threshold for ClusterKey")
            else:
                # Scenario 2: Generate SecretKey (allows seed)
                if operation == BlindfoldOperation.SUM and threshold is None:
                    raise ValueError("SUM operation requires threshold for SecretKey")

            self.key = None
            self.operation = operation
            self.seed = seed
            self.use_cluster_key = use_cluster_key
            self.threshold = threshold
        else:
            raise ValueError("Must provide either key or operation")


async def to_blindfold_key(
    options: BlindfoldFactoryConfig,
    cluster_size: int,
) -> BlindfoldKey:
    """
    Create a blindfold key based on the provided configuration.

    Args:
        options: Configuration for key creation
        cluster_size: Number of nodes in the cluster

    Returns:
        SecretKey or ClusterKey based on configuration
    """
    Log.debug(
        {
            "hasExistingKey": options.key is not None,
            "operation": options.operation.value if options.operation else "existing-key",
            "clusterSize": cluster_size,
            "useClusterKey": options.use_cluster_key or False,
            "hasSeed": options.seed is not None,
        },
        "Creating blindfold key",
    )

    if options.key is not None:
        Log.debug({"keyType": type(options.key).__name__}, "Using existing key")
        return options.key

    operation = options.operation
    if operation is None:
        raise ValueError("Operation is required when not using existing key")

    op = {
        operation.value: True,
    }

    threshold = options.threshold
    cluster = {"nodes": [{} for _ in range(cluster_size)]}

    use_cluster_key = options.use_cluster_key or False
    use_seed = options.seed is not None
    is_cluster_key = use_cluster_key or (not use_seed and cluster_size > 1)

    key_type = "ClusterKey" if is_cluster_key else "SecretKey"

    if is_cluster_key:
        key = ClusterKey.generate(cluster, op, threshold)
    else:
        key = SecretKey.generate(
            cluster,
            op,
            threshold,
            options.seed,
        )

    Log.debug(
        {
            "key": key_type,
            "operation": operation.value,
            "threshold": threshold,
            "nodes": cluster_size,
        },
        "Key generated",
    )
    return key


async def encrypt(
    key: BlindfoldKey, plaintext: Union[int, str, bytes]
) -> Union[str, List[str], List[int], List[List[int]]]:
    """
    Encrypt a plaintext value using the blindfold library.

    Args:
        key: SecretKey or ClusterKey for encryption
        plaintext: Value to encrypt

    Returns:
        Encrypted value
    """
    return blindfold_encrypt(key, plaintext)


async def conceal(
    key: BlindfoldKey,
    data: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """
    Encrypts fields marked with `%allot` and then splits the object into an array of secret shares.

    Args:
        key: SecretKey or ClusterKey for encryption
        data: Data to conceal with fields marked with `%allot`

    Returns:
        Array of secret shares, one per node

    Example:
        data = [{
            "patientId": {"%allot": "user-123"},  # This value will be concealed
            "visitDate": "2025-06-24",            # This value will remain public
        }]

        # Output assuming 2 nodes:
        [
            # Document to be stored on Node 1
            {
                "patientId": {"%share": "<ciphertext_a_for_user-123>"},
                "visitDate": "2025-06-24",
            },
            # Document to be stored on Node 2
            {
                "patientId": {"%share": "<ciphertext_b_for_user-123>"},
                "visitDate": "2025-06-24",
            },
        ]
    """
    Log.debug(
        {
            "keyType": type(key).__name__,
            "dataKeys": list(data.keys()),
        },
        "Starting data concealment",
    )

    async def encrypt_deep(obj):
        if not isinstance(obj, (dict, list)):
            return obj

        if isinstance(obj, dict):
            encrypted = {}
            for k, value in obj.items():
                if isinstance(value, dict):
                    if "%allot" in value:
                        encrypted_value = await encrypt(key, value["%allot"])
                        encrypted[k] = {"%allot": encrypted_value}
                    else:
                        encrypted[k] = await encrypt_deep(value)
                elif isinstance(value, list):
                    encrypted[k] = await encrypt_deep(value)
                else:
                    encrypted[k] = value
            return encrypted
        # else:  # list
        encrypted = []
        for item in obj:
            if isinstance(item, dict) and "%allot" in item:
                encrypted_value = await encrypt(key, item["%allot"])
                encrypted.append({"%allot": encrypted_value})
            else:
                encrypted_item = await encrypt_deep(item)
                encrypted.append(encrypted_item)
        return encrypted

    encrypted_data = await encrypt_deep(data)

    # splits data into one record per-node where each node gets a secret share
    shares = allot(encrypted_data)

    Log.debug(
        {"type": type(key).__name__, "shares": len(shares)},
        "Data concealed",
    )

    return shares


async def unify(
    key: BlindfoldKey,
    shares: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Recombines an array of secret shares and decrypts the concealed values to restore the original object.

    Args:
        key: SecretKey or ClusterKey for decryption
        shares: Array of secret shares from different nodes

    Returns:
        Original data with concealed values revealed
    """
    return blindfold_unify(key, shares)


async def reveal(
    key: BlindfoldKey,
    shares: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Recombines an array of secret shares and decrypts the concealed values to restore the original object.

    Args:
        key: SecretKey or ClusterKey for decryption
        shares: Array of secret shares from different nodes

    Returns:
        Original data with concealed values revealed

    Example:
        shares = [
            {
                "patientId": {"%share": "<ciphertext_A_for_user-123>"},
                "visitDate": "2025-06-24",
            },
            {
                "patientId": {"%share": "<ciphertext_B_for_user-123>"},
                "visitDate": "2025-06-24",
            },
        ]

        # Output:
        {
            "patientId": "user-123",
            "visitDate": "2025-06-24",
        }
    """
    unified = await unify(key, shares)

    Log.debug(
        {
            "type": type(key).__name__,
            "keys": list(unified.keys()),
        },
        "Revealed data",
    )

    return unified
