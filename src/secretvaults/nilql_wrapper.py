""" NilQLWrapper provides encryption and decryption of data using Nillion's technology. """
from enum import Enum
from typing import Optional, Union, Sequence

import nilql

NIQL_INIT_ERROR = "NilQLWrapper not initialized. Call init() first."


class KeyType(Enum):
    """Define an enum for key types"""

    CLUSTER = "cluster"
    SECRET = "secret"


class OperationType(Enum):
    """Define an enum for operations"""

    STORE = "store"
    SUM = "sum"
    MATCH = "match"


class NilQLWrapper:
    """
    NilQLWrapper provides encryption and decryption of data using Nillion's technology.
    It generates and manages secret keys, splits data into shares when encrypting,
    and recombines shares when decrypting.

    Attributes:
        cluster (dict): Cluster-related information needed for key generation.
        operation (dict): The type of operation the encrypted data will be used for.
        secret_key (Optional[SecretKey]): A pre-generated secret key.
        key_type (KeyType): Specifies whether to use a "CLUSTER" or "SECRET".
    """

    def __init__(
        self,
        cluster: dict,
        operation: str = OperationType.STORE.value,
        secret_key: Optional[nilql.SecretKey] = None,
        key_type: KeyType = KeyType.CLUSTER,
    ):
        self.cluster = cluster
        self.secret_key = secret_key
        self.operation = {operation: True}
        self.key_type = key_type

        # Initialize the appropriate key if not provided
        if self.secret_key is None:
            if self.key_type == KeyType.SECRET:
                self.secret_key = nilql.SecretKey.generate(self.cluster, self.operation)
            elif self.key_type == KeyType.CLUSTER:
                self.secret_key = nilql.ClusterKey.generate(self.cluster, self.operation)

    async def encrypt(self, data: Union[int, str]) -> Union[str, Sequence[str], Sequence[int]]:
        """
        Encrypts the provided data using the initialized secret key and splits it into encrypted shares.

        Args:
            data (Union[int, str]): The data to be encrypted.

        Returns:
            Union[str, Sequence[str], Sequence[int]]: A list of encrypted data shares.

        Raises:
            RuntimeError: If the secret key has not been initialized, or if encryption fails due to
                          any other errors during the encryption process.

        Example:
            encrypted_shares = await wrapper.encrypt(data)
        """
        if not self.secret_key:
            raise RuntimeError(NIQL_INIT_ERROR)
        try:
            encrypted_shares = nilql.encrypt(self.secret_key, data)
            return list(encrypted_shares)
        except Exception as e:
            raise RuntimeError(f"Encryption failed: {str(e)}") from e

    async def decrypt(self, shares: Union[str, Sequence[str], Sequence[int]]) -> Union[bytes, int]:
        """
        Decrypts the provided encrypted data shares using the initialized secret key.

        Args:
            shares (Union[str, Sequence[str], Sequence[int]]): A list of encrypted data shares to be decrypted.

        Returns:
            bytes | int: The decrypted data, which can either be an integer or string,
                       depending on the original data format.

        Raises:
            RuntimeError: If the secret key has not been initialized, or if decryption fails
                          due to any other errors during the decryption process.

        Example:
            decrypted_data = await wrapper.decrypt(encrypted_shares)
        """
        if not self.secret_key:
            raise RuntimeError(NIQL_INIT_ERROR)
        try:
            decrypted_data = nilql.decrypt(self.secret_key, shares)
            return decrypted_data
        except Exception as e:
            raise RuntimeError(f"Decryption failed: {str(e)}") from e

    async def prepare_and_allot(
        self, data: Union[int, bool, str, list, dict]
    ) -> Sequence[Union[int, bool, str, list, dict]]:
        """
        Recursively encrypts all values marked with %allot in the given data object
        and prepares it for secure processing, returning the encrypted data in allotted shares.

        Args:
            data (Union[int, bool, str, list, dict]): The data object to be processed.

        Returns:
            Sequence[Union[int, bool, str, list, dict]]: A sequence of the encrypted data. The structure of the returned
                                                        data mirrors the
                                                        original input, but with the "%allot" values
                                                        replaced with their encrypted counterparts.

        Raises:
            RuntimeError: If the secret key has not been initialized, or if encryption or allotment
                          fails due to any other errors during the process.

        Example:
            data = {
                "user_info": {
                    "%allot": "sensitive_data",
                    "other_info": "non_sensitive_data"
                }
            }
            encrypted_data = await wrapper.prepare_and_allot(data)
        """
        if not self.secret_key:
            raise RuntimeError(NIQL_INIT_ERROR)

        async def encrypt_deep(obj):
            if not isinstance(obj, (dict, list)):
                return obj

            encrypted = [] if isinstance(obj, list) else {}

            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, dict):
                        if "%allot" in value:
                            encrypted_value = await self.encrypt(value["%allot"])
                            encrypted[key] = {"%allot": encrypted_value}
                        else:
                            encrypted[key] = await encrypt_deep(value)
                    else:
                        encrypted[key] = value
            else:  # list
                for item in obj:
                    encrypted_item = await encrypt_deep(item)
                    encrypted.append(encrypted_item)

            return encrypted

        encrypted_data: Union[int, bool, str, list, dict] = await encrypt_deep(data)
        return nilql.allot(encrypted_data)

    async def unify(self, shares: Sequence[Union[int, bool, str, list, dict]]) -> Union[int, bool, str, list, dict]:
        """
        Recombines the encrypted shares back into the original data structure using the initialized secret key.

        Args:
            shares (Sequence[Union[int, bool, str, list, dict]]): A sequence of encrypted data shares that
                                                                  need to be recombined.
        Returns:
            Union[int, bool, str, list, dict]: The recombined and decrypted data, restored to its original
                                               structure.

        Raises:
            RuntimeError: If the secret key has not been initialized, or if the process of recombining
                          the shares fails due to any other errors.

        Example:
            encrypted_shares = [share1, share2, share3]
            original_data = await wrapper.unify(encrypted_shares)
        """
        if not self.secret_key:
            raise RuntimeError(NIQL_INIT_ERROR)

        return nilql.unify(self.secret_key, shares)
