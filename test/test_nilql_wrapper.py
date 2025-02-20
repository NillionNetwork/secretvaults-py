import pytest
from unittest import TestCase
import nilql
from secretvaults import NilQLWrapper, OperationType, KeyType

SEED = "my_seed"


class TestNilQLWrapper(TestCase):
    """
    Test suite for NilQLWrapper functionalities.
    """

    def setUp(self):
        """
        Setup method to initialize NilQLWrapper instances before each test.
        """
        self.cluster_config = {"nodes": [{}, {}, {}]}  # Valid multi-node cluster

        # Valid multi-node cluster setup
        self.wrapper_store = NilQLWrapper(self.cluster_config, OperationType.STORE.value)
        self.wrapper_sum = NilQLWrapper(self.cluster_config, OperationType.SUM.value)
        self.wrapper_match = NilQLWrapper(self.cluster_config, OperationType.MATCH.value)

    def test_secret_key_initialization(self):
        """
        Test if secret key is properly generated.
        """
        self.assertIsInstance(self.wrapper_store.secret_key, nilql.SecretKey)
        self.assertIsInstance(self.wrapper_sum.secret_key, nilql.SecretKey)
        self.assertIsInstance(self.wrapper_match.secret_key, nilql.SecretKey)

    def test_secret_key_generation_with_seed(self):
        """
        Test secret key generation with a fixed seed.
        """
        wrapper_with_seed = NilQLWrapper(self.cluster_config, OperationType.STORE.value, secret_key_seed=SEED)
        self.assertIsInstance(wrapper_with_seed.secret_key, nilql.SecretKey)

    def test_invalid_secret_key_generation(self):
        """
        Test error handling during invalid secret key generation.
        """
        with pytest.raises(ValueError, match="valid cluster configuration is required"):
            NilQLWrapper(cluster=123, operation=OperationType.STORE.value)

        with pytest.raises(ValueError, match="cluster configuration must contain at least one node"):
            NilQLWrapper(cluster={"nodes": []}, operation=OperationType.STORE.value)

    def test_key_type_enum(self):
        """
        Test that KeyType enum values are correctly defined.
        """
        assert KeyType.CLUSTER.value == "cluster"
        assert KeyType.SECRET.value == "secret"

        # Ensure Enum members exist
        assert KeyType["CLUSTER"] == KeyType.CLUSTER
        assert KeyType["SECRET"] == KeyType.SECRET

        # Ensure invalid key type raises KeyError
        with pytest.raises(KeyError):
            KeyType["INVALID"]

        # Ensure direct instantiation with valid values works
        assert KeyType("cluster") == KeyType.CLUSTER
        assert KeyType("secret") == KeyType.SECRET

        # Ensure instantiation with an invalid value raises ValueError
        with pytest.raises(ValueError):
            KeyType("invalid_value")

    @pytest.mark.asyncio
    async def test_encrypt_decrypt_for_store(self):
        """
        Test encryption and decryption for the 'store' operation.
        """
        plaintext = 123
        encrypted = await self.wrapper_store.encrypt(plaintext)
        decrypted = await self.wrapper_store.decrypt(encrypted)

        assert decrypted == plaintext

    @pytest.mark.asyncio
    async def test_encrypt_decrypt_for_match(self):
        """
        Test encryption and decryption for the 'match' operation.
        """
        plaintext = "match_string"

        encrypted_one = await self.wrapper_match.encrypt(plaintext)
        encrypted_two = await self.wrapper_match.encrypt(plaintext)

        # Matching encryption should return the same result
        assert encrypted_one == encrypted_two

    @pytest.mark.asyncio
    async def test_encrypt_decrypt_for_sum(self):
        """
        Test encryption and decryption for the 'sum' operation.
        """
        plaintext = 100
        encrypted = await self.wrapper_sum.encrypt(plaintext)
        decrypted = await self.wrapper_sum.decrypt(encrypted)

        assert decrypted == plaintext

    @pytest.mark.asyncio
    async def test_encrypt_invalid_type(self):
        """
        Test encryption of an invalid data type.
        """
        with pytest.raises(ValueError, match="unsupported data type"):
            await self.wrapper_store.encrypt({"invalid": "data"})

    @pytest.mark.asyncio
    async def test_decrypt_invalid_shares(self):
        """
        Test decryption with invalid shares.
        """
        with pytest.raises(RuntimeError, match="Decryption failed"):
            await self.wrapper_store.decrypt(["invalid_share"])

    @pytest.mark.asyncio
    async def test_prepare_and_allot(self):
        """
        Test prepare_and_allot method for encrypting %allot fields.
        """
        data = {"user_info": {"%allot": "sensitive_data", "other_info": "non_sensitive_data"}}
        encrypted_data = await self.wrapper_store.prepare_and_allot(data)

        assert "%allot" in encrypted_data["user_info"]
        assert encrypted_data["user_info"]["%allot"] != "sensitive_data"

    @pytest.mark.asyncio
    async def test_unify(self):
        """
        Test unify method to recombine encrypted shares.
        """
        data = {"user_info": {"%allot": "sensitive_data", "other_info": "non_sensitive_data"}}
        encrypted_data = await self.wrapper_store.prepare_and_allot(data)
        decrypted_data = await self.wrapper_store.unify(encrypted_data)

        assert decrypted_data["user_info"]["%allot"] == "sensitive_data"

    @pytest.mark.asyncio
    async def test_uninitialized_wrapper_encrypt(self):
        """
        Test error handling when encrypting with an uninitialized NilQLWrapper.
        """
        uninitialized_wrapper = NilQLWrapper(self.cluster_config)
        uninitialized_wrapper.secret_key = None

        with pytest.raises(RuntimeError, match="NilQLWrapper not initialized"):
            await uninitialized_wrapper.encrypt("test")

    @pytest.mark.asyncio
    async def test_uninitialized_wrapper_decrypt(self):
        """
        Test error handling when decrypting with an uninitialized NilQLWrapper.
        """
        uninitialized_wrapper = NilQLWrapper(self.cluster_config)
        uninitialized_wrapper.secret_key = None

        with pytest.raises(RuntimeError, match="NilQLWrapper not initialized"):
            await uninitialized_wrapper.decrypt(["encrypted_data"])

    @pytest.mark.asyncio
    async def test_invalid_encryption_input(self):
        """
        Test encryption with inputs exceeding allowed limits.
        """
        with pytest.raises(ValueError, match="numeric plaintext must be a valid 32-bit signed integer"):
            await self.wrapper_store.encrypt(2**32)

        with pytest.raises(
            ValueError, match="string or binary plaintext must be possible to encode in 4096 bytes or fewer"
        ):
            await self.wrapper_store.encrypt("X" * 4097)

    @pytest.mark.asyncio
    async def test_invalid_decryption_key_mismatch(self):
        """
        Test decryption with a mismatched key.
        """
        other_wrapper = NilQLWrapper({"nodes": [{}, {}]}, OperationType.STORE.value)
        encrypted_data = await self.wrapper_store.encrypt(123)

        with pytest.raises(ValueError, match="cannot decrypt the supplied ciphertext using the supplied key"):
            await other_wrapper.decrypt(encrypted_data)

    @pytest.mark.asyncio
    async def test_secure_computation_workflow(self):
        """
        Test secure summation workflow.
        """
        plaintext_values = [100, 200, 300]

        encrypted_values = [await self.wrapper_sum.encrypt(v) for v in plaintext_values]

        combined_shares = [sum(x) % (2**32 + 15) for x in zip(*encrypted_values)]
        decrypted_sum = await self.wrapper_sum.decrypt(combined_shares)

        assert decrypted_sum == sum(plaintext_values)
