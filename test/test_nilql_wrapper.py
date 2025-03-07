"""Test suite for NilQLWrapper"""

import pytest
import nilql
from secretvaults import NilQLWrapper, OperationType, KeyType

SEED = "my_seed"


@pytest.fixture
def cluster_config():
    """Returns a valid multi-node cluster configuration."""
    return {"nodes": [{}, {}, {}]}


@pytest.fixture
def wrapper_store(cluster_config):
    """Fixture for NilQLWrapper with STORE operation."""
    return NilQLWrapper(cluster_config, OperationType.STORE)


@pytest.fixture
def wrapper_sum(cluster_config):
    """Fixture for NilQLWrapper with SUM operation."""
    return NilQLWrapper(cluster_config, OperationType.SUM)


@pytest.fixture
def wrapper_match(cluster_config):
    """Fixture for NilQLWrapper with MATCH operation."""
    return NilQLWrapper(cluster_config, OperationType.MATCH.value)


def test_secret_key_initialization(wrapper_store, wrapper_sum, wrapper_match):
    """Test if secret key is properly generated."""
    assert isinstance(wrapper_store.secret_key, nilql.SecretKey)
    assert isinstance(wrapper_sum.secret_key, nilql.SecretKey)
    assert isinstance(wrapper_match.secret_key, nilql.SecretKey)


def test_secret_key_generation_with_seed(cluster_config):
    """Test secret key generation with a fixed seed."""
    wrapper_with_seed = NilQLWrapper(cluster_config, OperationType.STORE, secret_key_seed=SEED)
    assert isinstance(wrapper_with_seed.secret_key, nilql.SecretKey)


def test_invalid_secret_key_generation():
    """Test error handling during invalid secret key generation."""
    with pytest.raises(ValueError, match="valid cluster configuration is required"):
        NilQLWrapper(cluster=123, operation=OperationType.STORE)

    with pytest.raises(ValueError, match="cluster configuration must contain at least one node"):
        NilQLWrapper(cluster={"nodes": []}, operation=OperationType.STORE)


def test_key_type_enum():
    """Test KeyType enum values."""
    assert KeyType.CLUSTER == "cluster"
    assert KeyType.SECRET == "secret"

    assert KeyType["CLUSTER"] == KeyType.CLUSTER
    assert KeyType["SECRET"] == KeyType.SECRET

    with pytest.raises(KeyError):
        KeyType["INVALID"]

    assert KeyType("cluster") == KeyType.CLUSTER
    assert KeyType("secret") == KeyType.SECRET

    with pytest.raises(ValueError):
        KeyType("invalid_value")


@pytest.mark.asyncio
async def test_encrypt_decrypt_for_store(wrapper_store):
    """Test encryption and decryption for the 'store' operation."""
    plaintext = 123
    encrypted = await wrapper_store.encrypt(plaintext)
    decrypted = await wrapper_store.decrypt(encrypted)
    assert decrypted == plaintext


@pytest.mark.asyncio
async def test_encrypt_decrypt_for_sum(wrapper_sum):
    """Test encryption and decryption for the 'sum' operation."""
    plaintext = 100
    encrypted = await wrapper_sum.encrypt(plaintext)
    decrypted = await wrapper_sum.decrypt(encrypted)
    assert decrypted == plaintext


@pytest.mark.asyncio
async def test_decrypt_invalid_shares(wrapper_store):
    """Test decryption with invalid shares."""
    with pytest.raises(RuntimeError, match="Decryption failed"):
        await wrapper_store.decrypt(["invalid_share"])


@pytest.mark.asyncio
async def test_prepare_and_allot(wrapper_store):
    """Test prepare_and_allot method for encrypting %allot fields."""

    # Test simple dictionary encryption
    data_dict = {"user_info": {"%allot": "sensitive_data", "other_info": "non_sensitive_data"}}
    encrypted_dict = await wrapper_store.prepare_and_allot(data_dict)

    assert isinstance(encrypted_dict, list), "prepare_and_allot should return a list"
    assert "%share" in encrypted_dict[0]["user_info"], "Encrypted key should be present"
    assert encrypted_dict[0]["user_info"]["%share"] != "sensitive_data", "Value should be encrypted"

    # Test list containing %allot
    data_list = [{"%allot": "list_sensitive"}, {"public": "safe_data"}]
    encrypted_list = await wrapper_store.prepare_and_allot(data_list)
    print(encrypted_list)
    assert isinstance(encrypted_list, list), "prepare_and_allot should return a list"
    assert "%share" in encrypted_list[0][0], "Encrypted list element should have '%share'"
    assert encrypted_list[0][0]["%share"] != "list_sensitive", "List data should be encrypted"

    # Test deeply nested dictionary structure
    deep_nested_data = {
        "level1": {
            "level2": {
                "sensitive": {"%allot": "deep_data"},
                "public_info": "visible",
            }
        }
    }
    encrypted_deep_nested = await wrapper_store.prepare_and_allot(deep_nested_data)

    assert isinstance(encrypted_deep_nested, list), "prepare_and_allot should return a list"
    assert (
        "%share" in encrypted_deep_nested[0]["level1"]["level2"]["sensitive"]
    ), "Deeply nested allot should be encrypted"
    assert (
        encrypted_deep_nested[0]["level1"]["level2"]["sensitive"]["%share"] != "deep_data"
    ), "Deep data should be encrypted"
    assert encrypted_deep_nested[0]["level1"]["level2"]["public_info"] == "visible", "Public data should be unchanged"

    # Test missing secret key should raise RuntimeError
    wrapper_store.secret_key = None
    with pytest.raises(RuntimeError, match="NilQLWrapper not initialized"):
        await wrapper_store.prepare_and_allot(data_dict)


@pytest.mark.asyncio
async def test_unify(wrapper_store):
    """Test unify method to recombine encrypted shares."""
    data = {"user_info": {"%allot": "sensitive_data", "other_info": "non_sensitive_data"}}
    encrypted_data = await wrapper_store.prepare_and_allot(data)
    decrypted_data = await wrapper_store.unify(encrypted_data)

    assert decrypted_data["user_info"] == "sensitive_data"


@pytest.mark.asyncio
async def test_uninitialized_wrapper_encrypt(cluster_config):
    """Test error handling when encrypting with an uninitialized NilQLWrapper."""
    uninitialized_wrapper = NilQLWrapper(cluster_config)
    uninitialized_wrapper.secret_key = None

    with pytest.raises(RuntimeError, match="NilQLWrapper not initialized"):
        await uninitialized_wrapper.encrypt("test")


@pytest.mark.asyncio
async def test_uninitialized_wrapper_decrypt(cluster_config):
    """Test error handling when decrypting with an uninitialized NilQLWrapper."""
    uninitialized_wrapper = NilQLWrapper(cluster_config)
    uninitialized_wrapper.secret_key = None

    with pytest.raises(RuntimeError, match="NilQLWrapper not initialized"):
        await uninitialized_wrapper.decrypt(["encrypted_data"])


@pytest.mark.asyncio
async def test_invalid_encryption_input(wrapper_store):
    """Test encryption with inputs exceeding allowed limits."""
    with pytest.raises(RuntimeError, match="numeric plaintext must be a valid 32-bit signed integer"):
        await wrapper_store.encrypt(2**32)

    with pytest.raises(
        RuntimeError, match="string or binary plaintext must be possible to encode in 4096 bytes or fewer"
    ):
        await wrapper_store.encrypt("X" * 4097)


@pytest.mark.asyncio
async def test_secure_computation_workflow(wrapper_sum):
    """Test secure summation workflow."""
    plaintext_values = [100, 200, 300]
    encrypted_values = [await wrapper_sum.encrypt(v) for v in plaintext_values]

    combined_shares = [sum(x) % (2**32 + 15) for x in zip(*encrypted_values)]
    decrypted_sum = await wrapper_sum.decrypt(combined_shares)

    assert decrypted_sum == sum(plaintext_values)
