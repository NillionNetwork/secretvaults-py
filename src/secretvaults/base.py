"""
Base classes for SecretVault clients.
"""

from abc import ABC
from typing import Any, Dict, Generic, List, Optional, TypeVar

from nuc.token import Did
from pydantic import BaseModel, ConfigDict

from .common.keypair import Keypair
from .common.types import ByNodeName
from .common.cluster import execute_on_cluster, prepare_concealed_request, prepare_plaintext_request
from .logger import Log

T_CLIENT = TypeVar("T_CLIENT")  # pylint: disable=invalid-name


class SecretVaultBaseOptions(BaseModel, Generic[T_CLIENT]):
    """Common constructor options for all SecretVault clients."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    keypair: Keypair
    clients: List[T_CLIENT]
    key: Optional[Any] = None  # SecretKey or ClusterKey from blindfold


class SecretVaultBaseClient(ABC, Generic[T_CLIENT]):
    """Provides common properties and methods for SecretVault clients."""

    def __init__(self, options: SecretVaultBaseOptions[T_CLIENT]) -> None:
        self._options = options

    @property
    def id(self) -> str:
        """The DID of the keypair as a string."""
        return str(self.did)

    @property
    def did(self) -> Did:
        """The DID of the keypair associated with this client."""
        return self._options.keypair.to_did()

    @property
    def nodes(self) -> List[T_CLIENT]:
        """The array of underlying node clients for the cluster."""
        return self._options.clients

    @property
    def keypair(self) -> Keypair:
        """The keypair used by this client for signing."""
        return self._options.keypair

    async def read_cluster_info(self) -> ByNodeName:
        """Retrieves information about each node in the cluster."""
        result = await execute_on_cluster(self.nodes, lambda c: c.about_node())
        Log.info({"nodes": len(result)}, "Cluster info retrieved")
        return result

    async def _prepare_node_payloads(self, body: Any) -> Dict[Did, Any]:
        """
        Prepare request payloads for all nodes (concealed or plaintext based on key configuration).
        
        Args:
            body: Request body to prepare
            
        Returns:
            Dictionary mapping node DIDs to their payloads
        """
        if self._options.key:
            return await prepare_concealed_request({"key": self._options.key, "clients": self.nodes, "body": body})
        return prepare_plaintext_request({"clients": self.nodes, "body": body})
