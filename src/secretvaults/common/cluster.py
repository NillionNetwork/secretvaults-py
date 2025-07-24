"""
Cluster utilities for executing operations across multiple nodes.
"""

import asyncio
import random
from typing import Any, Callable, Dict, List, TypeVar

from ..common.types import ByNodeName, Did
from ..common.blindfold import conceal, reveal
from ..logger import Log

T = TypeVar("T")


async def execute_on_cluster(clients: List[Any], operation: Callable[[Any], Any]) -> ByNodeName:
    """
    Execute an operation on all nodes in the cluster.

    Args:
        clients: List of node clients
        operation: Function to execute on each client

    Returns:
        Dictionary mapping node DIDs to operation results
    """
    tasks = []
    for client in clients:
        task = asyncio.create_task(operation(client))
        tasks.append((client.id, task))

    results = ByNodeName({})
    for node_id, task in tasks:
        try:
            result = await task
            results[node_id] = result
        except Exception as e:  # pylint: disable=broad-exception-caught
            error_body = getattr(e, "message", str(e))
            Log.error("Node operation failed", node_id=node_id, error=str(e), error_body=error_body)
            results[node_id] = e

    return results


def prepare_plaintext_request(options: Dict[str, Any]) -> Dict[Did, Any]:
    """
    Prepares a plaintext request by replicating the body for each node.

    Args:
        options: Dictionary containing 'clients' and 'body'

    Returns:
        Dictionary mapping node DIDs to request payloads
    """
    clients = options["clients"]
    body = options["body"]

    # Convert Pydantic model to dictionary if needed
    if hasattr(body, "model_dump") and not isinstance(body, dict):
        body_dict = body.model_dump(by_alias=True)
    elif hasattr(body, "dict") and not isinstance(body, dict):
        body_dict = body.dict(by_alias=True)
    else:
        body_dict = dict(body)

    Log.debug({"nodes": len(clients)}, "Preparing plaintext request")

    payloads = {}
    for client in clients:
        payloads[client.id] = {**body_dict}

    return payloads


async def prepare_concealed_request(options: Dict[str, Any]) -> Dict[Did, Any]:
    """
    Prepares a request by concealing its data for distribution to all nodes.

    Args:
        options: Dictionary containing 'key', 'clients', and 'body'

    Returns:
        Dictionary mapping node DIDs to encrypted payloads
    """
    key = options["key"]
    clients = options["clients"]
    body = options["body"]

    # Convert Pydantic model to dictionary if needed
    if hasattr(body, "model_dump") and not isinstance(body, dict):
        body_dict = body.model_dump(by_alias=True)
    elif hasattr(body, "dict") and not isinstance(body, dict):
        body_dict = body.dict(by_alias=True)
    else:
        body_dict = dict(body)

    # Determine if records are under 'data' key or at root level
    if "data" in body_dict and isinstance(body_dict["data"], list):
        # Records are under 'data' key (e.g., CreateStandardDataRequest)
        records = body_dict["data"]
        Log.debug(
            {
                "key": type(key).__name__,
                "nodes": len(clients),
                "documents": len(records),
            },
            "Preparing concealed data",
        )

        # 1. Conceal documents, eg: [[doc1_shareA, doc1_shareB], [doc2_shareA, doc2_shareB]].
        concealed_docs = await asyncio.gather(*[conceal(key, d) for d in records])

        # Ensure the number of shares matches the number of clients/nodes.
        if len(concealed_docs[0]) != len(clients):
            if len(concealed_docs[0]) == 1:
                return prepare_plaintext_request(options)
            Log.error(
                "Concealed shares count mismatch",
                shares=len(concealed_docs[0]) if concealed_docs else 0,
                nodes=len(clients),
            )
            raise ValueError("Concealed shares count mismatch.")

        # 2. Transpose the results from a document-major to a node-major structure.
        # We now have an array where the top-level index corresponds to the client index.
        # Result: [[doc1_shareA, doc2_shareA], [doc1_shareB, doc2_shareB]]
        shares_by_node = [
            [concealed_docs[doc_idx][node_idx] for doc_idx in range(len(concealed_docs))]
            for node_idx in range(len(clients))
        ]

        # 3. Map to pairs of [Did, payload] for conversion into a ByNodeName object.
        payloads = {}
        for client_idx, client in enumerate(clients):
            payload = {**body_dict, "data": shares_by_node[client_idx]}
            payloads[client.id] = payload

        Log.debug("Concealed data prepared")
        return payloads

    # Records are at root level (e.g., UpdateDataRequest, DeleteDataRequest)
    # Conceal the entire body as a single record
    Log.debug(
        "Preparing concealed data at root level",
        key=type(key).__name__,
        nodes=len(clients),
        request_type="root-level-concealment",
    )

    # Conceal the entire body as a single record
    concealed_shares = await conceal(key, body_dict)

    # Ensure the number of shares matches the number of clients/nodes.
    if len(concealed_shares) != len(clients):
        Log.error(
            "Concealed shares count mismatch",
            shares=len(concealed_shares),
            nodes=len(clients),
        )
        raise ValueError("Concealed shares count mismatch.")

    # Map to pairs of [Did, payload] for conversion into a ByNodeName object.
    payloads = {}
    for client_idx, client in enumerate(clients):
        payloads[client.id] = concealed_shares[client_idx]

    Log.debug("Concealed data prepared")
    return payloads


def process_plaintext_response(results: ByNodeName, strategy: str = "first") -> Any:
    """
    Selects a single canonical response from a map of node results.

    Args:
        results: Dictionary of results by node
        strategy: Strategy for selecting response ("first" or "random")

    Returns:
        Selected response
    """
    values = list(results.values())

    Log.debug(
        "Processing plaintext response",
        nodes=len(values),
        strategy=strategy,
    )

    # 1. Determine the index based on the chosen strategy.
    index = 0  # Default to 'first'
    if strategy == "random":

        index = random.randint(0, len(values) - 1)

    # 2. Select the result using the determined index.
    selected = values[index] if index < len(values) else None

    # 3. Safeguard
    if selected is None:
        Log.error("No response to select", resultsCount=len(values))
        raise ValueError("Failed to select a canonical response.")
    Log.debug("Response selected", selectedIndex=index)
    return selected


async def process_concealed_list_response(options: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Processes and reveals a list of documents from a cluster response.

    Args:
        options: Dictionary containing 'key' and 'resultsByNode'

    Returns:
        List of revealed documents
    """
    key = options["key"]
    results_by_node = options["resultsByNode"]

    Log.debug(
        "Processing concealed list response",
        key=type(key).__name__,
        nodes=len(results_by_node),
    )

    # 1. Flatten responses into an array of document shares.
    all_shares = []
    for response in results_by_node.values():
        if hasattr(response, "data"):
            all_shares.extend(response.data)
        else:
            all_shares.extend(response)

    Log.debug("Flattened document shares", totalShares=len(all_shares))

    # 2. Group shares by their id.
    grouped_shares = {}
    for doc in all_shares:
        doc_id = doc.get("_id") if isinstance(doc, dict) else getattr(doc, "_id", None)
        if doc_id:
            if doc_id not in grouped_shares:
                grouped_shares[doc_id] = []
            grouped_shares[doc_id].append(doc)

    Log.debug(
        "Grouped shares by document ID",
        documentCount=len(grouped_shares),
    )

    # 3. Create an array of reveal promises, one for each document group.
    reveal_promises = [reveal(key, shares) for shares in grouped_shares.values()]

    # 4. Await all reveal operations to run in parallel for maximum efficiency.
    revealed = await asyncio.gather(*reveal_promises)
    Log.debug(
        "Documents revealed successfully",
        revealedCount=len(revealed),
    )

    return revealed


async def process_concealed_object_response(options: Dict[str, Any]) -> Dict[str, Any]:
    """
    Processes and reveals a single document from a cluster response.

    Args:
        options: Dictionary containing 'key' and 'resultsByNode'

    Returns:
        Revealed document
    """
    key = options["key"]
    results_by_node = options["resultsByNode"]

    Log.debug(
        "Processing concealed object response",
        key=type(key).__name__,
        nodes=len(results_by_node),
    )

    shares = []

    for response in results_by_node.values():
        if hasattr(response, "data"):
            shares.append(response.data)
        else:
            shares.append(response)

    Log.debug("Collected object shares", shareCount=len(shares))

    revealed = await reveal(key, shares)
    Log.debug("Object revealed successfully")

    return revealed
