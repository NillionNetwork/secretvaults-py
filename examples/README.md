# SecretVault Client Examples

This directory contains examples and documentation for the SecretVault Client, which provides a comprehensive interface for managing SecretVaults with automatic handling of concealed data.

## Table of Contents

- [Overview](#overview)
  - [Example Scripts](#example-scripts)
- [Operations](#operations)
  - [Builders](#builders)
  - [Collections](#collections)
  - [Data](#data)
  - [Users](#users)
  - [Queries](#queries)

## Overview

The `SecretVaultBuilderClient` is the main client for builders to manage their SecretVaults. It provides automatic handling of concealed data if configured and supports distributed operations across multiple nodes.

The `SecretVaultUserClient` enables end users to interact with their own owned data in SecretVaults. It supports creating, reading, updating, and sharing user-owned documents, as well as fine-grained access control and delegation. This client is essential for building user-facing applications that leverage SecretVaults' privacy and access control features.

**Blindfold encryption** allows you to automatically conceal and reveal sensitive fields in your data, ensuring privacy even across distributed nodes. When enabled, fields marked for concealment are encrypted and split into shares, and are automatically revealed when read.

**Distributed operations** ensure that all data and operations are executed in parallel across all configured nodes, providing redundancy, consistency, and resilience to node failures.

### Example Scripts

- `interactive_demo.py`: Interactive CLI for exploring all builder and user features.
- `standard_data_example.py`: Demonstrates the standard builder workflow for creating collections and managing standard data.
- `file_encryption_example.py`: Shows how to encrypt, store, and retrieve files using SecretVaults with blindfold encryption.
- `owned_data_example.py`: Demonstrates the full user/owned data flow, including access control, delegation, and sharing.

You can run any of these examples directly with `python examples/<script_name>.py`.

## Operations

### Builders

- `register()`: Register a new builder identity with the cluster.
- `refresh_root_token()`: Obtain or refresh the builder's root token, required for most operations.
- `subscription_status()`: Verify the builder's subscription status with the cluster.
- `read_profile()`: Retrieve the builder's profile, including collections and queries.
- `update_builder_profile()`: Update builder profile information.
- `delete_builder()`: Remove the builder and all associated resources.

### Collections

- `create_collection()`: Define and create a new collection with a JSON schema. Use type `owned` for user-specific data and access control.
- `read_collections()`: List all collections owned by the builder.
- `read_collection()`: Retrieve metadata for a specific collection.
- `delete_collection()`: Remove a collection and all its data.
- `create_collection_index()`: Add an index to a collection for faster queries.
- `drop_collection_index()`: Remove an index from a collection.

### Data

- `create_standard_data()`: Insert new standard data records into a collection.
- `find_data()`: Query and retrieve data records from a collection.
- `update_data()`: Update existing data records matching a filter.
- `delete_data()`: Delete data records matching a filter.
- `flush_data()`: Remove all data from a collection.
- `tail_data()`: Retrieve the most recent records from a collection.


### Users

- `create_owned_data()`: Users can create owned data in an owned collection using a delegation token from the builder.
- `list_data_references()`: List all references to a user's owned data.
- `read_data()`: Read a specific owned document by collection and document ID.
- `update_data()`: Update a user's owned document.
- `delete_data()`: Delete a user's owned document.
- `grant_access()`: Grant access to a document to another DID.
- `revoke_access()`: Revoke access from a DID.


### Queries

- `get_queries()`: Retrieve all queries defined for the builder.
- `get_query()`: Retrieve a specific query by its ID.
- `create_query()`: Define and create a new query for a collection.
- `delete_query()`: Remove a query by its ID.
- `run_query()`: Execute a query with optional variables and retrieve results.
- `read_query_run_results()`: Retrieve the results of a specific query run. 

