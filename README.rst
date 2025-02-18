secretvaults
=============

Wrapper classes for simplifying usage of Nillion's Secret Vault and the nilQL encryption and decryption library.

|pypi| |readthedocs| |actions| |coveralls|

.. |pypi| image:: https://badge.fury.io/py/secretvaults.svg
   :target: https://badge.fury.io/py/secretvaults
   :alt: PyPI version and link.

.. |readthedocs| image:: https://readthedocs.org/projects/secretvaults/badge/?version=latest
   :target: https://secretvaults.readthedocs.io/en/latest/?badge=latest
   :alt: Read the Docs documentation status.

.. |actions| image:: https://github.com/nillionnetwork/nillion-sv-wrappers-py/workflows/lint-test-cover-docs/badge.svg
   :target: https://github.com/nillionnetwork/nillion-sv-wrappers-py/actions/workflows/lint-test-cover-docs.yml
   :alt: GitHub Actions status.

.. |coveralls| image:: https://coveralls.io/repos/github/NillionNetwork/secretvaults-py/badge.svg?branch=main
   :target: https://coveralls.io/github/NillionNetwork/secretvaults-py?branch=main
   :alt: Coveralls test coverage summary.

Description and Purpose
------------------------

This wrapper provides functions to simplify usage of Nillion's SecretVault and nilQL.


NilQLWrapper: Lightweight wrapper for encryption and decryption using nilQL
============================================================================

- Encrypts data into shares for distributed storage across nodes
- Handles structured data with ``%allot`` markers for selective encryption
- Recombines shares and decrypts data marked ``%share`` using unify
- Manages secret keys for encryption/decryption operations
- Recombines and decrypts shares to recover original data
- Maintains compatibility with SecretVault timestamps
- No node configuration required when used standalone


SecretVaultWrapper: Wrapper for Secret Vault API operations
============================================================

Authentication
--------------

- Handles JWT creation and management per node
- Manages node authentication automatically


Schema Operations
------------------

Create: Deploy schema across nodes (``/api/v1/schemas``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Creates schemas with optional custom ID
- Validates schema structure
- Distributes to all nodes


Read: List available schemas (``/api/v1/schemas``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Retrieves schema configurations
- Shows schema metadata and structure


Delete: Remove schema definition (``/api/v1/schemas``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Deletes schema across all nodes


Data Operations
----------------

Create: Upload data to the specified schema collection (``/api/v1/data/create``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Writes data to multiple nodes
- Encrypts specified fields with ``%allot`` markers before distribution
- Distributes encrypted shares marked ``%share`` across nodes


Read: Retrieve data from the specified schema collection (``/api/v1/data/read``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Retrieves data from all nodes
- Recombines encrypted shares marked ``%share`` from nodes to decrypt specified fields automatically
- Returns decrypted record


Update: Update data in the specified schema collection (``/api/v1/data/update``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Encrypts specified fields with ``%allot`` markers before distribution
- Distributes encrypted shares marked ``%share`` across nodes, updating existing records matching the provided filter


Delete: Delete data from the specified schema collection (``/api/v1/data/delete``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Deletes existing records on all nodes that match the provided filter


Flush: Remove all documents in a schema collection (``/api/v1/data/flush``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Removes all data across nodes from a schema collection


Query Operations
------------------

Create: Deploy query across nodes (``/api/v1/queries``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Creates query with optional custom ID
- Includes Mongo Aggregation Pipeline and custom Runtime Variables
- Distributes to all nodes


Execute: Runs the query across nodes (``/api/v1/queries/execute``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Executes the query on all nodes with the provided variables
- Recombines encrypted shares or aggregation results marked ``%share`` from nodes to decrypt specified fields automatically
- Returns decrypted record


Read: List available queries (``/api/v1/queries``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Retrieves available queries
- Shows query definition and structure


Delete: Remove query across nodes (``/api/v1/queries``)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

- Deletes query across all nodes


Installation and Usage
-----------------------

You can install the package using pip:

.. code-block:: bash

    pip install secretvaults


The library can be imported in the usual ways:

.. code-block:: python

    import secretvaults
    from secretvaults import *



Standalone NilQLWrapper Example
-------------------------------

An example workflow that demonstrates use of the wrapper is presented below:

Run examples:

.. code-block:: bash

    python3 examples/nilql_encryption.py

SecretVaultWrapper Example
---------------------------

Copy the ``.env.example`` to create a ``.env`` file that uses the example org:

.. code-block:: bash

    cp .env.example .env

Run example to encrypt and upload data to all nodes, then read data from nodes:

.. code-block:: bash

    python3 examples/data_create_read.py

Development
-----------

All installation and development dependencies are fully specified in ``pyproject.toml``. The ``project.optional-dependencies`` object is used to `specify optional requirements <https://peps.python.org/pep-0621>`__ for various development tasks. This makes it possible to specify additional options (such as ``docs``, ``lint``, and so on) when performing installation using `pip <https://pypi.org/project/pip>`__:

.. code-block:: bash

    python -m pip install ".[docs,lint]"

Documentation
-------------

The documentation can be generated automatically from the source files using `Sphinx <https://www.sphinx-doc.org>`__:

.. code-block:: bash

    python -m pip install ".[docs]"
    cd docs
    sphinx-apidoc -f -E --templatedir=_templates -o _source .. && make html

Testing and Conventions
------------------------

All unit tests are executed and their coverage is measured when using `pytest <https://docs.pytest.org>`__ (see the ``pyproject.toml`` file for configuration details):

.. code-block:: bash

    python -m pip install ".[test]"
    python -m pytest

The subset of the unit tests included in the module itself and can be executed using `doctest <https://docs.python.org/3/library/doctest.html>`__:

.. code-block:: bash

    python src/secretvaults/secret_vault_wrapper.py -v

Style conventions are enforced using `Pylint <https://pylint.readthedocs.io>`__:

.. code-block:: bash

    python -m pip install ".[lint]"
    python -m pylint src/secretvaults

Contributions
-------------

To contribute to the source code, open an issue or submit a pull request on the `GitHub page <https://github.com/nillionnetwork/secretvaults-py>`__ for this library.

Versioning
----------

The version number format for this library and the changes to the library associated with version number increments conform to `Semantic Versioning 2.0.0 <https://semver.org/#semantic-versioning-200>`__.

Publishing
----------

This library can be published as a `package on PyPI <https://pypi.org/project/secretvaults>`__ via the GitHub Actions workflow found in ``.github/workflows/build-publish-sign-release.yml`` that follows the `recommendations found in the Python Packaging User Guide <https://packaging.python.org/en/latest/guides/publishing-package-distribution-releases-using-github-actions-ci-cd-workflows/>`__.

Ensure that any links in this README document to the Read the Docs documentation of this package (or its dependencies) have appropriate version numbers. Also ensure that the Read the Docs project for this library has an `automation rule <https://docs.readthedocs.io/en/stable/automation-rules.html>`__ that activates and sets as the default all tagged versions.
