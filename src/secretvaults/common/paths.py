"""
API endpoint paths for Nildb client, matching the TypeScript NilDbEndpoint structure.
"""


class NilDbEndpoint:  # pylint: disable=too-few-public-methods
    """Root of all API endpoint paths for the Nildb client, matching the TypeScript NilDbEndpoint structure."""

    class v1:  # pylint: disable=invalid-name,too-few-public-methods
        """Version 1 API endpoints."""

        class builders:  # pylint: disable=invalid-name,too-few-public-methods
            """Endpoints for builder operations."""

            register = "/v1/builders/register"
            me = "/v1/builders/me"

        class data:  # pylint: disable=invalid-name,too-few-public-methods
            """Endpoints for data operations."""

            root = "/v1/data"
            find = "/v1/data/find"
            update = "/v1/data/update"
            delete = "/v1/data/delete"
            flushById = "/v1/data/:id/flush"
            tailById = "/v1/data/:id/tail"
            createOwned = "/v1/data/owned"
            createStandard = "/v1/data/standard"

        class queries:  # pylint: disable=invalid-name,too-few-public-methods
            """Endpoints for query operations."""

            root = "/v1/queries"
            byId = "/v1/queries/:id"
            run = "/v1/queries/run"
            runById = "/v1/queries/run/:id"

        class collections:  # pylint: disable=invalid-name,too-few-public-methods
            """Endpoints for collection operations."""

            root = "/v1/collections"
            byId = "/v1/collections/:id"
            indexesById = "/v1/collections/:id/indexes"
            indexesByNameById = "/v1/collections/:id/indexes/:name"

        class system:  # pylint: disable=invalid-name,too-few-public-methods
            """Endpoints for system operations and health checks."""

            about = "/about"
            health = "/health"
            metrics = "/metrics"
            openApiJson = "/openapi.json"
            maintenanceStart = "/v1/system/maintenance/start"
            maintenanceStop = "/v1/system/maintenance/stop"
            logLevel = "/v1/system/log-level"

        class users:  # pylint: disable=invalid-name,too-few-public-methods
            """Endpoints for user operations."""

            me = "/v1/users/me"

            class data:  # pylint: disable=invalid-name,too-few-public-methods
                """Endpoints for user data operations."""

                root = "/v1/users/data"
                byId = "/v1/users/data/:collection/:document"
                aclById = "/v1/users/data/:collection/:document/acl"

                class acl:  # pylint: disable=invalid-name,too-few-public-methods
                    """Endpoints for user data ACL operations."""

                    grant = "/v1/users/data/acl/grant"
                    revoke = "/v1/users/data/acl/revoke"


NilDbEndpoint = NilDbEndpoint()
