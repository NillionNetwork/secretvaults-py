"""
API endpoint paths for Nildb client, matching the TypeScript NilDbEndpoint structure.
"""
# pylint: disable=invalid-name,disable=too-few-public-methods

class NilDbEndpoint:
    """Root of all API endpoint paths for the Nildb client, matching the TypeScript NilDbEndpoint structure."""

    class v1:
        """Version 1 API endpoints."""

        class builders:
            """Endpoints for builder operations."""

            register = '/v1/builders/register'
            me = '/v1/builders/me'

        class data:
            """Endpoints for data operations."""

            root = '/v1/data'
            find = '/v1/data/find'
            update = '/v1/data/update'
            delete = '/v1/data/delete'
            flushById = '/v1/data/:id/flush'
            tailById = '/v1/data/:id/tail'
            createOwned = '/v1/data/owned'
            createStandard = '/v1/data/standard'

        class queries:
            """Endpoints for query operations."""

            root = '/v1/queries'
            byId = '/v1/queries/:id'
            run = '/v1/queries/run'
            runById = '/v1/queries/run/:id'

        class collections:
            """Endpoints for collection operations."""

            root = '/v1/collections'
            byId = '/v1/collections/:id'
            indexesById = '/v1/collections/:id/indexes'
            indexesByNameById = '/v1/collections/:id/indexes/:name'

        class system:
            """Endpoints for system operations and health checks."""

            about = '/about'
            health = '/health'
            metrics = '/metrics'
            openApiJson = '/openapi.json'
            maintenanceStart = '/v1/system/maintenance/start'
            maintenanceStop = '/v1/system/maintenance/stop'
            logLevel = '/v1/system/log-level'

        class users:
            """Endpoints for user operations."""

            me = '/v1/users/me'

            class data:
                """Endpoints for user data operations."""

                root = '/v1/users/data'
                byId = '/v1/users/data/:collection/:document'
                aclById = '/v1/users/data/:collection/:document/acl'

                class acl:
                    """Endpoints for user data ACL operations."""

                    grant = '/v1/users/data/acl/grant'
                    revoke = '/v1/users/data/acl/revoke'

NilDbEndpointClass = NilDbEndpoint
NilDbEndpoint = NilDbEndpoint()
