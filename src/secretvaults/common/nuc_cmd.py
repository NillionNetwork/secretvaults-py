"""
NUC command constants for SecretVaults.
"""

from enum import Enum


class NucCmd(Enum):
    """NUC command constants."""

    # Nil DB commands
    NIL_DB_BUILDERS_READ = "nil.db.builders.read"
    NIL_DB_BUILDERS_UPDATE = "nil.db.builders.update"
    NIL_DB_BUILDERS_DELETE = "nil.db.builders.delete"
    NIL_DB_COLLECTIONS_CREATE = "nil.db.collections.create"
    NIL_DB_COLLECTIONS_READ = "nil.db.collections.read"
    NIL_DB_COLLECTIONS_DELETE = "nil.db.collections.delete"
    NIL_DB_COLLECTIONS_UPDATE = "nil.db.collections.update"
    NIL_DB_DATA_CREATE = "nil.db.data.create"
    NIL_DB_DATA_READ = "nil.db.data.read"
    NIL_DB_DATA_UPDATE = "nil.db.data.update"
    NIL_DB_DATA_DELETE = "nil.db.data.delete"
    NIL_DB_DATA_TAIL = "nil.db.data.tail"
    NIL_DB_QUERIES_CREATE = "nil.db.queries.create"
    NIL_DB_QUERIES_READ = "nil.db.queries.read"
    NIL_DB_QUERIES_DELETE = "nil.db.queries.delete"
    NIL_DB_QUERIES_RUN = "nil.db.queries.execute"
    NIL_DB_USERS_READ = "nil.db.users.read"
    NIL_DB_USERS_UPDATE = "nil.db.users.update"
    NIL_DB_USERS_DELETE = "nil.db.users.delete"

    @property
    def value(self) -> str:  # pylint: disable=invalid-overridden-method
        return self._value_  # pylint: disable=no-member
