# coding: utf-8
__version__ = "0.17.1"


from .advanced_api import AdvancedApiSession

# from .dialect import get_sqlalchemy_table
from .exceptions import (
    OepApiException,
    OepAuthenticationException,
    OepClientSideException,
    OepServerSideException,
    OepTableAlreadyExistsException,
    OepTableNotFoundException,
)
from .oep_client import (
    DEFAULT_API_VERSION,
    DEFAULT_BATCH_SIZE,
    DEFAULT_HOST,
    DEFAULT_INSERT_RETRIES,
    DEFAULT_PROTOCOL,
    DEFAULT_SCHEMA,
    TOKEN_ENV_VAR,
    OepClient,
)

__all__ = [
    "AdvancedApiSession",
    # "get_sqlalchemy_table",
    "OepApiException",
    "OepAuthenticationException",
    "OepClientSideException",
    "OepServerSideException",
    "OepTableAlreadyExistsException",
    "OepTableNotFoundException",
    "DEFAULT_API_VERSION",
    "DEFAULT_BATCH_SIZE",
    "DEFAULT_HOST",
    "DEFAULT_INSERT_RETRIES",
    "DEFAULT_PROTOCOL",
    "DEFAULT_SCHEMA",
    "TOKEN_ENV_VAR",
    "OepClient",
]
