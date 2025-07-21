"""
Base NIL DB client implementation.
"""

import asyncio
from typing import Any, Dict, Optional, TypeVar
from urllib.parse import urljoin

import aiohttp
from pydantic import BaseModel

from ..common.paths import NilDbEndpoint
from ..dto.system import ReadAboutNodeResponse
from ..logger import Log

T = TypeVar("T", bound=BaseModel)


class NilDbBaseClientOptions(BaseModel):
    """Options for NIL DB base client."""

    about: ReadAboutNodeResponse
    base_url: str


class AuthenticatedRequestOptions(BaseModel):
    """Options for authenticated requests."""

    path: str
    token: Optional[str] = None
    method: str = "GET"
    body: Optional[Dict[str, Any]] = None


class NilDbBaseClient:
    """Base NIL DB client implementation."""

    def __init__(self, options: NilDbBaseClientOptions):
        self._options = options
        self._session: Optional[aiohttp.ClientSession] = None

    @property
    def name(self) -> str:
        """Get the client name (last 4 chars of public key)."""
        return self._options.about.public_key[-4:]

    @property
    def id(self) -> str:
        """Get the client ID (DID)."""
        return f"did:nil:{self._options.about.public_key}"

    async def _get_session(self) -> aiohttp.ClientSession:
        """Get or create HTTP session."""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def _close_session(self):
        """Close HTTP session."""
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def close(self):
        """Close the client and cleanup resources."""
        await self._close_session()

    def _is_retryable_error(self, error: Exception) -> bool:
        """Determine if an error is retryable."""
        error_name = type(error).__name__
        error_message = str(error).lower()

        retryable_names = [
            "NetworkError",
            "AbortError",
            "TimeoutError",
            "ERR_NETWORK",
            "ECONNREFUSED",
            "ECONNRESET",
            "ETIMEDOUT",
            "ENOTFOUND",
            "EAI_AGAIN",
        ]

        if error_name in retryable_names:
            return True

        # Check error message for network-related issues
        if any(term in error_message for term in ["network", "fetch failed", "connection refused", "timeout"]):
            return True

        # Check if it's a response error with retryable status
        if hasattr(error, "status"):
            status = getattr(error, "status")
            return status >= 500 or status in [429, 408]

        return False

    async def _fetch_with_retry(  # pylint: disable=unused-argument
        self, endpoint: str, fetch_options: Dict[str, Any], context: str, max_retries: int = 5
    ) -> aiohttp.ClientResponse:
        """Execute a fetch request with retry logic for network failures."""
        session = await self._get_session()
        last_error: Optional[Exception] = None

        for attempt in range(1, max_retries + 1):
            try:
                return await session.request(**fetch_options)
            except Exception as error:  # pylint: disable=broad-exception-caught
                last_error = error

                if not self._is_retryable_error(error) or attempt == max_retries:
                    Log.debug(f"{context} failed permanently after {attempt} attempts: {error}")
                    raise error

                delay = min(1000 * (2 ** (attempt - 1)), 10000)  # Exponential backoff with max 10s
                Log.debug(f"{context} failed (attempt {attempt}/{max_retries}), retrying in {delay}ms: {error}")
                await asyncio.sleep(delay / 1000)  # Convert to seconds

        if last_error:
            raise last_error
        raise RuntimeError("Unexpected error in retry logic")

    def _handle_error_response(self, response: aiohttp.ClientResponse, method: str, path: str, body: Any) -> None:
        """Handle error responses with consistent error information."""
        error_message = f"Request failed: {method} {path}"
        if body:
            error_message += f" - Response body: {body}"

        raise aiohttp.ClientResponseError(
            request_info=response.request_info,
            history=response.history,
            status=response.status,
            message=error_message,
            headers=response.headers,
        )

    async def request(  # pylint: disable=too-many-return-statements,too-many-branches
        self, options: AuthenticatedRequestOptions, response_schema: Optional[type[T]] = None
    ) -> T:
        """Make an authenticated request to the NilDb API."""
        headers: Dict[str, str] = {}

        if options.token:
            headers["Authorization"] = f"Bearer {options.token}"

        if options.body:
            headers["Content-Type"] = "application/json"

        endpoint = urljoin(self._options.base_url, options.path)
        context = f"{options.method} {options.path}"

        fetch_options = {
            "method": options.method,
            "url": endpoint,
            "headers": headers,
        }

        if options.body:
            fetch_options["json"] = options.body

        response = await self._fetch_with_retry(endpoint, fetch_options, context)

        content_type = response.headers.get("content-type", "")
        status = response.status

        Log.debug(f"Response status: {status}, content-type: {content_type}")

        if "application/json" in content_type:
            json_data = await response.json()
            Log.debug(f"Response was application/json: {json_data}")

            if not response.ok:
                Log.error(f"HTTP {response.status} error for {options.method} {endpoint}")
                Log.error(f"Request body: {options.body}")
                Log.error(f"Response body: {json_data}")
                self._handle_error_response(response, options.method, endpoint, json_data)

            if response_schema is str:
                # If expecting a string, but got JSON, return as string
                return str(json_data)  # type: ignore
            if response_schema is None:
                return None  # type: ignore
            return response_schema.model_validate(json_data)

        if "text/plain" in content_type:
            text = await response.text()
            Log.debug(f"Response was text/plain: {text}")

            if not response.ok:
                Log.error(f"HTTP {response.status} error for {options.method} {endpoint}")
                Log.error(f"Request body: {options.body}")
                Log.error(f"Response body: {text}")
                self._handle_error_response(response, options.method, options.path, text)

            if response_schema is str:
                return text  # type: ignore
            if response_schema is None:
                return None  # type: ignore
            return response_schema.model_validate(text)

        # Check if response has content length
        content_length = response.headers.get("content-length", "0")
        Log.debug(f"Response had no body: {status}, content-length: {content_length}")

        if not response.ok:
            Log.error(f"HTTP {response.status} error for {options.method} {endpoint}")
            Log.error(f"Request body: {options.body}")
            Log.error(f"Response had no body, content-length: {content_length}")
            self._handle_error_response(response, options.method, options.path, None)

        if response_schema is str:
            return ""  # type: ignore
        if response_schema is None:
            return None  # type: ignore
        return response_schema.model_validate(None)

    async def about_node(self) -> ReadAboutNodeResponse:
        """Retrieve comprehensive node information including version and configuration."""
        return await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.system.about), ReadAboutNodeResponse
        )

    async def health_check(self) -> str:
        """Check node health status."""
        response = await self.request(
            AuthenticatedRequestOptions(path=NilDbEndpoint.v1.system.health), str  # Accept plain string response
        )
        return response

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self._close_session()
