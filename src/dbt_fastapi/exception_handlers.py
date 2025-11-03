"""
FastAPI exception handlers for converting domain exceptions to HTTP responses.

This module handles the translation between domain exceptions (DbtFastApiError)
and HTTP responses.
Keeps the business logic (DbtManager) decoupled from the HTTP layer.
"""

import logging
from fastapi import Request
from fastapi.responses import JSONResponse

from dbt_fastapi.exceptions import DbtFastApiError


logger = logging.getLogger(__name__)


async def dbt_error_handler(request: Request, exc: DbtFastApiError) -> JSONResponse:
    """
    Convert DbtFastApiError exceptions to properly formatted HTTP responses.

    This handler:
    1. Extracts the HTTP status code from the exception
    2. Builds a consistent error response structure
    3. Logs the error for observability
    4. Returns a JSONResponse with appropriate status code

    Args:
        request: The FastAPI request object
        exc: The domain exception that was raised

    Returns:
        JSONResponse with error details and appropriate status code
    """
    # Log the error with context
    logger.error(
        f"{type(exc).__name__}: {exc.message}",
        extra={
            "exception_type": type(exc).__name__,
            "http_status": exc.http_status_code,
            "details": exc.details,
            "path": request.url.path,
            "method": request.method,
        },
        exc_info=exc.original_exception if exc.original_exception else None,
    )

    # Build error response
    error_response = {
        "error": type(exc).__name__,
        "message": exc.message,
        **exc.details,
    }

    return JSONResponse(
        status_code=exc.http_status_code,
        content=error_response,
    )


async def generic_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Catch-all handler for unexpected exceptions.

    This provides a safety net for any exceptions that aren't caught by
    domain exception handlers.

    Args:
        request: The FastAPI request object
        exc: The unexpected exception

    Returns:
        JSONResponse with 500 status code
    """
    logger.exception(
        f"Unexpected error: {str(exc)}",
        extra={
            "exception_type": type(exc).__name__,
            "path": request.url.path,
            "method": request.method,
        },
    )

    return JSONResponse(
        status_code=500,
        content={
            "error": "InternalServerError",
            "message": "An unexpected error occurred",
            "details": {
                "suggestion": "Please try again. If the problem persists, contact support."
            },
        },
    )
