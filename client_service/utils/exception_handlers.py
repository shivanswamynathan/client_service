import logging

from client_service.api.constants.status_codes import StatusCode
from fastapi import HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

logger = logging.getLogger(__name__)


async def http_exception_handler(request: Request, exc: HTTPException) -> JSONResponse:
    """
    Handle all HTTPException errors (e.g., 401, 403, 404)
    and return uniform APIResponse format.
    """
    # Log authentication/authorization issues differently for clarity
    if exc.status_code in (401, 403):
        logger.warning(
            f"Auth Exception: {exc.status_code} - {exc.detail} - Path: {request.url.path}"
        )
    else:
        logger.warning(
            f"HTTP Exception: {exc.status_code} - {exc.detail} - Path: {request.url.path}"
        )

    return JSONResponse(
        status_code=exc.status_code,
        content={"success": False, "message": exc.detail or "HTTP Error", "data": None},
    )


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    """
    Handle FastAPI validation errors (422) and return uniform format.
    """
    logger.warning(
        f"Validation Error - Path: {request.url.path} - Details: {exc.errors()}"
    )
    return JSONResponse(
        status_code=StatusCode.UNPROCESSABLE_ENTITY,
        content={"success": False, "message": "Validation error", "data": exc.errors()},
    )


async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """
    Handle all unhandled exceptions and return uniform APIResponse format.
    """
    # If an HTTPException somehow slips past, pass it to http_exception_handler
    if isinstance(exc, HTTPException):
        return await http_exception_handler(request, exc)

    logger.error(
        f"Unhandled exception: {str(exc)} - Path: {request.url.path}", exc_info=True
    )

    return JSONResponse(
        status_code=StatusCode.INTERNAL_SERVER_ERROR,
        content={"success": False, "message": "Internal server error", "data": None},
    )


def register_exception_handlers(app):
    """
    Register all exception handlers to the FastAPI application.
    Ensures consistent API response format for all error types.
    """
    app.add_exception_handler(HTTPException, http_exception_handler)
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, global_exception_handler)

    logger.info("✅ Exception handlers registered successfully")
