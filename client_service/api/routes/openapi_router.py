from fastapi import APIRouter, Request, HTTPException
from client_service.schemas.base_response import APIResponse
from client_service.api.constants.status_codes import StatusCode
import logging

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get(
    "/openapi-schema",
    response_model=APIResponse,
    summary="Get OpenAPI Schema",
    description="This API returns the Client Service's complete API documentation including all endpoints, request schemas, response schemas, and data models. Use when: 'get API documentation', 'show all endpoints', 'view API schema'.",
)
async def get_openapi_schema(request: Request):
    """
    Retrieve the OpenAPI schema for the entire API.
    
    This endpoint returns the same schema available at /openapi.json
    but wrapped in the standard API response format.
    """
    try:
        # Get the OpenAPI schema from the FastAPI app
        openapi_schema = request.app.openapi()

        return APIResponse(
            success=True,
            message="OpenAPI schema retrieved successfully",
            data=openapi_schema,
        )
    except Exception as e:
        logger.error(f"Error retrieving OpenAPI schema: {str(e)}")
        raise HTTPException(
            status_code=StatusCode.INTERNAL_SERVER_ERROR,
            detail=f"Error retrieving OpenAPI schema: {str(e)}"
        )