from fastapi import APIRouter, status, Depends  
from sqlalchemy.ext.asyncio import AsyncSession 
from client_service.api.dependencies import get_database_session 
from client_service.services.client_schema_service import ClientSchemaService
from client_service.schemas.base_response import APIResponse
from typing import List
from client_service.schemas.pydantic_schemas import (
    ClientSchemaCreate,
    ClientSchemaUpdate
)

router = APIRouter()


@router.post(
    "/client-schemas/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_schema",
    summary="Create client schema",
    description="Creates a new client-specific schema definition for dynamic documents (e.g., invoices, purchase orders, GRNs). Supports field validation, versioning, and references. Required: client_id (UUID string), schema_name (string), fields (array of {name, type, required}). Example: POST /client-schemas/create with body: {\"client_id\": \"uuid-here\", \"schema_name\": \"invoice\", \"fields\": [{\"name\": \"invoice_number\", \"type\": \"string\", \"required\": true}]}.",
)
async def create_client_schema(
    schema_data: List[ClientSchemaCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """
    Create a new client schema definition
    
    - Validates client exists in PostgreSQL
    - Auto-generates version if not provided
    - Deactivates other versions if is_active=True
    - Validates field types and references
    """
    return await ClientSchemaService.create(schema_data, db)  # ← ADDED db


@router.get(
    "/client-schemas/{schema_id}",
    response_model=APIResponse,
    operation_id="get_schema",
    summary="Get schema by ID",
    description="Retrieves a specific schema definition by its MongoDB ObjectId. Returns complete schema including all field definitions, validation rules, version info, and metadata. Call: GET /client-schemas/{schema_id} where schema_id is a MongoDB ObjectId string (24 hex chars).",
)
async def get_client_schema(schema_id: str):
    """Get a client schema by MongoDB ObjectId"""
    return await ClientSchemaService.get_by_id(schema_id)


@router.get(
    "/client-schemas",
    response_model=APIResponse,
    operation_id="list_schemas",
    summary="List all schemas",
    description="Lists all schema definitions across all clients with pagination. Returns schema_name, version, is_active status, client_id, and field count for each schema. Call: GET /client-schemas?skip=0&limit=100. Default: skip=0, limit=100.",
)
async def get_all_client_schemas(skip: int = 0, limit: int = 100):
    """Get all client schemas with pagination"""
    return await ClientSchemaService.get_all(skip, limit)


@router.get(
    "/client-schemas/client/{client_id}",
    response_model=APIResponse,
    operation_id="get_client_schemas",
    summary="Get schemas by client",
    description="Retrieves all schema definitions (all versions) for a specific client by client_id. Returns complete schema details including fields, versions, and active status. Call: GET /client-schemas/client/{client_id} where client_id is a UUID string.",
)
async def get_schemas_by_client(client_id: str):
    """Get all schemas for a specific client"""
    return await ClientSchemaService.get_by_client_id(client_id)


@router.get(
    "/client-schemas/client/{client_id}/{schema_name}",
    response_model=APIResponse,
    operation_id="get_schema_by_name",
    summary="Get schema by name",
    description="Retrieves all versions of a specific schema by client_id and schema_name (e.g., 'invoice', 'purchase_order'). Returns version history with field definitions. Call: GET /client-schemas/client/{client_id}/{schema_name} where both are strings.",
)
async def get_schema_by_name(client_id: str, schema_name: str):
    """Get all versions of a specific schema for a client"""
    return await ClientSchemaService.get_by_client_and_name(client_id, schema_name)


@router.get(
    "/client-schemas/client/{client_id}/{schema_name}/active",
    response_model=APIResponse,
    operation_id="get_active_schema",
    summary="Get active schema version",
    description="Retrieves the currently active version of a schema by client_id and schema_name. Only one version can be active at a time. Call: GET /client-schemas/client/{client_id}/{schema_name}/active. Returns the active schema for validation.",
)
async def get_active_schema(client_id: str, schema_name: str):
    """Get the active version of a schema"""
    return await ClientSchemaService.get_active_schema(client_id, schema_name)


@router.put(
    "/client-schemas/{schema_id}",
    response_model=APIResponse,
    operation_id="update_schema",
    summary="Update schema",
    description="Updates an existing schema definition by schema_id. Can modify description, field definitions, or validation rules. Call: PUT /client-schemas/{schema_id} with body containing fields to update: {\"description\": \"...\", \"fields\": [...]}. Updates in place - use caution on active schemas.",
)
async def update_client_schema(schema_id: str, schema_data: ClientSchemaUpdate):
    """
    Update a client schema
    
    - Updates the existing document
    - Can update description, fields, or is_active status
    - If activating, deactivates other versions
    """
    return await ClientSchemaService.update(schema_id, schema_data)


@router.patch(
    "/client-schemas/{schema_id}/activate",
    response_model=APIResponse,
    operation_id="activate_schema",
    summary="Activate schema version",
    description="Activates a specific schema version by schema_id, automatically deactivating all other versions of the same schema. Call: PATCH /client-schemas/{schema_id}/activate (no body required). The activated version becomes the default for document validation.",
)
async def activate_schema_version(schema_id: str):
    """
    Activate a specific version of a schema
    
    - Deactivates all other versions of the same schema
    - Sets this version as the active one
    """
    return await ClientSchemaService.activate_version(schema_id)


@router.delete(
    "/client-schemas/{schema_id}",
    response_model=APIResponse,
    operation_id="delete_schema",
    summary="Delete schema",
    description="Permanently deletes a schema definition by schema_id. WARNING: Cannot be undone. Call: DELETE /client-schemas/{schema_id} (no body required). Existing documents unaffected, but new documents cannot use this schema.",
)
async def delete_client_schema(schema_id: str):
    """Delete a client schema"""
    return await ClientSchemaService.delete(schema_id)