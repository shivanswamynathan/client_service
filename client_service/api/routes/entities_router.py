from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.entities_service import EntityService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    ClientEntityCreate,
    ClientEntityUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/entities/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_entity",
    summary="Create client entity",
    description="Creates a new entity (branch, subsidiary, or location) under a client organization. Entities represent physical locations or business units. Required: client_id, entity_name. Optional: gst_number, pan_number, address. Returns created entity with UUID.",
)
async def create_entity(
    entity_data: List[ClientEntityCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new entity"""
    return await EntityService.create(entity_data, db)

@router.get(
    "/entities/search",
    response_model=APIResponse,
    operation_id="search_entities",
    summary="Search entities by column",
    description=(
        "Search entities by specific column and value. Returns all matching results.\n\n"
        "Query params:\n"
        "- column: one of [entity_name, client_id, gst_id, company_pan, tan, parent_client_id]\n"
        "- value: text to match (case-insensitive, partial matches allowed)"
    ),
)
async def search_entities(
    column: str,
    value: str,
    db: AsyncSession = Depends(get_database_session)
):
    """
    Search entities by specific column.
    
    Allowed columns: entity_name, client_id, gst_id, company_pan, tan, parent_client_id
    """
    return await EntityService.search(column, value, db)


@router.get(
    "/entities/{entity_id}",
    response_model=APIResponse,
    operation_id="get_entity",
    summary="Get entity by ID",
    description="Retrieves complete entity details by entity_id (UUID). Returns entity_name, client_id, gst_number, pan_number, address, and timestamps. Use to get branch/location information.",
)
async def get_entity(
    entity_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get an entity by ID"""
    return await EntityService.get_by_id(entity_id, db)


@router.get(
    "/entities",
    response_model=APIResponse,
    operation_id="list_entities",
    summary="List all entities",
    description="Lists all entities (branches/locations) across all clients with pagination. Returns array with entity details including associated client_id. Supports skip/limit (max 100). Useful for viewing all branches system-wide.",
)
async def get_all_entities(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all entities with pagination"""
    return await EntityService.get_all(skip, limit, db)


@router.get(
    "/entities/client/{client_id}",
    response_model=APIResponse,
    operation_id="get_client_entities",
    summary="Get entities by client",
    description="Retrieves all entities (branches/locations) belonging to a specific client by client_id. Returns complete entity list for that organization. Use to see all branches/locations of a particular client.",
)
async def get_entities_by_client(
    client_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all entities by client ID"""
    return await EntityService.get_by_client_id(client_id, db)


@router.put(
    "/entities/{entity_id}",
    response_model=APIResponse,
    operation_id="update_entity",
    summary="Update entity",
    description="Updates entity details by entity_id. Can modify entity_name, gst_number, pan_number, or address. Partial updates supported. Returns updated entity object.",
)
async def update_entity(
    entity_id: UUID,
    entity_data: ClientEntityUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update an entity"""
    return await EntityService.update(entity_id, entity_data, db)


@router.delete(
    "/entities/{entity_id}",
    response_model=APIResponse,
    operation_id="delete_entity",
    summary="Delete entity",
    description="Permanently deletes an entity by entity_id. WARNING: Cascades to delete related transactions, users, and documents associated with this entity. Cannot be undone.",
)
async def delete_entity(
    entity_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete an entity"""
    return await EntityService.delete(entity_id, db)