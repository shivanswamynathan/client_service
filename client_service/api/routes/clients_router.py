from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.clients_service import ClientService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    ClientCreate,
    ClientUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/clients/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_client",
    summary="Create a new client",
    description="Creates a new client organization in the system. A client represents a company/organization that can have multiple entities (branches), users, and documents. Required fields: client_name. Optional: api_key for external integrations. Returns created client with UUID.",
)
async def create_client(
    client_data: List[ClientCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new client"""
    return await ClientService.create(client_data, db)


@router.get(
    "/clients/{client_id}",
    response_model=APIResponse,
    operation_id="get_client",
    summary="Get client by ID",
    description="Retrieves complete client organization details by client_id (UUID). Returns client_name, api_key, creation timestamp, and associated metadata. Use this to verify client exists or get client information.",
)
async def get_client(
    client_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a client by ID"""
    return await ClientService.get_by_id(client_id, db)


@router.get(
    "/clients",
    response_model=APIResponse,
    operation_id="list_clients",
    summary="List all clients",
    description="Lists all client organizations in the system with pagination. Returns array of clients with id, name, api_key, and timestamps. Supports skip/limit parameters (max 100 per request). Useful for admin dashboards or client selection.",
)
async def get_all_clients(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all clients with pagination"""
    return await ClientService.get_all(skip, limit, db)


@router.put(
    "/clients/{client_id}",
    response_model=APIResponse,
    operation_id="update_client",
    summary="Update client information",
    description="Updates client organization details by client_id. Can modify client_name or regenerate api_key. Partial updates supported - only provided fields are changed. Returns updated client object.",
)
async def update_client(
    client_id: UUID,
    client_data: ClientUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update a client"""
    return await ClientService.update(client_id, client_data, db)


@router.delete(
    "/clients/{client_id}",
    response_model=APIResponse,
    operation_id="delete_client",
    summary="Delete client",
    description="Permanently deletes a client organization by client_id. WARNING: This cascades to delete ALL related data including entities, users, roles, documents, and schemas. Cannot be undone. Use with extreme caution.",
)
async def delete_client(
    client_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete a client"""
    return await ClientService.delete(client_id, db)