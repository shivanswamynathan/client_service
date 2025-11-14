from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.central_client_service import CentralClientService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    CentralClientCreate, 
    CentralClientUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/central-clients/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_central_client",
    summary="Create central client",
    description="Creates a parent central client. Use when: 'create central client', 'add parent client'.",
)
async def create_central_client(
    central_client_data: List[CentralClientCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new central client"""
    return await CentralClientService.create(central_client_data, db)


@router.get(
    "/central-clients/{client_id}",
    response_model=APIResponse,
    operation_id="get_central_client",
    summary="Get central client by ID",
    description="Retrieves central client by UUID. Use when: 'get central client', 'show parent client'.",
)
async def get_central_client(
    client_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a central client by ID"""
    return await CentralClientService.get_by_id(client_id, db)


@router.get(
    "/central-clients",
    response_model=APIResponse,
    operation_id="list_central_clients",
    summary="List all central clients",
    description="Get all central clients with pagination. Use when: 'list central clients', 'show all parent clients'.",
)
async def get_all_central_clients(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all central clients with pagination"""
    return await CentralClientService.get_all(skip, limit, db)


@router.put(
    "/central-clients/{client_id}",
    response_model=APIResponse,
    operation_id="update_central_client",
    summary="Update central client",
    description="Updates central client name. Use when: 'update central client', 'modify parent client'.",
)
async def update_central_client(
    client_id: UUID,
    central_client_data: CentralClientUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update a central client"""
    return await CentralClientService.update(client_id, central_client_data, db)


@router.delete(
    "/central-clients/{client_id}",
    response_model=APIResponse,
    operation_id="delete_central_client",
    summary="Delete central client",
    description="Deletes central client permanently. Use when: 'delete central client', 'remove parent client'.",
)
async def delete_central_client(
    client_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete a central client"""
    return await CentralClientService.delete(client_id, db)