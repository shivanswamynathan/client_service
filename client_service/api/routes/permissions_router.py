from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.permissions_service import PermissionService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    PermissionCreate,
    PermissionUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/permissions/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_permission",
    summary="Create permission",
    description="Creates a new permission for fine-grained access control. Permissions define specific actions users can perform. Required: permission_name. Optional: description. Examples: 'create_invoice', 'approve_po', 'view_reports'.",
)
async def create_permission(
    permission_data: List[PermissionCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new permission"""
    return await PermissionService.create(permission_data, db)


@router.get(
    "/permissions/{permission_id}",
    response_model=APIResponse,
    operation_id="get_permission",
    summary="Get permission by ID",
    description="Retrieves complete permission details by permission_id (UUID). Returns permission_name, description, and timestamps. Use to view what a specific permission allows.",
)
async def get_permission(
    permission_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a permission by ID"""
    return await PermissionService.get_by_id(permission_id, db)


@router.get(
    "/permissions",
    response_model=APIResponse,
    operation_id="list_permissions",
    summary="List all permissions",
    description="Lists all permissions in the system with pagination. Returns array of permissions with id, name, description, and timestamps. Supports skip/limit (max 100). Useful for assigning permissions to roles.",
)
async def get_all_permissions(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all permissions with pagination"""
    return await PermissionService.get_all(skip, limit, db)


@router.put(
    "/permissions/{permission_id}",
    response_model=APIResponse,
    operation_id="update_permission",
    summary="Update permission",
    description="Updates permission details by permission_id. Can modify permission_name or description. Partial updates supported. Returns updated permission object. Changes affect all roles using this permission.",
)
async def update_permission(
    permission_id: UUID,
    permission_data: PermissionUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update a permission"""
    return await PermissionService.update(permission_id, permission_data, db)


@router.delete(
    "/permissions/{permission_id}",
    response_model=APIResponse,
    operation_id="delete_permission",
    summary="Delete permission",
    description="Permanently deletes a permission by permission_id. WARNING: Removes this permission from all roles. Users with those roles lose this access. Cannot be undone.",
)
async def delete_permission(
    permission_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete a permission"""
    return await PermissionService.delete(permission_id, db)