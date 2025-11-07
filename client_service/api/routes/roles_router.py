from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.roles_service import RoleService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    RoleCreate,
    RoleUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/roles/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_role",
    summary="Create role",
    description="Creates a new role for access control. Roles group permissions and can be assigned to users. Required: role_name. Optional: description. Returns created role with UUID. Examples: 'Admin', 'Accountant', 'Viewer'.",
)
async def create_role(
    role_data: List[RoleCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new role"""
    return await RoleService.create(role_data, db)


@router.get(
    "/roles/{role_id}",
    response_model=APIResponse,
    operation_id="get_role",
    summary="Get role by ID",
    description="Retrieves complete role details by role_id (UUID). Returns role_name, description, and timestamps. Use to view role configuration before assigning to users.",
)
async def get_role(
    role_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a role by ID"""
    return await RoleService.get_by_id(role_id, db)


@router.get(
    "/roles",
    response_model=APIResponse,
    operation_id="list_roles",
    summary="List all roles",
    description="Lists all roles in the system with pagination. Returns array of roles with id, name, description, and timestamps. Supports skip/limit (max 100). Useful for role selection in user management.",
)
async def get_all_roles(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all roles with pagination"""
    return await RoleService.get_all(skip, limit, db)


@router.put(
    "/roles/{role_id}",
    response_model=APIResponse,
    operation_id="update_role",
    summary="Update role",
    description="Updates role details by role_id. Can modify role_name or description. Partial updates supported. Returns updated role object. Note: Does not affect existing user-role assignments.",
)
async def update_role(
    role_id: UUID,
    role_data: RoleUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update a role"""
    return await RoleService.update(role_id, role_data, db)


@router.delete(
    "/roles/{role_id}",
    response_model=APIResponse,
    operation_id="delete_role",
    summary="Delete role",
    description="Permanently deletes a role by role_id. WARNING: Removes role from all users assigned to it. Associated permissions are also deleted. Cannot be undone.",
)
async def delete_role(
    role_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete a role"""
    return await RoleService.delete(role_id, db)