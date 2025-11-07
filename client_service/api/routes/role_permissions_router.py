from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.role_permissions_service import RolePermissionService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import RolePermissionCreate
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/role-permissions/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="assign_role_permission",
    summary="Assign permission to role",
    description="Assigns a permission to a role. Use when: 'assign permission', 'give role permission', 'grant permission'.",
)
async def assign_permission_to_role(
    role_permission_data: List[RolePermissionCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Assign a permission to a role"""
    return await RolePermissionService.assign(role_permission_data, db)


@router.get(
    "/role-permissions/role/{role_id}",
    response_model=APIResponse,
    operation_id="get_role_permissions",
    summary="Get role permissions",
    description="Get all permissions for a role. Use when: 'show role permissions', 'list permissions for role'.",
)
async def get_role_permissions(
    role_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all permissions for a role"""
    return await RolePermissionService.get_by_role_id(role_id, db)


@router.get(
    "/role-permissions/permission/{permission_id}",
    response_model=APIResponse,
    operation_id="get_roles_by_permission",
    summary="Get roles with permission",
    description="Get all roles with a permission. Use when: 'show roles with permission', 'list roles by permission'.",
)
async def get_roles_with_permission(
    permission_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all roles with a permission"""
    return await RolePermissionService.get_by_permission_id(permission_id, db)


@router.delete(
    "/role-permissions/remove",
    response_model=APIResponse,
    operation_id="remove_role_permission",
    summary="Remove permission from role",
    description="Removes a permission from role. Use when: 'remove permission', 'revoke permission'.",
)
async def remove_permission_from_role(
    role_id: UUID,
    permission_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Remove a permission from a role"""
    return await RolePermissionService.remove(role_id, permission_id, db)