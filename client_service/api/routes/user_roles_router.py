from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.user_roles_service import UserRoleService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import UserRoleCreate
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/user-roles/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Assign role to user",
    description="Assigns a role to a user. Use when: 'assign role', 'give user role', 'add role to user'.",
)
async def assign_role_to_user(
    user_role_data: List[UserRoleCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Assign a role to a user"""
    return await UserRoleService.assign(user_role_data, db)


@router.get(
    "/user-roles/user/{user_id}",
    response_model=APIResponse,
    operation_id="get_user_roles_by_user",
    summary="Get user roles",
    description="Get all roles for a user. Use when: 'show user roles', 'list roles for user'.",
)
async def get_user_roles(
    user_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all roles for a user"""
    return await UserRoleService.get_by_user_id(user_id, db)


@router.get(
    "/user-roles/role/{role_id}",
    response_model=APIResponse,
    summary="Get users with role",
    description="Get all users with a role. Use when: 'show users with role', 'list users by role'.",
)
async def get_users_with_role(
    role_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all users with a role"""
    return await UserRoleService.get_by_role_id(role_id, db)


@router.delete(
    "/user-roles/remove",
    response_model=APIResponse,
    summary="Remove role from user",
    description="Removes a role from user. Use when: 'remove role', 'revoke user role', 'unassign role'.",
)
async def remove_role_from_user(
    user_id: UUID,
    role_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Remove a role from a user"""
    return await UserRoleService.remove(user_id, role_id, db)