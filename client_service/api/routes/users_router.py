from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.users_service import UserService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    UserCreate,
    UserUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/users/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_user",
    summary="Create a new user",
    description="Creates a new user account in the system. Users belong to a client organization and can be assigned roles/permissions. Required: client_id, user_name, email, password. Optional: phone_number. Returns created user with UUID (password is hashed).",
)
async def create_user(
    user_data: List[UserCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new user with name, email, client_id, and password."""
    return await UserService.create(user_data, db)


@router.get(
    "/users/{user_id}",
    response_model=APIResponse,
    operation_id="get_user",
    summary="Get user by ID",
    description="Retrieves complete user details by user_id (UUID). Returns user_name, email, phone_number, client_id, and timestamps. Password is never returned. Use to get user profile information.",
)
async def get_user(
    user_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a user by ID"""
    return await UserService.get_by_id(user_id, db)


@router.get(
    "/users",
    response_model=APIResponse,
    operation_id="list_users",
    summary="List all users",
    description="Lists all users across all clients with pagination. Returns array of users with id, name, email, phone, client_id, and timestamps. Supports skip/limit (max 100). Useful for user management dashboards.",
)
async def get_all_users(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all users with pagination"""
    return await UserService.get_all(skip, limit, db)


@router.put(
    "/users/{user_id}",
    response_model=APIResponse,
    operation_id="update_user",
    summary="Update user information",
    description="Updates user account details by user_id. Can modify user_name, email, phone_number, or password. Partial updates supported. Password is automatically hashed if provided. Returns updated user object.",
)
async def update_user(
    user_id: UUID,
    user_data: UserUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update a user"""
    return await UserService.update(user_id, user_data, db)


@router.delete(
    "/users/{user_id}",
    response_model=APIResponse,
    operation_id="delete_user",
    summary="Delete user",
    description="Permanently deletes a user account by user_id. WARNING: Removes user access and all associated role assignments. Cannot be undone. Consider deactivation instead for audit trails.",
)
async def delete_user(
    user_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete a user"""
    return await UserService.delete(user_id, db)