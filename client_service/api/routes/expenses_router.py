from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.expenses_service import ExpenseService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    ExpenseCategoryCreate,
    ExpenseCategoryUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/expenses/categories/create",
    response_model=APIResponse,
    operation_id="create_expense_category",
    status_code=status.HTTP_201_CREATED,
    summary="Create expense category",
    description="Creates a new expense category master record. Use when: 'create expense category', 'add expense type'.",
)
async def create_expense_category(
    category_data: List[ExpenseCategoryCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new expense category"""
    return await ExpenseService.create(category_data, db)


@router.get(
    "/expenses/categories/{category_id}",
    response_model=APIResponse,
    operation_id="get_expense_category",
    summary="Get expense category by ID",
    description="Retrieves expense category by UUID. Use when: 'get expense category', 'show category details'.",
)
async def get_expense_category(
    category_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get an expense category by ID"""
    return await ExpenseService.get_by_id(category_id, db)


@router.get(
    "/expenses/categories",
    response_model=APIResponse,
    operation_id="list_expense_categories",
    summary="List all expense categories",
    description="Get all expense categories with pagination. Use when: 'list expense categories', 'show all categories'.",
)
async def get_all_expense_categories(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all expense categories with pagination"""
    return await ExpenseService.get_all(skip, limit, db)


@router.put(
    "/expenses/categories/{category_id}",
    response_model=APIResponse,
    operation_id="update_expense_category",
    summary="Update expense category",
    description="Updates expense category details. Use when: 'update expense category', 'modify category'.",
)
async def update_expense_category(
    category_id: UUID,
    category_data: ExpenseCategoryUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update an expense category"""
    return await ExpenseService.update(category_id, category_data, db)


@router.delete(
    "/expenses/categories/{category_id}",
    response_model=APIResponse,
    operation_id="delete_expense_category",
    summary="Delete expense category",
    description="Deletes an expense category (cascades to items/vendors if configured). Use when: 'delete expense category', 'remove category'.",
)
async def delete_expense_category(
    category_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete an expense category"""
    return await ExpenseService.delete(category_id, db)