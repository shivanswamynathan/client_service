from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.transactions_service import TransactionService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    TransactionCreate,
    TransactionUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/transactions/create",
    response_model=APIResponse,
    operation_id="create_transaction",
    status_code=status.HTTP_201_CREATED,
    summary="Create transaction",
    description="Creates a new vendor transaction. Use when: 'create transaction', 'add invoice', 'record payment'.",
)
async def create_transaction(
    transaction_data: List[TransactionCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new transaction"""
    return await TransactionService.create(transaction_data, db)


@router.get(
    "/transactions/{transaction_id}",
    response_model=APIResponse,
    operation_id="get_transaction",
    summary="Get transaction by ID",
    description="Retrieves transaction details. Use when: 'get transaction', 'show invoice', 'find payment'.",
)
async def get_transaction(
    transaction_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a transaction by ID"""
    return await TransactionService.get_by_id(transaction_id, db)


@router.get(
    "/transactions",
    response_model=APIResponse,
    operation_id="list_transactions",
    summary="List all transactions",
    description="Get all transactions with pagination. Use when: 'list transactions', 'show all invoices'.",
)
async def get_all_transactions(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all transactions with pagination"""
    return await TransactionService.get_all(skip, limit, db)


@router.get(
    "/transactions/vendor/{vendor_id}",
    response_model=APIResponse,
    operation_id="get_vendor_transactions",
    summary="Get transactions by vendor",
    description="Get all transactions for a vendor. Use when: 'show vendor transactions', 'list vendor invoices'.",
)
async def get_transactions_by_vendor(
    vendor_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all transactions by vendor ID"""
    return await TransactionService.get_by_vendor_id(vendor_id, db)


@router.put(
    "/transactions/{transaction_id}",
    response_model=APIResponse,
    operation_id="update_transaction",
    summary="Update transaction",
    description="Updates transaction details. Use when: 'update transaction', 'modify invoice'.",
)
async def update_transaction(
    transaction_id: UUID,
    transaction_data: TransactionUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update a transaction"""
    return await TransactionService.update(transaction_id, transaction_data, db)


@router.delete(
    "/transactions/{transaction_id}",
    response_model=APIResponse,
    operation_id="delete_transaction",
    summary="Delete transaction",
    description="Deletes a transaction. Use when: 'delete transaction', 'remove invoice'.",
)
async def delete_transaction(
    transaction_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete a transaction"""
    return await TransactionService.delete(transaction_id, db)