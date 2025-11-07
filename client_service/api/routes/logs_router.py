from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.logs_service import LogService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    ActionLogCreate,
    TransactionLogCreate,
    UserLogCreate
)
from uuid import UUID
from typing import List

router = APIRouter()


# ==================== ACTION LOG ROUTES ====================

@router.post(
    "/action-logs/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_action_log",
    summary="Create action log",
    description="Creates an action log entry. Use when: 'log action', 'create action log'.",
)
async def create_action_log(
    action_log_data: List[ActionLogCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new action log"""
    return await LogService.create_action_log(action_log_data, db)


@router.get(
    "/action-logs/{log_id}",
    response_model=APIResponse,
    operation_id="get_action_log",
    summary="Get action log by ID",
    description="Retrieves action log. Use when: 'get action log', 'show action log'.",
)
async def get_action_log(
    log_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get an action log by ID"""
    return await LogService.get_by_id_action_log(log_id, db)


@router.get(
    "/action-logs",
    response_model=APIResponse,
    operation_id="list_action_logs",
    summary="List all action logs",
    description="Get all action logs. Use when: 'list action logs', 'show all action logs'.",
)
async def get_all_action_logs(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all action logs with pagination"""
    return await LogService.get_all_action_logs(skip, limit, db)


# ==================== TRANSACTION LOG ROUTES ====================

@router.post(
    "/transaction-logs/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_transaction_log",
    summary="Create transaction log",
    description="Creates a transaction log. Use when: 'log transaction', 'create transaction log'.",
)
async def create_transaction_log(
    transaction_log_data: List[TransactionLogCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new transaction log"""
    return await LogService.create_transaction_log(transaction_log_data, db)


@router.get(
    "/transaction-logs/{log_id}",
    response_model=APIResponse,
    operation_id="get_transaction_log",
    summary="Get transaction log by ID",
    description="Retrieves transaction log. Use when: 'get transaction log', 'show transaction log'.",
)
async def get_transaction_log(
    log_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a transaction log by ID"""
    return await LogService.get_by_id_transaction_log(log_id, db)


@router.get(
    "/transaction-logs/transaction/{transaction_id}",
    response_model=APIResponse,
    operation_id="get_transaction_logs",
    summary="Get logs by transaction",
    description="Get all logs for a transaction. Use when: 'show transaction logs', 'list logs for transaction'.",
)
async def get_logs_by_transaction(
    transaction_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all transaction logs by transaction ID"""
    return await LogService.get_by_transaction_id(transaction_id, db)


# ==================== USER LOG ROUTES ====================

@router.post(
    "/user-logs/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_user_log",
    summary="Create user log",
    description="Creates a user activity log. Use when: 'log user action', 'create user log'.",
)
async def create_user_log(
    user_log_data: List[UserLogCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new user log"""
    return await LogService.create_user_log(user_log_data, db)


@router.get(
    "/user-logs/{log_id}",
    response_model=APIResponse,
    operation_id="get_user_log",
    summary="Get user log by ID",
    description="Retrieves user log. Use when: 'get user log', 'show user log'.",
)
async def get_user_log(
    log_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a user log by ID"""
    return await LogService.get_by_id_user_log(log_id, db)


@router.get(
    "/user-logs/user/{user_id}",
    response_model=APIResponse,
    operation_id="get_user_logs",
    summary="Get logs by user",
    description="Get all logs for a user. Use when: 'show user activity', 'list user logs'.",
)
async def get_logs_by_user(
    user_id: UUID,
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all user logs by user ID with pagination"""
    return await LogService.get_by_user_id(user_id, skip, limit, db)


@router.get(
    "/user-logs",
    response_model=APIResponse,
    operation_id="list_user_logs",
    summary="List all user logs",
    description="Get all user logs. Use when: 'list user logs', 'show all user activity'.",
)
async def get_all_user_logs(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all user logs with pagination"""
    return await LogService.get_all_user_logs(skip, limit, db)