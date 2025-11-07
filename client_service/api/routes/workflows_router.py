from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.workflows_service import WorkflowService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    WorkflowCreate,
    WorkflowUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/workflows/create",
    response_model=APIResponse,
    operation_id="create_workflow",
    status_code=status.HTTP_201_CREATED,
    summary="Create workflow ledger",
    description="Creates a workflow request ledger. Use when: 'create workflow', 'start workflow', 'initialize workflow'.",
)
async def create_workflow_ledger(
    workflow_data: List[WorkflowCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new workflow ledger"""
    return await WorkflowService.create(workflow_data, db)


@router.get(
    "/workflows/{ledger_id}",
    response_model=APIResponse,
    operation_id="get_workflow",
    summary="Get workflow by ID",
    description="Retrieves workflow details. Use when: 'get workflow', 'show workflow', 'find workflow'.",
)
async def get_workflow_ledger(
    ledger_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a workflow ledger by ID"""
    return await WorkflowService.get_by_id(ledger_id, db)


@router.get(
    "/workflows",
    response_model=APIResponse,
    operation_id="list_workflows",
    summary="List all workflows",
    description="Get all workflows with pagination. Use when: 'list workflows', 'show all workflows'.",
)
async def get_all_workflow_ledgers(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all workflow ledgers with pagination"""
    return await WorkflowService.get_all(skip, limit, db)


@router.get(
    "/workflows/client/{client_id}",
    response_model=APIResponse,
    operation_id="get_client_workflows",
    summary="Get workflows by client",
    description="Get all workflows for a client. Use when: 'show client workflows', 'list workflows for client'.",
)
async def get_workflows_by_client(
    client_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all workflow ledgers by client ID"""
    return await WorkflowService.get_by_client_id(client_id, db)


@router.get(
    "/workflows/user/{user_id}",
    response_model=APIResponse,
    operation_id="get_user_workflows",
    summary="Get workflows by user",
    description="Get all workflows for a user. Use when: 'show user workflows', 'list workflows for user'.",
)
async def get_workflows_by_user(
    user_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all workflow ledgers by user ID"""
    return await WorkflowService.get_by_user_id(user_id, db)


@router.put(
    "/workflows/{ledger_id}",
    response_model=APIResponse,
    operation_id="update_workflow",
    summary="Update workflow",
    description="Updates workflow information. Use when: 'update workflow', 'modify workflow'.",
)
async def update_workflow_ledger(
    ledger_id: UUID,
    workflow_data: WorkflowUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update a workflow ledger"""
    return await WorkflowService.update(ledger_id, workflow_data, db)


@router.patch(
    "/workflows/{ledger_id}/increment",
    response_model=APIResponse,
    operation_id="increment_workflow",
    summary="Increment workflow request count",
    description="Increments request count for workflow. Use when: 'increment workflow', 'increase request count'.",
)
async def increment_workflow_request(
    ledger_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Increment the request count for a workflow ledger"""
    return await WorkflowService.increment(ledger_id, db)


@router.delete(
    "/workflows/{ledger_id}",
    response_model=APIResponse,
    operation_id="delete_workflow",
    summary="Delete workflow",
    description="Deletes a workflow ledger. Use when: 'delete workflow', 'remove workflow'.",
)
async def delete_workflow_ledger(
    ledger_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete a workflow ledger"""
    return await WorkflowService.delete(ledger_id, db)