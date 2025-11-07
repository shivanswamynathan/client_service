from fastapi import APIRouter, status, Depends
from typing import List

from client_service.schemas.pydantic_schemas import (
    WorkflowExecutionLogCreate
)
from client_service.schemas.base_response import APIResponse
from client_service.services.workflow_executionlog_service import WorkflowExecutionLogService

router = APIRouter()

# Dependency injection
def get_workflow_executionlog_service() -> WorkflowExecutionLogService:
    return WorkflowExecutionLogService()

# ─────────────────────────────
# CREATE LOG
# ─────────────────────────────
@router.post(
    "/",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a workflow execution log",
    description="Creates a new workflow execution log to track the run status, inputs, and outputs of a workflow execution."
)
async def create_log(
    log_data: WorkflowExecutionLogCreate,
    service: WorkflowExecutionLogService = Depends(get_workflow_executionlog_service)
):
    return await service.create_log(log_data)

# ─────────────────────────────
# GET LOG BY ID
# ─────────────────────────────
@router.get(
    "/{log_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Get workflow execution log by ID",
    description="Retrieves detailed information of a specific workflow execution log using its MongoDB ObjectId."
)
async def get_log_by_id(
    log_id: str,
    service: WorkflowExecutionLogService = Depends(get_workflow_executionlog_service)
):
    return await service.get_log_by_id(log_id)

# ─────────────────────────────
# GET ALL LOGS
# ─────────────────────────────
@router.get(
    "/",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Get all workflow execution logs",
    description="Fetches all workflow execution logs with pagination support using `skip` and `limit` parameters."
)
async def get_all_logs(skip: int = 0, limit: int = 100,
    service: WorkflowExecutionLogService = Depends(get_workflow_executionlog_service)
):
    return await service.get_all_logs(skip, limit)

# ─────────────────────────────
# UPDATE LOG
# ─────────────────────────────
@router.put(
    "/{log_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Update workflow execution log",
    description="Updates the status, result, or metadata of an existing workflow execution log by its ObjectId."
)
async def update_log(
    log_id: str,
    log_data: dict,
    service: WorkflowExecutionLogService = Depends(get_workflow_executionlog_service)
):
    return await service.update_log(log_id, log_data)

# ─────────────────────────────
# DELETE LOG
# ─────────────────────────────
@router.delete(
    "/{log_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Delete workflow execution log",
    description="Delete a workflow execution log permanently by ID"
)
async def delete_log(
    log_id: str,
    service: WorkflowExecutionLogService = Depends(get_workflow_executionlog_service)
):
    return await service.delete_log(log_id)
