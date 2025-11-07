from fastapi import APIRouter, status, Depends
from client_service.schemas.pydantic_schemas import (
    AgentExecutionLogCreate,
    AgentExecutionLogUpdate
)
from client_service.schemas.base_response import APIResponse
from client_service.services.agent_executionlog_service import AgentExecutionService

router = APIRouter()

# Dependency injection
def get_agent_execution_service() -> AgentExecutionService:
    return AgentExecutionService()

# ─────────────────────────────
# CREATE AGENT LOG
# ─────────────────────────────
@router.post(
    "/",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create agent execution log",
    description="Creates a new record of an agent’s execution within a workflow. "
    "Use when: 'agent started a task', 'store execution result', or 'log agent output'. "
    "Includes status, process steps, user feedback, and rule-wise results."
)
async def create_agent_log(
    log_data: AgentExecutionLogCreate,
    service: AgentExecutionService = Depends(get_agent_execution_service)
):
    return await service.create_log(log_data)

# ─────────────────────────────
# GET AGENT LOG BY ID
# ─────────────────────────────
@router.get(
    "/{log_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Get agent execution log by ID",
    description= "Retrieves detailed information of a specific agent execution log by its MongoDB ObjectId. "
                 "Use when: 'view agent run details', 'check execution result', or 'debug workflow step'."
)
async def get_agent_log_by_id(
    log_id: str,
    service: AgentExecutionService = Depends(get_agent_execution_service)
):
    return await service.get_log_by_id(log_id)

# ─────────────────────────────
# GET ALL AGENT LOGS
# ─────────────────────────────
@router.get(
    "/",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Get all agent execution logs",
    description="Fetches all agent execution log entries across workflows and agents. "
    "Use when: 'list agent runs', 'analyze execution history', or 'generate reports'."
)
async def get_all_agent_logs(skip: int = 0, limit: int = 100,
    service: AgentExecutionService = Depends(get_agent_execution_service)
):
    return await service.get_all_logs(skip, limit)

# ─────────────────────────────
# UPDATE AGENT LOG
# ─────────────────────────────
@router.put(
    "/{log_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Update agent execution log",
    description="Updates the details of an existing agent execution log. "
    "Use when: 'correct log status', 'add user feedback', or 'update error/output details'."
)
async def update_agent_log(
    log_id: str,
    log_data: AgentExecutionLogUpdate,
    service: AgentExecutionService = Depends(get_agent_execution_service)
):
    return await service.update_log(log_id, log_data)

# ─────────────────────────────
# DELETE AGENT LOG
# ─────────────────────────────
@router.delete(
    "/{log_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Delete agent execution log",
    description="Deletes an existing agent execution log permanently. "
    "Use when: 'remove invalid entries' or 'clean up workflow logs'."
)
async def delete_agent_log(
    log_id: str,
    service: AgentExecutionService = Depends(get_agent_execution_service)
):
    return await service.delete_log(log_id)
