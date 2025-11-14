from fastapi import APIRouter, status, Depends
from typing import List

from client_service.schemas.pydantic_schemas import (
    ClientWorkflowCreate,
    ClientWorkflowUpdate
)
from client_service.schemas.base_response import APIResponse
from client_service.services.client_workflow_service import ClientWorkflowService

router = APIRouter()

# Dependency injection for the service
def get_client_workflow_service() -> ClientWorkflowService:
    return ClientWorkflowService()

# ─────────────────────────────
# CREATE WORKFLOW
# ─────────────────────────────
@router.post(
    "/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new client workflow",
    description="Creates a new workflow for a client. Use when you need to initialize a workflow for a client."
)
async def create_workflow(
    workflow_data: ClientWorkflowCreate,
    service: ClientWorkflowService = Depends(get_client_workflow_service)
):
    """Create a new client workflow"""
    return await service.create_workflow(workflow_data)


# ─────────────────────────────
# GET WORKFLOW BY ID
# ─────────────────────────────
@router.get(
    "/{workflow_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Get workflow by ID",
    description="Retrieves a workflow by its unique ID. Use when you want to view a specific workflow."
)
async def get_workflow_by_id(
    workflow_id: str,
    service: ClientWorkflowService = Depends(get_client_workflow_service)
):
    """Get a client workflow by ID"""
    return await service.get_workflow_by_id(workflow_id)


# ─────────────────────────────
# GET ALL WORKFLOWS
# ─────────────────────────────
@router.get(
    "/",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Get all client workflows",
    description="Retrieves all workflows for all clients. Use when you need a list of all client workflows."
)
async def get_all_workflows(skip: int = 0, limit: int = 100,
    service: ClientWorkflowService = Depends(get_client_workflow_service)
):
    """Get all client workflows"""
    return await service.get_all_workflows(skip, limit)


# ─────────────────────────────
# UPDATE WORKFLOW
# ─────────────────────────────
@router.put(
    "/{workflow_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Update a client workflow",
    description="Updates an existing client workflow by ID. Use when you want to modify workflow details."
)
async def update_workflow(
    workflow_id: str,
    workflow_data: ClientWorkflowUpdate,
    service: ClientWorkflowService = Depends(get_client_workflow_service)
):
    """Update a client workflow"""
    return await service.update_workflow(workflow_id, workflow_data)


# ─────────────────────────────
# DELETE WORKFLOW
# ─────────────────────────────
@router.delete(
    "/{workflow_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Delete a client workflow",
    description="Deletes a workflow permanently by ID. Use when you need to remove a workflow from the system."
)
async def delete_workflow(
    workflow_id: str,
    service: ClientWorkflowService = Depends(get_client_workflow_service)
):
    """Delete a client workflow"""
    return await service.delete_workflow(workflow_id)