from typing import List, Optional
from datetime import datetime, timezone
from beanie import PydanticObjectId
from fastapi import HTTPException
import logging

from client_service.schemas.mongo_schemas.client_workflow_execution import ClientWorkflows
from client_service.schemas.pydantic_schemas import (
    ClientWorkflowCreate,
    ClientWorkflowUpdate,
    ClientWorkflowResponse
)
from client_service.schemas.base_response import APIResponse
from client_service.api.constants.status_codes import StatusCode
from client_service.api.constants.messages import ClientWorkflowMessages

# Initialize logger
logger = logging.getLogger(__name__)

class ClientWorkflowService:
    """Service class for managing client workflows with uniform API responses"""

    # ─────────────────────────────
    # CREATE
    # ─────────────────────────────
    @staticmethod
    async def create_workflow(data: ClientWorkflowCreate) -> APIResponse:
        """Create a new client workflow"""
        logger.info("Creating a new client workflow with data: %s", data.dict())
        try:
            workflow = ClientWorkflows(**data.dict())
            await workflow.insert()
            logger.info("Client workflow created successfully: %s", workflow.name)
            return APIResponse(
                success=True,
                message=ClientWorkflowMessages.CREATED_SUCCESS.format(name=workflow.name),
                data=[ClientWorkflowResponse(**workflow.dict())],
            )
        except Exception as e:
            logger.error("Error creating client workflow: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientWorkflowMessages.CREATE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # READ: Get by ID
    # ─────────────────────────────
    @staticmethod
    async def get_workflow_by_id(workflow_id: str) -> APIResponse:
        """Retrieve a single client workflow by ID"""
        logger.info("Retrieving client workflow with ID: %s", workflow_id)
        try:
            workflow = await ClientWorkflows.get(PydanticObjectId(workflow_id))
            if not workflow:
                logger.warning("Client workflow not found with ID: %s", workflow_id)
                return APIResponse(
                    success=False,
                    message=ClientWorkflowMessages.NOT_FOUND.format(id=workflow_id),
                    data=None,
                )
            logger.info("Client workflow retrieved successfully: %s", workflow.name)
            return APIResponse(
                success=True,
                message=ClientWorkflowMessages.RETRIEVED_SUCCESS.format(name=workflow.name),
                data=[ClientWorkflowResponse(**workflow.dict())],
            )
        except Exception as e:
            logger.error("Error retrieving client workflow: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientWorkflowMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # READ: Get All
    # ─────────────────────────────
    @staticmethod
    async def get_all_workflows(skip: int = 0, limit: int = 50) -> APIResponse:
        """Retrieve all client workflows with pagination"""
        logger.info("Retrieving all client workflows (skip=%s, limit=%s)", skip, limit)
        try:
            workflows = await ClientWorkflows.find_all().skip(skip).limit(limit).to_list()
            logger.info("Retrieved %d client workflows", len(workflows))

            return APIResponse(
                success=True,
                message=ClientWorkflowMessages.RETRIEVED_ALL_SUCCESS.format(count=len(workflows)),
                data=[
                    ClientWorkflowResponse.model_validate(w).model_dump()
                    for w in workflows
                ],
            )
        except Exception as e:
            logger.error("Error retrieving all client workflows: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientWorkflowMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # UPDATE
    # ─────────────────────────────
    @staticmethod
    async def update_workflow(workflow_id: str, data: ClientWorkflowUpdate) -> APIResponse:
        """Update a client workflow"""
        logger.info("Updating client workflow with ID: %s and data: %s", workflow_id, data.dict(exclude_unset=True))
        try:
            workflow = await ClientWorkflows.get(PydanticObjectId(workflow_id))
            if not workflow:
                logger.warning("Client workflow not found with ID: %s", workflow_id)
                return APIResponse(
                    success=False,
                    message=ClientWorkflowMessages.NOT_FOUND.format(id=workflow_id),
                    data=None,
                )

            update_data = data.dict(exclude_unset=True)
            for field, value in update_data.items():
                setattr(workflow, field, value)

            workflow.updated_at = datetime.now(timezone.utc)
            await workflow.save()

            logger.info("Client workflow updated successfully: %s", workflow.name)
            return APIResponse(
                success=True,
                message=ClientWorkflowMessages.UPDATED_SUCCESS.format(name=workflow.name),
                data=[ClientWorkflowResponse(**workflow.dict())],
            )
        except Exception as e:
            logger.error("Error updating client workflow: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientWorkflowMessages.UPDATE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # DELETE
    # ─────────────────────────────
    @staticmethod
    async def delete_workflow(workflow_id: str) -> APIResponse:
        """Delete a client workflow"""
        logger.info("Deleting client workflow with ID: %s", workflow_id)
        try:
            workflow = await ClientWorkflows.get(PydanticObjectId(workflow_id))
            if not workflow:
                logger.warning("Client workflow not found with ID: %s", workflow_id)
                return APIResponse(
                    success=False,
                    message=ClientWorkflowMessages.NOT_FOUND.format(id=workflow_id),
                    data=None,
                )

            await workflow.delete()
            logger.info("Client workflow deleted successfully with ID: %s", workflow_id)
            return APIResponse(
                success=True,
                message=ClientWorkflowMessages.DELETED_SUCCESS.format(id=workflow_id),
                data=None,
            )
        except Exception as e:
            logger.error("Error deleting client workflow: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientWorkflowMessages.DELETE_ERROR.format(error=str(e))
            )