from typing import List, Optional
from datetime import datetime, timezone
from beanie import PydanticObjectId
from fastapi import HTTPException
import logging

from client_service.schemas.mongo_schemas.client_workflow_execution import WorkflowExecutionLogs
from client_service.schemas.pydantic_schemas import (
    WorkflowExecutionLogCreate,
    WorkflowExecutionLogResponse
)
from client_service.schemas.base_response import APIResponse
from client_service.api.constants.status_codes import StatusCode
from client_service.api.constants.messages import WorkflowExecutionLogMessages

logger = logging.getLogger(__name__)

class WorkflowExecutionLogService:
    """Service class for managing workflow execution logs"""

    # ─────────────────────────────
    # CREATE
    # ─────────────────────────────
    @staticmethod
    async def create_log(data: WorkflowExecutionLogCreate) -> APIResponse:
        logger.info("Creating workflow execution log with data: %s", data.dict())
        try:
            log = WorkflowExecutionLogs(**data.dict())
            await log.insert()
            logger.info("Workflow execution log created successfully: %s", log.id)
            return APIResponse(
                success=True,
                message=WorkflowExecutionLogMessages.CREATED_SUCCESS.format(name="WorkflowExecutionLog"),
                data=[WorkflowExecutionLogResponse(**log.dict())]
            )
        except Exception as e:
            logger.error("Error creating workflow execution log: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowExecutionLogMessages.CREATE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # GET BY ID
    # ─────────────────────────────
    @staticmethod
    async def get_log_by_id(log_id: str) -> APIResponse:
        logger.info("Retrieving workflow execution log with ID: %s", log_id)
        try:
            log = await WorkflowExecutionLogs.get(PydanticObjectId(log_id))
            if not log:
                logger.warning("Workflow execution log not found with ID: %s", log_id)
                return APIResponse(
                    success=False,
                    message=WorkflowExecutionLogMessages.NOT_FOUND.format(id=log_id),
                    data=None
                )
            return APIResponse(
                success=True,
                message=WorkflowExecutionLogMessages.RETRIEVED_SUCCESS.format(name="WorkflowExecutionLog"),
                data=[WorkflowExecutionLogResponse(**log.dict())]
            )
        except Exception as e:
            logger.error("Error retrieving workflow execution log: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowExecutionLogMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # GET ALL
    # ─────────────────────────────
    @staticmethod
    async def get_all_logs(skip: int = 0, limit: int = 50) -> APIResponse:
        """Retrieve all workflow execution logs with pagination"""
        logger.info("Retrieving workflow execution logs (skip=%d, limit=%d)", skip, limit)
        try:
            logs = await WorkflowExecutionLogs.find_all().skip(skip).limit(limit).to_list()
            count = len(logs)
            logger.info("Retrieved %d workflow execution logs", count)
            return APIResponse(
                success=True,
                message=WorkflowExecutionLogMessages.RETRIEVED_ALL_SUCCESS.format(count=count),
                data=[WorkflowExecutionLogResponse(**log.dict()) for log in logs],
            )
        except Exception as e:
            logger.error("Error retrieving workflow execution logs: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowExecutionLogMessages.RETRIEVE_ALL_ERROR.format(error=str(e)),
            )

    # ─────────────────────────────
    # UPDATE
    # ─────────────────────────────
    @staticmethod
    async def update_log(log_id: str, data: dict) -> APIResponse:
        logger.info("Updating workflow execution log ID %s with data: %s", log_id, data)
        try:
            log = await WorkflowExecutionLogs.get(PydanticObjectId(log_id))
            if not log:
                return APIResponse(
                    success=False,
                    message=WorkflowExecutionLogMessages.NOT_FOUND.format(id=log_id),
                    data=None
                )

            for field, value in data.items():
                setattr(log, field, value)
            log.updated_at = datetime.now(timezone.utc)
            await log.save()

            return APIResponse(
                success=True,
                message=WorkflowExecutionLogMessages.UPDATED_SUCCESS.format(name="WorkflowExecutionLog"),
                data=[WorkflowExecutionLogResponse(**log.dict())]
            )
        except Exception as e:
            logger.error("Error updating workflow execution log: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowExecutionLogMessages.UPDATE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # DELETE
    # ─────────────────────────────
    @staticmethod
    async def delete_log(log_id: str) -> APIResponse:
        logger.info("Deleting workflow execution log with ID: %s", log_id)
        try:
            log = await WorkflowExecutionLogs.get(PydanticObjectId(log_id))
            if not log:
                return APIResponse(
                    success=False,
                    message=WorkflowExecutionLogMessages.NOT_FOUND.format(id=log_id),
                    data=None
                )

            await log.delete()
            return APIResponse(
                success=True,
                message=WorkflowExecutionLogMessages.DELETED_SUCCESS.format(id=log_id),
                data=None
            )
        except Exception as e:
            logger.error("Error deleting workflow execution log: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowExecutionLogMessages.DELETE_ERROR.format(error=str(e))
            )
