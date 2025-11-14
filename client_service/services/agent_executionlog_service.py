from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
from beanie import PydanticObjectId
from fastapi import HTTPException
import logging

from client_service.schemas.mongo_schemas.client_workflow_execution import AgentExecutionLogs
from client_service.schemas.pydantic_schemas import (
    AgentExecutionLogCreate,
    AgentExecutionLogUpdate,
    AgentExecutionLogResponse
)
from client_service.schemas.base_response import APIResponse
from client_service.api.constants.status_codes import StatusCode
from client_service.api.constants.messages import AgentExecutionLogMessages

logger = logging.getLogger(__name__)

class AgentExecutionService:
    """Service class for managing agent execution logs"""

    # ─────────────────────────────
    # CREATE
    # ─────────────────────────────
    @staticmethod
    async def create_log(data: AgentExecutionLogCreate) -> APIResponse:
        logger.info("Creating agent execution log with data: %s", data.dict())
        try:
            log = AgentExecutionLogs(**data.dict())
            await log.insert()
            logger.info("Agent execution log created successfully: %s", log.id)
            return APIResponse(
                success=True,
                message=AgentExecutionLogMessages.CREATED_SUCCESS.format(name="AgentExecutionLog"),
                data=[AgentExecutionLogResponse(**log.dict())]
            )
        except Exception as e:
            logger.error("Error creating agent execution log: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=AgentExecutionLogMessages.CREATE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # GET BY ID
    # ─────────────────────────────
    @staticmethod
    async def get_log_by_id(log_id: str) -> APIResponse:
        logger.info("Retrieving agent execution log with ID: %s", log_id)
        try:
            log = await AgentExecutionLogs.get(PydanticObjectId(log_id))
            if not log:
                logger.warning("Agent execution log not found with ID: %s", log_id)
                return APIResponse(
                    success=False,
                    message=AgentExecutionLogMessages.NOT_FOUND.format(id=log_id),
                    data=None
                )
            return APIResponse(
                success=True,
                message=AgentExecutionLogMessages.RETRIEVED_SUCCESS.format(name="AgentExecutionLog"),
                data=[AgentExecutionLogResponse(**log.dict())]
            )
        except Exception as e:
            logger.error("Error retrieving agent execution log: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=AgentExecutionLogMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # GET ALL
    # ─────────────────────────────
    @staticmethod
    async def get_all_logs(skip: int = 0, limit: int = 50) -> APIResponse:
        """Retrieve all agent execution logs with pagination"""
        logger.info("Retrieving all agent execution logs with pagination skip=%s, limit=%s", skip, limit)
        try:
            logs = await AgentExecutionLogs.find_all().skip(skip).limit(limit).to_list()
            logger.info(AgentExecutionLogMessages.RETRIEVED_ALL_SUCCESS.format(count=len(logs)))

            return APIResponse(
                success=True,
                message=AgentExecutionLogMessages.RETRIEVED_ALL_SUCCESS.format(count=len(logs)),
                data=[
                    AgentExecutionLogResponse.model_validate(log).model_dump()
                    for log in logs
                ]
            )
        except Exception as e:
            logger.error("Error retrieving all agent execution logs: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=AgentExecutionLogMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )
    # ─────────────────────────────
    # UPDATE
    # ─────────────────────────────
    @staticmethod
    async def update_log(log_id: str, data: AgentExecutionLogUpdate) -> APIResponse:
        logger.info("Updating agent execution log ID %s with data: %s", log_id, data.dict(exclude_unset=True))
        try:
            log = await AgentExecutionLogs.get(PydanticObjectId(log_id))
            if not log:
                return APIResponse(
                    success=False,
                    message=AgentExecutionLogMessages.NOT_FOUND.format(id=log_id),
                    data=None
                )

            update_data = data.dict(exclude_unset=True)
            for field, value in update_data.items():
                setattr(log, field, value)

            log.updated_at = datetime.now(timezone.utc)
            await log.save()

            return APIResponse(
                success=True,
                message=AgentExecutionLogMessages.UPDATED_SUCCESS.format(name="AgentExecutionLog"),
                data=[AgentExecutionLogResponse(**log.dict())]
            )
        except Exception as e:
            logger.error("Error updating agent execution log: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=AgentExecutionLogMessages.UPDATE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # DELETE
    # ─────────────────────────────
    @staticmethod
    async def delete_log(log_id: str) -> APIResponse:
        logger.info("Deleting agent execution log with ID: %s", log_id)
        try:
            log = await AgentExecutionLogs.get(PydanticObjectId(log_id))
            if not log:
                return APIResponse(
                    success=False,
                    message=AgentExecutionLogMessages.NOT_FOUND.format(id=log_id),
                    data=None
                )

            await log.delete()
            return APIResponse(
                success=True,
                message=AgentExecutionLogMessages.DELETED_SUCCESS.format(id=log_id),
                data=None
            )
        except Exception as e:
            logger.error("Error deleting agent execution log: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=AgentExecutionLogMessages.DELETE_ERROR.format(error=str(e))
            )
