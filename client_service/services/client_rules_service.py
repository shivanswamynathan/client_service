from typing import List, Optional
from datetime import datetime, timezone
from beanie import PydanticObjectId
from fastapi import HTTPException
import logging

from client_service.schemas.mongo_schemas.client_workflow_execution import ClientRules, ClientWorkflows
from client_service.schemas.pydantic_schemas import (
    ClientRuleCreate,
    ClientRuleUpdate,
    ClientRuleResponse
)
from client_service.schemas.base_response import APIResponse
from client_service.api.constants.status_codes import StatusCode
from client_service.api.constants.messages import ClientRuleMessages

# Initialize logger
logger = logging.getLogger(__name__)

class ClientRulesService:
    """Service class for managing client rules with uniform API responses"""

    @staticmethod
    async def create_rule(data: ClientRuleCreate) -> APIResponse:
        """Create a new client rule"""
        logger.info("Creating a new client rule with data: %s", data.dict())
        try:
            # Validate client_workflow_id
            workflow_id = data.client_workflow_id
            if not PydanticObjectId.is_valid(workflow_id):
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail=f"Invalid client_workflow_id: {workflow_id}. Must be a valid ObjectId."
                )

            # Check if the workflow exists
            workflow = await ClientWorkflows.get(PydanticObjectId(workflow_id))
            if not workflow:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"Client workflow with ID {workflow_id} not found."
                )

            # Create the rule
            rule = ClientRules(**data.dict())
            await rule.insert()
            logger.info("Client rule created successfully: %s", rule.name)
            return APIResponse(
                success=True,
                message=ClientRuleMessages.CREATED_SUCCESS.format(name=rule.name),
                data=[ClientRuleResponse(**rule.dict())],
            )
        except HTTPException as e:
            logger.error("Error creating client rule: %s", e.detail)
            raise e
        except Exception as e:
            logger.error("Error creating client rule: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientRuleMessages.CREATE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # READ: Get by ID
    # ─────────────────────────────
    @staticmethod
    async def get_rule_by_id(rule_id: str) -> APIResponse:
        """Retrieve a single client rule by ID"""
        logger.info("Retrieving client rule with ID: %s", rule_id)
        try:
            rule = await ClientRules.get(PydanticObjectId(rule_id))
            if not rule:
                logger.warning("Client rule not found with ID: %s", rule_id)
                return APIResponse(
                    success=False,
                    message=ClientRuleMessages.NOT_FOUND.format(id=rule_id),
                    data=None,
                )
            logger.info("Client rule retrieved successfully: %s", rule.name)
            return APIResponse(
                success=True,
                message=ClientRuleMessages.RETRIEVED_SUCCESS.format(name=rule.name),
                data=[ClientRuleResponse(**rule.dict())],
            )
        except Exception as e:
            logger.error("Error retrieving client rule: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientRuleMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # READ: Get All
    # ─────────────────────────────
    @staticmethod
    async def get_all_rules(skip: int = 0, limit: int = 50) -> APIResponse:
        """Retrieve all client rules with pagination"""
        logger.info("Retrieving client rules with pagination: skip=%s, limit=%s", skip, limit)
        try:
            # Fetch paginated rules
            rules = await ClientRules.find_all().skip(skip).limit(limit).to_list()

            logger.info("Retrieved %d client rules (paginated)", len(rules))
            return APIResponse(
                success=True,
                message=ClientRuleMessages.RETRIEVED_ALL_SUCCESS.format(count=len(rules)),
                data=[
                    ClientRuleResponse.model_validate(rule).model_dump()
                    for rule in rules
                ],
            )
        except Exception as e:
            logger.error("Error retrieving client rules: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientRuleMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # UPDATE
    # ─────────────────────────────
    @staticmethod
    async def update_rule(rule_id: str, data: ClientRuleUpdate) -> APIResponse:
        """Update a client rule"""
        logger.info("Updating client rule with ID: %s and data: %s", rule_id, data.dict(exclude_unset=True))
        try:
            rule = await ClientRules.get(PydanticObjectId(rule_id))
            if not rule:
                logger.warning("Client rule not found with ID: %s", rule_id)
                return APIResponse(
                    success=False,
                    message=ClientRuleMessages.NOT_FOUND.format(id=rule_id),
                    data=None,
                )

            update_data = data.dict(exclude_unset=True)
            for field, value in update_data.items():
                setattr(rule, field, value)

            rule.updated_at = datetime.now(timezone.utc)
            await rule.save()

            logger.info("Client rule updated successfully: %s", rule.name)
            return APIResponse(
                success=True,
                message=ClientRuleMessages.UPDATED_SUCCESS.format(name=rule.name),
                data=[ClientRuleResponse(**rule.dict())],
            )
        except Exception as e:
            logger.error("Error updating client rule: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientRuleMessages.UPDATE_ERROR.format(error=str(e))
            )

    # ─────────────────────────────
    # DELETE
    # ─────────────────────────────
    @staticmethod
    async def delete_rule(rule_id: str) -> APIResponse:
        """Delete a client rule"""
        logger.info("Deleting client rule with ID: %s", rule_id)
        try:
            rule = await ClientRules.get(PydanticObjectId(rule_id))
            if not rule:
                logger.warning("Client rule not found with ID: %s", rule_id)
                return APIResponse(
                    success=False,
                    message=ClientRuleMessages.NOT_FOUND.format(id=rule_id),
                    data=None,
                )

            await rule.delete()
            logger.info("Client rule deleted successfully with ID: %s", rule_id)
            return APIResponse(
                success=True,
                message=ClientRuleMessages.DELETED_SUCCESS.format(id=rule_id),
                data=None,
            )
        except Exception as e:
            logger.error("Error deleting client rule: %s", str(e))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientRuleMessages.DELETE_ERROR.format(error=str(e))
            )