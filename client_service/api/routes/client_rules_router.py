from fastapi import APIRouter, status, Depends
from typing import List

from client_service.schemas.pydantic_schemas import (
    ClientRuleCreate,
    ClientRuleUpdate
)
from client_service.schemas.base_response import APIResponse
from client_service.services.client_rules_service import ClientRulesService

router = APIRouter()

# Dependency injection for the service
def get_client_rules_service() -> ClientRulesService:
    return ClientRulesService()

# ─────────────────────────────
# CREATE RULE
# ─────────────────────────────
@router.post(
    "/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new client rule",
    description="Adds a new client rule for workflow automation or validation logic."
)
async def create_rule(
    rule_data: ClientRuleCreate,
    service: ClientRulesService = Depends(get_client_rules_service)
):
    """Create a new client rule"""
    return await service.create_rule(rule_data)


# ─────────────────────────────
# GET RULE BY ID
# ─────────────────────────────
@router.get(
    "/{rule_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Get rule by ID",
    description="Retrieves details of a specific client rule using its MongoDB ObjectId."
)
async def get_rule_by_id(
    rule_id: str,
    service: ClientRulesService = Depends(get_client_rules_service)
):
    """Get a client rule by ID"""
    return await service.get_rule_by_id(rule_id)


# ─────────────────────────────
# GET ALL RULES
# ─────────────────────────────
@router.get(
    "/",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Get all client rules",
    description="Fetches all client rules with pagination support using `skip` and `limit` parameters."
)
async def get_all_rules(skip: int = 0, limit: int = 100,
    service: ClientRulesService = Depends(get_client_rules_service)
):
    """Get all client rules"""
    return await service.get_all_rules(skip, limit)


# ─────────────────────────────
# UPDATE RULE
# ─────────────────────────────
@router.put(
    "/{rule_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Update a client rule",
    description="Modifies an existing client rule identified by its ObjectId."
)
async def update_rule(
    rule_id: str,
    rule_data: ClientRuleUpdate,
    service: ClientRulesService = Depends(get_client_rules_service)
):
    """Update a client rule"""
    return await service.update_rule(rule_id, rule_data)


# ─────────────────────────────
# DELETE RULE
# ─────────────────────────────
@router.delete(
    "/{rule_id}",
    response_model=APIResponse,
    status_code=status.HTTP_200_OK,
    summary="Delete a client rule",
    description="Deletes a rule permanently by ID."
)
async def delete_rule(
    rule_id: str,
    service: ClientRulesService = Depends(get_client_rules_service)
):
    """Delete a client rule"""
    return await service.delete_rule(rule_id)