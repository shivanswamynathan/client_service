from fastapi import APIRouter, Depends, status
from sqlalchemy.ext.asyncio import AsyncSession
from client_service.services.vendor_classification_service import VendorClassificationService
from client_service.api.dependencies import get_database_session
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import (
    VendorClassificationCreate,
    VendorClassificationUpdate
)
from uuid import UUID
from typing import List

router = APIRouter()


@router.post(
    "/vendors/classifications/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_vendor_class",
    summary="Create vendor classification",
    description="Assigns a vendor to an expense category for a client entity. Use when: 'classify vendor', 'assign category to vendor'.",
)
async def create_vendor_classification(
    classification_data: List[VendorClassificationCreate],
    db: AsyncSession = Depends(get_database_session)
):
    """Create a new vendor classification"""
    return await VendorClassificationService.create(classification_data, db)


@router.get(
    "/vendors/classifications/{client_entity_id}/{expense_category_id}/{vendor_id}",
    response_model=APIResponse,
    operation_id="get_vendor_class",
    summary="Get vendor classification by keys",
    description="Retrieves classification by composite keys. Use when: 'get vendor classification'.",
)
async def get_vendor_classification(
    client_entity_id: UUID,
    expense_category_id: UUID,
    vendor_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Get a vendor classification by keys"""
    return await VendorClassificationService.get_by_keys(client_entity_id, expense_category_id, vendor_id, db)


@router.get(
    "/vendors/classifications",
    response_model=APIResponse,
    operation_id="list_vendor_classes",
    summary="List all vendor classifications",
    description="Get all classifications with pagination. Use when: 'list vendor classifications'.",
)
async def get_all_vendor_classifications(
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session)
):
    """Get all vendor classifications with pagination"""
    return await VendorClassificationService.get_all(skip, limit, db)


@router.put(
    "/vendors/classifications/{client_entity_id}/{expense_category_id}/{vendor_id}",
    response_model=APIResponse,
    operation_id="update_vendor_class",
    summary="Update vendor classification",
    description="Updates classification (limited for junction table). Use when: 'update vendor classification'.",
)
async def update_vendor_classification(
    client_entity_id: UUID,
    expense_category_id: UUID,
    vendor_id: UUID,
    update_data: VendorClassificationUpdate,
    db: AsyncSession = Depends(get_database_session)
):
    """Update a vendor classification"""
    return await VendorClassificationService.update(client_entity_id, expense_category_id, vendor_id, update_data, db)


@router.delete(
    "/vendors/classifications/{client_entity_id}/{expense_category_id}/{vendor_id}",
    response_model=APIResponse,
    operation_id="delete_vendor_class",
    summary="Delete vendor classification",
    description="Removes vendor classification. Use when: 'unclassify vendor', 'remove category assignment'.",
)
async def delete_vendor_classification(
    client_entity_id: UUID,
    expense_category_id: UUID,
    vendor_id: UUID,
    db: AsyncSession = Depends(get_database_session)
):
    """Delete a vendor classification"""
    return await VendorClassificationService.delete(client_entity_id, expense_category_id, vendor_id, db)