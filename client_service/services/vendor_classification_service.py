from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.vendor_models import VendorClassification, VendorMaster
from client_service.schemas.client_db.client_models import ClientEntity
from client_service.schemas.client_db.expense_models import ExpenseMaster
from client_service.schemas.pydantic_schemas import VendorClassificationCreate, VendorClassificationUpdate, VendorClassificationResponse
from client_service.api.constants.messages import VendorClassificationMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from datetime import datetime, timezone
import logging
from uuid import UUID
from typing import List

logger = logging.getLogger(__name__)


class VendorClassificationService:
    """Service class for Vendor Classification business logic"""
    
    @staticmethod
    async def create(classification_data: List[VendorClassificationCreate], db: AsyncSession):
        """Create a new vendor classification"""
        created_classifications = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not classification_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Classifications list cannot be empty. Provide at least one classification."
                )
            
            logger.info(f"Processing creation of {len(classification_data)} vendor classification(s)")
            
            # Validation Phase 2: Collect all unique IDs
            unique_entity_ids = set(c.client_entity_id for c in classification_data)
            unique_category_ids = set(c.expense_category_id for c in classification_data)
            unique_vendor_ids = set(c.vendor_id for c in classification_data)

            # Validation Phase 3: Verify all client entities exist
            entities_result = await db.execute(
                select(ClientEntity).where(ClientEntity.entity_id.in_(unique_entity_ids))
            )
            existing_entities = entities_result.scalars().all()
            existing_entity_ids = {entity.entity_id for entity in existing_entities}
            missing_entity_ids = unique_entity_ids - existing_entity_ids
            
            if missing_entity_ids:
                logger.warning(f"Client entity IDs not found: {missing_entity_ids}")
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"The following client entity IDs do not exist: {', '.join(str(id) for id in missing_entity_ids)}"
                )
            
            # Validation Phase 4: Verify all expense categories exist
            categories_result = await db.execute(
                select(ExpenseMaster).where(ExpenseMaster.expense_category_id.in_(unique_category_ids))
            )
            existing_categories = categories_result.scalars().all()
            existing_category_ids = {category.expense_category_id for category in existing_categories}
            missing_category_ids = unique_category_ids - existing_category_ids
            
            if missing_category_ids:
                logger.warning(f"Expense category IDs not found: {missing_category_ids}")
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"The following expense category IDs do not exist: {', '.join(str(id) for id in missing_category_ids)}"
                )
            
            # Validation Phase 5: Verify all vendors exist
            vendors_result = await db.execute(
                select(VendorMaster).where(VendorMaster.vendor_id.in_(unique_vendor_ids))
            )
            existing_vendors = vendors_result.scalars().all()
            existing_vendor_ids = {vendor.vendor_id for vendor in existing_vendors}
            missing_vendor_ids = unique_vendor_ids - existing_vendor_ids
            
            if missing_vendor_ids:
                logger.warning(f"Vendor IDs not found: {missing_vendor_ids}")
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"The following vendor IDs do not exist: {', '.join(str(id) for id in missing_vendor_ids)}"
                )
            
            # Create lookup maps for reference
            entities_map = {entity.entity_id: entity for entity in existing_entities}
            # ExpenseMaster uses 'expense_category_id' as the PK field name
            categories_map = {category.expense_category_id: category for category in existing_categories}
            vendors_map = {vendor.vendor_id: vendor for vendor in existing_vendors}
            
            # Validation Phase 6: Check for duplicate classifications within batch
            batch_classifications = set()
            for idx, classification in enumerate(classification_data):
                classification_key = (
                    classification.client_entity_id,
                    classification.expense_category_id,
                    classification.vendor_id
                )
                if classification_key in batch_classifications:
                    logger.warning(f"Duplicate classification in batch: entity {classification.client_entity_id}, category {classification.expense_category_id}, vendor {classification.vendor_id}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate classification in batch for entity {classification.client_entity_id}, category {classification.expense_category_id}, vendor {classification.vendor_id}"
                    )
                batch_classifications.add(classification_key)
            
            # Validation Phase 7: Check for existing classifications in database
            existing_result = await db.execute(
                select(VendorClassification).where(
                    (VendorClassification.client_entity_id.in_(unique_entity_ids)) &
                    (VendorClassification.expense_category_id.in_(unique_category_ids)) &
                    (VendorClassification.vendor_id.in_(unique_vendor_ids))
                )
            )
            existing_classifications = existing_result.scalars().all()
            existing_classification_keys = {
                (vc.client_entity_id, vc.expense_category_id, vc.vendor_id)
                for vc in existing_classifications
            }
            
            # Check if any new classifications already exist
            already_existing = []
            for classification in classification_data:
                classification_key = (
                    classification.client_entity_id,
                    classification.expense_category_id,
                    classification.vendor_id
                )
                if classification_key in existing_classification_keys:
                    already_existing.append(classification_key)
            
            if already_existing:
                already_existing_str = [
                    f"entity {c[0]}, category {c[1]}, vendor {c[2]}"
                    for c in already_existing
                ]
                logger.warning(f"Classifications already exist: {already_existing_str}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following vendor classifications already exist: {', '.join(already_existing_str)}"
                )
            
            # Validation Phase 8: Create all new classification records
            new_classifications = []
            for classification_data in classification_data:
                try:
                    # Create new classification instance
                    new_classification = VendorClassification(**classification_data.model_dump(exclude_unset=True))
                    new_classifications.append(new_classification)
                    
                except Exception as e:
                    logger.error(f"Error preparing vendor classification: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing vendor classification: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_classification in new_classifications:
                db.add(new_classification)
            
            await db.commit()
            
            # Refresh all classifications to get metadata and build response
            for new_classification in new_classifications:
                await db.refresh(new_classification)
                
                # Get entity, category, and vendor names for logging
                entity = entities_map.get(new_classification.client_entity_id)
                category = categories_map.get(new_classification.expense_category_id)
                vendor = vendors_map.get(new_classification.vendor_id)
                
                created_classifications.append(
                    VendorClassificationResponse.model_validate(new_classification).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_classifications)} vendor classification(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_classifications)} vendor classification(s)",
                data=created_classifications
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(VendorClassificationMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorClassificationMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_keys(client_entity_id: UUID, expense_category_id: UUID, vendor_id: UUID, db: AsyncSession):
        """Get a vendor classification by composite keys"""
        try:
            result = await db.execute(
                select(VendorClassification).where(
                    VendorClassification.client_entity_id == client_entity_id,
                    VendorClassification.expense_category_id == expense_category_id,
                    VendorClassification.vendor_id == vendor_id
                )
            )
            classification = result.scalar_one_or_none()
            
            if not classification:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=VendorClassificationMessages.NOT_FOUND.format(
                        entity_id=client_entity_id,
                        category_id=expense_category_id,
                        vendor_id=vendor_id
                    )
                )

            # Fetch names for message
            vendor_result = await db.execute(
                select(VendorMaster).where(VendorMaster.vendor_id == vendor_id)
            )
            vendor_obj = vendor_result.scalar_one_or_none()
            vendor_name = vendor_obj.vendor_name if vendor_obj else "Unknown"

            category_result = await db.execute(
                select(ExpenseMaster).where(ExpenseMaster.expense_category_id == expense_category_id)
            )
            category_obj = category_result.scalar_one_or_none()
            category_name = category_obj.category_name if category_obj else "Unknown"

            logger.info(
                VendorClassificationMessages.RETRIEVED_SUCCESS.format(
                    vendor_name=vendor_name,
                    category_name=category_name
                )
            )
            
            logger.info(VendorClassificationMessages.RETRIEVED_SUCCESS.format(
                vendor_name=vendor_name,
                category_name=category_name
            ))
            return APIResponse(
                success=True,
                message=VendorClassificationMessages.RETRIEVED_SUCCESS.format(
                    vendor_name=vendor_name,
                    category_name=category_name
                ),
                data=VendorClassificationResponse.model_validate(classification).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(VendorClassificationMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorClassificationMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all vendor classifications with pagination"""
        try:
            result = await db.execute(
                select(VendorClassification).offset(skip).limit(limit)
            )
            classifications = result.scalars().all()
            
            logger.info(VendorClassificationMessages.RETRIEVED_ALL_SUCCESS.format(count=len(classifications)))
            return APIResponse(
                success=True,
                message=VendorClassificationMessages.RETRIEVED_ALL_SUCCESS.format(count=len(classifications)),
                data=[VendorClassificationResponse.model_validate(cl).model_dump() for cl in classifications]
            )

        except Exception as e:
            logger.error(VendorClassificationMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorClassificationMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(client_entity_id: UUID, expense_category_id: UUID, vendor_id: UUID, update_data: VendorClassificationUpdate, db: AsyncSession):
        """Update a vendor classification (limited fields, as junction)"""
        try:
            result = await db.execute(
                select(VendorClassification).where(
                    VendorClassification.client_entity_id == client_entity_id,
                    VendorClassification.expense_category_id == expense_category_id,
                    VendorClassification.vendor_id == vendor_id
                )
            )
            classification = result.scalar_one_or_none()
            
            if not classification:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=VendorClassificationMessages.NOT_FOUND.format(
                        entity_id=client_entity_id,
                        category_id=expense_category_id,
                        vendor_id=vendor_id
                    )
                )

            # For junction, updates might reassign (e.g., change category/vendor), but validate new FKs if provided
            update_dict = update_data.model_dump(exclude_unset=True)
            if 'expense_category_id' in update_dict and update_dict['expense_category_id'] != expense_category_id:
                new_category_result = await db.execute(
                    select(ExpenseMaster).where(ExpenseMaster.expense_category_id == update_dict['expense_category_id'])
                )
                if not new_category_result.scalar_one_or_none():
                    raise HTTPException(
                        status_code=StatusCode.NOT_FOUND,
                        detail=VendorClassificationMessages.CATEGORY_NOT_FOUND.format(category_id=update_dict['expense_category_id'])
                    )
                # Update key – but for simplicity, assume no key change; delete/recreate if needed
            if 'vendor_id' in update_dict and update_dict['vendor_id'] != vendor_id:
                new_vendor_result = await db.execute(
                    select(VendorMaster).where(VendorMaster.vendor_id == update_dict['vendor_id'])
                )
                if not new_vendor_result.scalar_one_or_none():
                    raise HTTPException(
                        status_code=StatusCode.NOT_FOUND,
                        detail=VendorClassificationMessages.VENDOR_NOT_FOUND.format(vendor_id=update_dict['vendor_id'])
                    )
                # Similar for entity

            # Since junction has no updatable fields beyond keys, this might be for future extensions
            # For now, log and return unchanged
            classification.updated_at = datetime.now(timezone.utc)
            await db.commit()
            await db.refresh(classification)
            
            logger.info(VendorClassificationMessages.UPDATED_SUCCESS)
            return APIResponse(
                success=True,
                message=VendorClassificationMessages.UPDATED_SUCCESS,
                data=VendorClassificationResponse.model_validate(classification).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(VendorClassificationMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorClassificationMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(client_entity_id: UUID, expense_category_id: UUID, vendor_id: UUID, db: AsyncSession):
        """Delete a vendor classification"""
        try:
            result = await db.execute(
                select(VendorClassification).where(
                    VendorClassification.client_entity_id == client_entity_id,
                    VendorClassification.expense_category_id == expense_category_id,
                    VendorClassification.vendor_id == vendor_id
                )
            )
            classification = result.scalar_one_or_none()
            
            if not classification:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=VendorClassificationMessages.NOT_FOUND.format(
                        entity_id=client_entity_id,
                        category_id=expense_category_id,
                        vendor_id=vendor_id
                    )
                )

            await db.delete(classification)
            await db.commit()
            
            logger.info(VendorClassificationMessages.DELETED_SUCCESS.format(
                vendor_id=vendor_id,
                category_id=expense_category_id
            ))
            return APIResponse(
                success=True,
                message=VendorClassificationMessages.DELETED_SUCCESS.format(
                    vendor_id=vendor_id,
                    category_id=expense_category_id
                ),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(VendorClassificationMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorClassificationMessages.DELETE_ERROR.format(error=str(e))
            )