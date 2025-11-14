from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.expense_models import ExpenseMaster
from client_service.schemas.pydantic_schemas import ExpenseCategoryCreate, ExpenseCategoryUpdate, ExpenseCategoryResponse
from client_service.api.constants.messages import ExpenseCategoryMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from datetime import datetime, timezone
import logging
from uuid import UUID
from typing import List

logger = logging.getLogger(__name__)


class ExpenseService:
    """Service class for Expense Category business logic"""
    
    @staticmethod
    async def create(category_data: List[ExpenseCategoryCreate], db: AsyncSession):
        """Create a new expense category"""
         
        created_categories = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not category_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Categories list cannot be empty. Provide at least one category."
                )
            
            logger.info(f"Processing creation of {len(category_data)} expense category(ies)")
            
            # Validation Phase 2: Collect all category names for batch duplicate check
            batch_names = {}  # {category_name: index}
            for idx, category in enumerate(category_data):
                if category.category_name in batch_names:
                    logger.warning(f"Duplicate category name in batch: {category.category_name}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate category name in batch: {category.category_name} (also at position {batch_names[category.category_name]})"
                    )
                else:
                    batch_names[category.category_name] = idx
            
            # Validation Phase 3: Check for duplicates in existing database
            existing_result = await db.execute(
                select(ExpenseMaster).where(ExpenseMaster.category_name.in_(batch_names.keys()))
            )
            existing_categories = existing_result.scalars().all()
            
            if existing_categories:
                existing_names = [c.category_name for c in existing_categories]
                logger.warning(f"Categories already exist in DB: {existing_names}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following category names already exist: {', '.join(existing_names)}"
                )
            
            # Validation Phase 4: Create all new category records
            new_categories = []
            for category_data in category_data:
                try:
                    # Create new category instance (UUID will be auto-generated)
                    new_category = ExpenseMaster(**category_data.model_dump(exclude_unset=True))
                    new_categories.append(new_category)
                    
                except Exception as e:
                    logger.error(f"Error preparing category {category_data.category_name}: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing category {category_data.category_name}: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_category in new_categories:
                db.add(new_category)
            
            await db.commit()
            
            # Refresh all categories to get generated IDs
            for new_category in new_categories:
                await db.refresh(new_category)
                created_categories.append(
                    ExpenseCategoryResponse.model_validate(new_category).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_categories)} expense category(ies)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_categories)} expense category(ies)",
                data=created_categories
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(ExpenseCategoryMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ExpenseCategoryMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id(category_id: UUID, db: AsyncSession):
        """Get an expense category by ID"""
        try:
            result = await db.execute(
                select(ExpenseMaster).where(ExpenseMaster.expense_category_id == category_id)
            )
            category = result.scalar_one_or_none()
            
            if not category:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ExpenseCategoryMessages.NOT_FOUND.format(id=category_id)
                )
            
            logger.info(ExpenseCategoryMessages.RETRIEVED_SUCCESS.format(name=category.category_name))
            return APIResponse(
                success=True,
                message=ExpenseCategoryMessages.RETRIEVED_SUCCESS.format(name=category.category_name),
                data=ExpenseCategoryResponse.model_validate(category).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ExpenseCategoryMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ExpenseCategoryMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all expense categories with pagination"""
        try:
            result = await db.execute(
                select(ExpenseMaster).offset(skip).limit(limit)
            )
            categories = result.scalars().all()
            
            logger.info(ExpenseCategoryMessages.RETRIEVED_ALL_SUCCESS.format(count=len(categories)))
            return APIResponse(
                success=True,
                message=ExpenseCategoryMessages.RETRIEVED_ALL_SUCCESS.format(count=len(categories)),
                data=[ExpenseCategoryResponse.model_validate(cat).model_dump() for cat in categories]
            )

        except Exception as e:
            logger.error(ExpenseCategoryMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ExpenseCategoryMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(category_id: UUID, category_data: ExpenseCategoryUpdate, db: AsyncSession):
        """Update an expense category"""
        try:
            result = await db.execute(
                select(ExpenseMaster).where(ExpenseMaster.expense_category_id == category_id)
            )
            category = result.scalar_one_or_none()
            
            if not category:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ExpenseCategoryMessages.NOT_FOUND.format(id=category_id)
                )

            # Check duplicate name if updated
            update_data = category_data.model_dump(exclude_unset=True)
            if 'category_name' in update_data:
                name_result = await db.execute(
                    select(ExpenseMaster).where(
                        ExpenseMaster.category_name == update_data['category_name'],
                        ExpenseMaster.expense_category_id != category_id
                    )
                )
                if name_result.scalar_one_or_none():
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=ExpenseCategoryMessages.DUPLICATE_NAME.format(name=update_data['category_name'])
                    )

            # Update fields
            for key, value in update_data.items():
                setattr(category, key, value)
            
            category.updated_at = datetime.now(timezone.utc)

            await db.commit()
            await db.refresh(category)
            
            logger.info(ExpenseCategoryMessages.UPDATED_SUCCESS.format(name=category.category_name))
            return APIResponse(
                success=True,
                message=ExpenseCategoryMessages.UPDATED_SUCCESS.format(name=category.category_name),
                data=ExpenseCategoryResponse.model_validate(category).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(ExpenseCategoryMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ExpenseCategoryMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(category_id: UUID, db: AsyncSession):
        """Delete an expense category"""
        try:
            result = await db.execute(
                select(ExpenseMaster).where(ExpenseMaster.expense_category_id == category_id)
            )
            category = result.scalar_one_or_none()
            
            if not category:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ExpenseCategoryMessages.NOT_FOUND.format(id=category_id)
                )

            await db.delete(category)
            await db.commit()
            
            logger.info(ExpenseCategoryMessages.DELETED_SUCCESS.format(id=category_id))
            return APIResponse(
                success=True,
                message=ExpenseCategoryMessages.DELETED_SUCCESS.format(id=category_id),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(ExpenseCategoryMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ExpenseCategoryMessages.DELETE_ERROR.format(error=str(e))
            )