from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.item_models import ItemMaster
from client_service.schemas.pydantic_schemas import ItemCreate, ItemUpdate, ItemResponse
from client_service.api.constants.messages import ItemMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from datetime import datetime, timezone
import logging
from uuid import UUID
from typing import List


logger = logging.getLogger(__name__)


class ItemService:
    """Service class for Item business logic"""
    
    @staticmethod
    async def create(item_data: List[ItemCreate], db: AsyncSession):
        """Create a new item"""
        created_items = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not item_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Items list cannot be empty. Provide at least one item."
                )
            
            logger.info(f"Processing creation of {len(item_data)} item(s)")
            
            # Validation Phase 2: Collect all item codes for batch duplicate check
            batch_codes = {}  # {item_code: index}
            for idx, item in enumerate(item_data):
                if item.item_code in batch_codes:
                    logger.warning(f"Duplicate item code in batch: {item.item_code}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate item code in batch: {item.item_code} (also at position {batch_codes[item.item_code]})"
                    )
                else:
                    batch_codes[item.item_code] = idx
            
            # Validation Phase 3: Check for duplicates in existing database
            existing_result = await db.execute(
                select(ItemMaster).where(ItemMaster.item_code.in_(batch_codes.keys()))
            )
            existing_items = existing_result.scalars().all()
            
            if existing_items:
                existing_codes = [i.item_code for i in existing_items]
                logger.warning(f"Items already exist in DB: {existing_codes}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following item codes already exist: {', '.join(existing_codes)}"
                )
            
            # Validation Phase 4: Create all new item records
            new_items = []
            for item_data in item_data:
                try:
                    # Create new item instance (UUID will be auto-generated)
                    new_item = ItemMaster(**item_data.model_dump(exclude_unset=True))
                    new_items.append(new_item)
                    
                except Exception as e:
                    logger.error(f"Error preparing item {item_data.item_code}: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing item {item_data.item_code}: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_item in new_items:
                db.add(new_item)
            
            await db.commit()
            
            # Refresh all items to get generated IDs
            for new_item in new_items:
                await db.refresh(new_item)
                created_items.append(
                    ItemResponse.model_validate(new_item).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_items)} item(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_items)} item(s)",
                data=created_items
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(ItemMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ItemMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id(item_id: UUID, db: AsyncSession):
        """Get an item by ID"""
        try:
            result = await db.execute(
                select(ItemMaster).where(ItemMaster.item_id == item_id)
            )
            item = result.scalar_one_or_none()
            
            if not item:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ItemMessages.NOT_FOUND.format(id=item_id)
                )
            
            logger.info(ItemMessages.RETRIEVED_SUCCESS.format(name=item.item_name))
            return APIResponse(
                success=True,
                message=ItemMessages.RETRIEVED_SUCCESS.format(name=item.item_name),
                data=ItemResponse.model_validate(item).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ItemMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ItemMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all items with pagination"""
        try:
            result = await db.execute(
                select(ItemMaster).offset(skip).limit(limit)
            )
            items = result.scalars().all()
            
            logger.info(ItemMessages.RETRIEVED_ALL_SUCCESS.format(count=len(items)))
            return APIResponse(
                success=True,
                message=ItemMessages.RETRIEVED_ALL_SUCCESS.format(count=len(items)),
                data=[ItemResponse.model_validate(item).model_dump() for item in items]
            )

        except Exception as e:
            logger.error(ItemMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ItemMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_code(item_code: str, db: AsyncSession):
        """Get an item by code"""
        try:
            result = await db.execute(
                select(ItemMaster).where(ItemMaster.item_code == item_code)
            )
            item = result.scalar_one_or_none()
            
            if not item:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ItemMessages.NOT_FOUND_BY_CODE.format(code=item_code)
                )
            
            logger.info(ItemMessages.RETRIEVED_BY_CODE_SUCCESS.format(name=item.item_name))
            return APIResponse(
                success=True,
                message=ItemMessages.RETRIEVED_BY_CODE_SUCCESS.format(name=item.item_name),
                data=ItemResponse.model_validate(item).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ItemMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ItemMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(item_id: UUID, item_data: ItemUpdate, db: AsyncSession):
        """Update an item"""
        try:
            result = await db.execute(
                select(ItemMaster).where(ItemMaster.item_id == item_id)
            )
            item = result.scalar_one_or_none()
            
            if not item:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ItemMessages.NOT_FOUND.format(id=item_id)
                )

            # Update fields
            for key, value in item_data.model_dump(exclude_unset=True).items():
                setattr(item, key, value)
            
            item.updated_at = datetime.now(timezone.utc)

            await db.commit()
            await db.refresh(item)
            
            logger.info(ItemMessages.UPDATED_SUCCESS.format(name=item.item_name))
            return APIResponse(
                success=True,
                message=ItemMessages.UPDATED_SUCCESS.format(name=item.item_name),
                data=ItemResponse.model_validate(item).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(ItemMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ItemMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(item_id: UUID, db: AsyncSession):
        """Delete an item"""
        try:
            result = await db.execute(
                select(ItemMaster).where(ItemMaster.item_id == item_id)
            )
            item = result.scalar_one_or_none()
            
            if not item:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ItemMessages.NOT_FOUND.format(id=item_id)
                )

            await db.delete(item)
            await db.commit()
            
            logger.info(ItemMessages.DELETED_SUCCESS.format(id=item_id))
            return APIResponse(
                success=True,
                message=ItemMessages.DELETED_SUCCESS.format(id=item_id),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(ItemMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ItemMessages.DELETE_ERROR.format(error=str(e))
            )