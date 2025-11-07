from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.vendor_models import VendorMaster
from client_service.schemas.pydantic_schemas import VendorCreate, VendorUpdate, VendorResponse
from client_service.api.constants.messages import VendorMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from datetime import datetime, timezone
from rapidfuzz import fuzz
import logging
from uuid import UUID
from typing import List

logger = logging.getLogger(__name__)


class VendorService:
    """Service class for Vendor business logic"""
    
    @staticmethod
    async def create(vendor_data: List[VendorCreate], db: AsyncSession):
        """Create a new vendor"""
        created_vendors = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not vendor_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Vendors list cannot be empty. Provide at least one vendor."
                )
            
            logger.info(f"Processing creation of {len(vendor_data)} vendor(s)")
            
            # Validation Phase 2: Collect all vendor codes for batch duplicate check
            batch_codes = {}  # {vendor_code: index}
            for idx, vendor in enumerate(vendor_data):
                if vendor.vendor_code in batch_codes:
                    logger.warning(f"Duplicate vendor code in batch: {vendor.vendor_code}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate vendor code in batch: {vendor.vendor_code} (also at position {batch_codes[vendor.vendor_code]})"
                    )
                else:
                    batch_codes[vendor.vendor_code] = idx
            
            # Validation Phase 3: Check for duplicate vendor codes in existing database
            existing_codes_result = await db.execute(
                select(VendorMaster).where(VendorMaster.vendor_code.in_(batch_codes.keys()))
            )
            existing_vendors = existing_codes_result.scalars().all()
            
            if existing_vendors:
                existing_codes = [v.vendor_code for v in existing_vendors]
                logger.warning(f"Vendor codes already exist in DB: {existing_codes}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following vendor codes already exist: {', '.join(existing_codes)}"
                )
            
            # Validation Phase 4: Create all new vendor records
            new_vendors = []
            for vendor_data in vendor_data:
                try:
                    # Create new vendor instance (UUID will be auto-generated)
                    new_vendor = VendorMaster(**vendor_data.model_dump(exclude_unset=True))
                    new_vendors.append(new_vendor)
                    
                except Exception as e:
                    logger.error(f"Error preparing vendor {vendor_data.vendor_code}: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing vendor {vendor_data.vendor_code}: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_vendor in new_vendors:
                db.add(new_vendor)
            
            await db.commit()
            
            # Refresh all vendors to get generated IDs
            for new_vendor in new_vendors:
                await db.refresh(new_vendor)
                created_vendors.append(
                    VendorResponse.model_validate(new_vendor).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_vendors)} vendor(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_vendors)} vendor(s)",
                data=created_vendors
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(VendorMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id(vendor_id: UUID, db: AsyncSession):
        """Get a vendor by ID"""
        try:
            result = await db.execute(
                select(VendorMaster).where(VendorMaster.vendor_id == vendor_id)
            )
            vendor = result.scalar_one_or_none()
            
            if not vendor:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=VendorMessages.NOT_FOUND.format(id=vendor_id)
                )
            
            logger.info(VendorMessages.RETRIEVED_SUCCESS.format(name=vendor.vendor_name))
            return APIResponse(
                success=True,
                message=VendorMessages.RETRIEVED_SUCCESS.format(name=vendor.vendor_name),
                data=VendorResponse.model_validate(vendor).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(VendorMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def search(column: str, value: str, db: AsyncSession, threshold: int = 70):
        """Search vendors by specific column and value"""
        try:
            # Dynamically get all column names from the model (excluding timestamps and IDs for search)
            excluded_columns = {'created_at', 'updated_at', 'vendor_id'}  # Columns we don't want to search
            
            # Get all columns from the VendorMaster model
            all_columns = {col.name: col for col in VendorMaster.__table__.columns}
            
            # Filter out excluded columns
            allowed_columns = {
                name: all_columns[name]
                for name in all_columns
                if name not in excluded_columns
            }
            
            # Validate column name
            if column not in allowed_columns:
                logger.warning(VendorMessages.INVALID_SEARCH_COLUMN.format(
                    column=column, 
                    allowed=', '.join(allowed_columns.keys())
                ))
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail=VendorMessages.INVALID_SEARCH_COLUMN.format(
                        column=column,
                        allowed=', '.join(allowed_columns.keys())
                    )
                )
            
            value = value.strip()
            
            # Get ALL vendors from database
            result = await db.execute(select(VendorMaster))
            all_vendors = result.scalars().all()
            
            # Filter vendors using fuzzy matching
            best_match = None
            best_score = 0
            
            for vendor in all_vendors:
                # Get the value of the column we're searching
                vendor_value = getattr(vendor, column)
                
                if vendor_value:
                    # Calculate similarity score
                    score = fuzz.partial_ratio(value.lower(), str(vendor_value).lower())
                    
                    if score >= threshold and score > best_score:
                        best_match = vendor
                        best_score = score
                        print(f"New best match: '{vendor_value}' (score: {score})")
            
            if not best_match:
                logger.info(VendorMessages.NO_SEARCH_RESULTS.format(column=column, value=value))
                return APIResponse(
                    success=True,
                    message=VendorMessages.NO_SEARCH_RESULTS.format(column=column, value=value),
                    data=[]
                )
            
            print(f"Final best match score: {best_score}")
            logger.info(f"Found best match for {column}='{value}' with score {best_score}")
            return APIResponse(
                success=True,
                message=f"Found best match where {column} matches '{value}' (score: {best_score})",
                data=[VendorResponse.model_validate(best_match).model_dump()]
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(VendorMessages.SEARCH_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorMessages.SEARCH_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(vendor_id: UUID, vendor_data: VendorUpdate, db: AsyncSession):
        """Update a vendor"""
        try:
            result = await db.execute(
                select(VendorMaster).where(VendorMaster.vendor_id == vendor_id)
            )
            vendor = result.scalar_one_or_none()
            
            if not vendor:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=VendorMessages.NOT_FOUND.format(id=vendor_id)
                )

            # Update fields
            for key, value in vendor_data.model_dump(exclude_unset=True).items():
                setattr(vendor, key, value)
            
            vendor.updated_at = datetime.now(timezone.utc)

            await db.commit()
            await db.refresh(vendor)
            
            logger.info(VendorMessages.UPDATED_SUCCESS.format(name=vendor.vendor_name))
            return APIResponse(
                success=True,
                message=VendorMessages.UPDATED_SUCCESS.format(name=vendor.vendor_name),
                data=VendorResponse.model_validate(vendor).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(VendorMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(vendor_id: UUID, db: AsyncSession):
        """Delete a vendor"""
        try:
            result = await db.execute(
                select(VendorMaster).where(VendorMaster.vendor_id == vendor_id)
            )
            vendor = result.scalar_one_or_none()
            
            if not vendor:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=VendorMessages.NOT_FOUND.format(id=vendor_id)
                )

            await db.delete(vendor)
            await db.commit()
            
            logger.info(VendorMessages.DELETED_SUCCESS.format(id=vendor_id))
            return APIResponse(
                success=True,
                message=VendorMessages.DELETED_SUCCESS.format(id=vendor_id),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(VendorMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=VendorMessages.DELETE_ERROR.format(error=str(e))
            )