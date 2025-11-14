from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.user_models import Roles
from client_service.schemas.pydantic_schemas import RoleCreate, RoleUpdate, RoleResponse
from client_service.api.constants.messages import RoleMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
import logging
from uuid import UUID
from typing import List

logger = logging.getLogger(__name__)


class RoleService:
    """Service class for Role business logic"""
    
    @staticmethod
    async def create(role_data: List[RoleCreate], db: AsyncSession):
        """Create a new role"""
        created_roles = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not role_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Roles list cannot be empty. Provide at least one role."
                )
            
            logger.info(f"Processing creation of {len(role_data)} role(s)")
            
            # Validation Phase 2: Collect all role names for batch duplicate check
            batch_names = {}  # {role_name: index}
            for idx, role in enumerate(role_data):
                if role.role_name in batch_names:
                    logger.warning(f"Duplicate role name in batch: {role.role_name}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate role name in batch: {role.role_name} (also at position {batch_names[role.role_name]})"
                    )
                else:
                    batch_names[role.role_name] = idx
            
            # Validation Phase 3: Check for duplicates in existing database
            existing_result = await db.execute(
                select(Roles).where(Roles.role_name.in_(batch_names.keys()))
            )
            existing_roles = existing_result.scalars().all()
            
            if existing_roles:
                existing_names = [r.role_name for r in existing_roles]
                logger.warning(f"Roles already exist in DB: {existing_names}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following role names already exist: {', '.join(existing_names)}"
                )
            
            # Validation Phase 4: Create all new role records
            new_roles = []
            for role_data in role_data:
                try:
                    # Create new role instance (UUID will be auto-generated)
                    new_role = Roles(**role_data.model_dump(exclude_unset=True))
                    new_roles.append(new_role)
                    
                except Exception as e:
                    logger.error(f"Error preparing role {role_data.role_name}: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing role {role_data.role_name}: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_role in new_roles:
                db.add(new_role)
            
            await db.commit()
            
            # Refresh all roles to get generated IDs
            for new_role in new_roles:
                await db.refresh(new_role)
                created_roles.append(
                    RoleResponse.model_validate(new_role).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_roles)} role(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_roles)} role(s)",
                data=created_roles
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(RoleMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=RoleMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id(role_id: UUID, db: AsyncSession):
        """Get a role by ID"""
        try:
            result = await db.execute(
                select(Roles).where(Roles.role_id == role_id)
            )
            role = result.scalar_one_or_none()
            
            if not role:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=RoleMessages.NOT_FOUND.format(id=role_id)
                )
            
            logger.info(RoleMessages.RETRIEVED_SUCCESS.format(name=role.role_name))
            return APIResponse(
                success=True,
                message=RoleMessages.RETRIEVED_SUCCESS.format(name=role.role_name),
                data=RoleResponse.model_validate(role).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(RoleMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=RoleMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all roles with pagination"""
        try:
            result = await db.execute(
                select(Roles).offset(skip).limit(limit)
            )
            roles = result.scalars().all()
            
            logger.info(RoleMessages.RETRIEVED_ALL_SUCCESS.format(count=len(roles)))
            return APIResponse(
                success=True,
                message=RoleMessages.RETRIEVED_ALL_SUCCESS.format(count=len(roles)),
                data=[RoleResponse.model_validate(role).model_dump() for role in roles]
            )

        except Exception as e:
            logger.error(RoleMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=RoleMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(role_id: UUID, role_data: RoleUpdate, db: AsyncSession):
        """Update a role"""
        try:
            result = await db.execute(
                select(Roles).where(Roles.role_id == role_id)
            )
            role = result.scalar_one_or_none()
            
            if not role:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=RoleMessages.NOT_FOUND.format(id=role_id)
                )

            # Update fields
            for key, value in role_data.model_dump(exclude_unset=True).items():
                setattr(role, key, value)

            await db.commit()
            await db.refresh(role)
            
            logger.info(RoleMessages.UPDATED_SUCCESS.format(name=role.role_name))
            return APIResponse(
                success=True,
                message=RoleMessages.UPDATED_SUCCESS.format(name=role.role_name),
                data=RoleResponse.model_validate(role).model_dump()
            )


        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(RoleMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=RoleMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(role_id: UUID, db: AsyncSession):
        """Delete a role"""
        try:
            result = await db.execute(
                select(Roles).where(Roles.role_id == role_id)
            )
            role = result.scalar_one_or_none()
            
            if not role:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=RoleMessages.NOT_FOUND.format(id=role_id)
                )

            await db.delete(role)
            await db.commit()
            
            logger.info(RoleMessages.DELETED_SUCCESS.format(id=role_id))
            return APIResponse(
                success=True,
                message=RoleMessages.DELETED_SUCCESS.format(id=role_id),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(RoleMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=RoleMessages.DELETE_ERROR.format(error=str(e))
            )