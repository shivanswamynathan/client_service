from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.user_models import Users,Roles
from client_service.schemas.client_db.client_models import Clients
from client_service.schemas.pydantic_schemas import UserCreate, UserUpdate, UserResponse
from client_service.api.constants.messages import UserMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from datetime import datetime, timezone
import logging
from uuid import UUID
from typing import List

logger = logging.getLogger(__name__)


class UserService:
    """Service class for User business logic"""
    
    @staticmethod
    async def create(user_data: List[UserCreate], db: AsyncSession):
        """Create a new user"""
        created_users = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not user_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Users list cannot be empty. Provide at least one user."
                )
            
            logger.info(f"Processing creation of {len(user_data)} user(s)")
            
            # Validation Phase 2: Collect all emails for batch duplicate check
            batch_emails = {}  # {email: index}
            for idx, user in enumerate(user_data):
                if user.email in batch_emails:
                    logger.warning(f"Duplicate email in batch: {user.email}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate email in batch: {user.email} (also at position {batch_emails[user.email]})"
                    )
                else:
                    batch_emails[user.email] = idx
            
            # Validation Phase 3: Check for duplicate emails in existing database
            existing_emails_result = await db.execute(
                select(Users).where(Users.email.in_(batch_emails.keys()))
            )
            existing_users_by_email = existing_emails_result.scalars().all()
            
            if existing_users_by_email:
                existing_emails = [u.email for u in existing_users_by_email]
                logger.warning(f"Emails already exist in DB: {existing_emails}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following emails already exist: {', '.join(existing_emails)}"
                )
            
            # Validation Phase 4: Collect all unique reporting_manager_ids
            unique_manager_ids = set(
                user.reporting_manager_id 
                for user in user_data 
                if user.reporting_manager_id is not None
            )
            
            # Validation Phase 5: Verify all reporting_manager_ids (roles) exist if provided
            if unique_manager_ids:
                roles_result = await db.execute(
                    select(Roles).where(Roles.role_id.in_(unique_manager_ids))
                )
                existing_roles = roles_result.scalars().all()
                existing_role_ids = {role.role_id for role in existing_roles}
                missing_role_ids = unique_manager_ids - existing_role_ids
                
                if missing_role_ids:
                    logger.warning(f"Role IDs (reporting managers) not found: {missing_role_ids}")
                    raise HTTPException(
                        status_code=StatusCode.NOT_FOUND,
                        detail=f"The following reporting manager role IDs do not exist: {', '.join(str(id) for id in missing_role_ids)}"
                    )
            
            # Validation Phase 6: Collect all unique client IDs
            unique_client_ids = set(user.client_id for user in user_data)
            
            # Validation Phase 7: Verify all clients exist
            clients_result = await db.execute(
                select(Clients).where(Clients.client_id.in_(unique_client_ids))
            )
            existing_clients = clients_result.scalars().all()
            existing_client_ids = {client.client_id for client in existing_clients}
            missing_client_ids = unique_client_ids - existing_client_ids
            
            if missing_client_ids:
                logger.warning(f"Client IDs not found: {missing_client_ids}")
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"The following client IDs do not exist: {', '.join(str(id) for id in missing_client_ids)}"
                )
            
            # Validation Phase 8: Create all new user records
            new_users = []
            for user_data in user_data:
                try:
                    # Create new user instance (UUID will be auto-generated)
                    new_user = Users(**user_data.model_dump(exclude_unset=True))
                    new_users.append(new_user)
                    
                except Exception as e:
                    logger.error(f"Error preparing user {user_data.user_name}: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing user {user_data.user_name}: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_user in new_users:
                db.add(new_user)
            
            await db.commit()
            
            # Refresh all users to get generated IDs
            for new_user in new_users:
                await db.refresh(new_user)
                created_users.append(
                    UserResponse.model_validate(new_user).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_users)} user(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_users)} user(s)",
                data=created_users
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(UserMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=UserMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id(user_id: UUID, db: AsyncSession):
        """Get a user by ID"""
        try:
            result = await db.execute(
                select(Users).where(Users.user_id == user_id)
            )
            user = result.scalar_one_or_none()
            
            if not user:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=UserMessages.NOT_FOUND.format(id=user_id)
                )
            
            logger.info(UserMessages.RETRIEVED_SUCCESS.format(name=user.user_name))
            return APIResponse(
                success=True,
                message=UserMessages.RETRIEVED_SUCCESS.format(name=user.user_name),
                data=UserResponse.model_validate(user).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(UserMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=UserMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all users with pagination"""
        try:
            result = await db.execute(
                select(Users).offset(skip).limit(limit)
            )
            users = result.scalars().all()
            
            logger.info(UserMessages.RETRIEVED_ALL_SUCCESS.format(count=len(users)))
            return APIResponse(
                success=True,
                message=UserMessages.RETRIEVED_ALL_SUCCESS.format(count=len(users)),
                data=[UserResponse.model_validate(user).model_dump() for user in users]
            )

        except Exception as e:
            logger.error(UserMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=UserMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(user_id: UUID, user_data: UserUpdate, db: AsyncSession):
        """Update a user"""
        try:
            result = await db.execute(
                select(Users).where(Users.user_id == user_id)
            )
            user = result.scalar_one_or_none()
            
            if not user:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=UserMessages.NOT_FOUND.format(id=user_id)
                )
            
            # Validate reporting_manager_id if updated
            update_data = user_data.model_dump(exclude_unset=True)
            if 'reporting_manager_id' in update_data and update_data['reporting_manager_id']:
                role_result = await db.execute(
                    select(Roles).where(Roles.role_id == update_data['reporting_manager_id'])
                )
                if not role_result.scalar_one_or_none():
                    raise HTTPException(
                        status_code=StatusCode.NOT_FOUND,
                        detail=UserMessages.REPORTING_MANAGER_NOT_FOUND.format(role_id=update_data['reporting_manager_id'])
                    )
            
            # Email uniqueness if updated
            if 'email' in update_data:
                email_result = await db.execute(
                    select(Users).where(
                        Users.email == update_data['email'],
                        Users.user_id != user_id
                    )
                )
                if email_result.scalar_one_or_none():
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=UserMessages.DUPLICATE_EMAIL.format(email=update_data['email'])
                    )

            # Update fields
            for key, value in user_data.model_dump(exclude_unset=True).items():
                setattr(user, key, value)
            
            user.updated_at = datetime.now(timezone.utc)

            await db.commit()
            await db.refresh(user)
            
            logger.info(UserMessages.UPDATED_SUCCESS.format(name=user.user_name))
            return APIResponse(
                success=True,
                message=UserMessages.UPDATED_SUCCESS.format(name=user.user_name),
                data=UserResponse.model_validate(user).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(UserMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=UserMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(user_id: UUID, db: AsyncSession):
        """Delete a user"""
        try:
            result = await db.execute(
                select(Users).where(Users.user_id == user_id)
            )
            user = result.scalar_one_or_none()
            
            if not user:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=UserMessages.NOT_FOUND.format(id=user_id)
                )

            await db.delete(user)
            await db.commit()
            
            logger.info(UserMessages.DELETED_SUCCESS.format(id=user_id))
            return APIResponse(
                success=True,
                message=UserMessages.DELETED_SUCCESS.format(id=user_id),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(UserMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=UserMessages.DELETE_ERROR.format(error=str(e))
            )
