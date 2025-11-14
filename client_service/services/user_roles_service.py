from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.user_models import UserRoles, Users, Roles
from client_service.schemas.pydantic_schemas import UserRoleCreate, UserRoleResponse
from client_service.api.constants.messages import UserRoleMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
import logging
from uuid import UUID
from typing import List

logger = logging.getLogger(__name__)


class UserRoleService:
    """Service class for UserRole business logic"""
    
    @staticmethod
    async def assign(user_role_data: List[UserRoleCreate], db: AsyncSession):
        """Assign a role to a user"""
        assigned_roles = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not user_role_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="User roles list cannot be empty. Provide at least one assignment."
                )
            
            logger.info(f"Processing assignment of {len(user_role_data)} role(s) to user(s)")
            
            # Validation Phase 2: Collect all unique user IDs and role IDs
            unique_user_ids = set(ur.user_id for ur in user_role_data)
            unique_role_ids = set(ur.role_id for ur in user_role_data)

            # Validation Phase 3: Verify all users exist
            users_result = await db.execute(
                select(Users).where(Users.user_id.in_(unique_user_ids))
            )
            existing_users = users_result.scalars().all()
            existing_user_ids = {user.user_id for user in existing_users}
            missing_user_ids = unique_user_ids - existing_user_ids
            
            if missing_user_ids:
                logger.warning(f"User IDs not found: {missing_user_ids}")
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"The following user IDs do not exist: {', '.join(str(id) for id in missing_user_ids)}"
                )
            
            # Validation Phase 4: Verify all roles exist
            roles_result = await db.execute(
                select(Roles).where(Roles.role_id.in_(unique_role_ids))
            )
            existing_roles = roles_result.scalars().all()
            existing_role_ids = {role.role_id for role in existing_roles}
            missing_role_ids = unique_role_ids - existing_role_ids
            
            if missing_role_ids:
                logger.warning(f"Role IDs not found: {missing_role_ids}")
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"The following role IDs do not exist: {', '.join(str(id) for id in missing_role_ids)}"
                )
            
            # Create user and role lookup maps for reference
            users_map = {user.user_id: user for user in existing_users}
            roles_map = {role.role_id: role for role in existing_roles}
            
            # Validation Phase 5: Check for duplicate assignments within batch
            batch_assignments = set()
            for idx, user_role in enumerate(user_role_data):
                assignment_key = (user_role.user_id, user_role.role_id)
                if assignment_key in batch_assignments:
                    logger.warning(f"Duplicate assignment in batch: user {user_role.user_id} to role {user_role.role_id}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate assignment in batch: user {user_role.user_id} with role {user_role.role_id} (also at position {idx - 1})"
                    )
                batch_assignments.add(assignment_key)
            
            # Validation Phase 6: Check for existing assignments in database
            existing_result = await db.execute(
                select(UserRoles).where(
                    (UserRoles.user_id.in_(unique_user_ids)) &
                    (UserRoles.role_id.in_(unique_role_ids))
                )
            )
            existing_assignments = existing_result.scalars().all()
            existing_assignment_keys = {(ur.user_id, ur.role_id) for ur in existing_assignments}
            
            # Check if any new assignments already exist
            already_assigned = []
            for user_role in user_role_data:
                assignment_key = (user_role.user_id, user_role.role_id)
                if assignment_key in existing_assignment_keys:
                    already_assigned.append(assignment_key)
            
            if already_assigned:
                already_assigned_str = [f"user {u[0]} with role {u[1]}" for u in already_assigned]
                logger.warning(f"Assignments already exist: {already_assigned_str}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following user-role assignments already exist: {', '.join(already_assigned_str)}"
                )
            
            # Validation Phase 7: Create all new user role records
            new_user_roles = []
            for user_role_data in user_role_data:
                try:
                    # Create new user role instance
                    new_user_role = UserRoles(**user_role_data.model_dump(exclude_unset=True))
                    new_user_roles.append(new_user_role)
                    
                except Exception as e:
                    logger.error(f"Error preparing user role assignment: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing user role assignment: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_user_role in new_user_roles:
                db.add(new_user_role)
            
            await db.commit()
            
            # Refresh all assignments to get metadata and build response
            for new_user_role in new_user_roles:
                await db.refresh(new_user_role)
                
                # Get user and role names for logging
                user = users_map.get(new_user_role.user_id)
                role = roles_map.get(new_user_role.role_id)
                
                assigned_roles.append(
                    UserRoleResponse.model_validate(new_user_role).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully assigned {len(assigned_roles)} role(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully assigned {len(assigned_roles)} role(s) to user(s)",
                data=assigned_roles
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(UserRoleMessages.ASSIGN_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=UserRoleMessages.ASSIGN_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_user_id(user_id: UUID, db: AsyncSession):
        """Get all roles for a user"""
        try:
            result = await db.execute(
                select(UserRoles).where(UserRoles.user_id == user_id)
            )
            user_roles = result.scalars().all()
            
            if not user_roles:
                logger.info(UserRoleMessages.NO_ROLES_FOR_USER.format(id=user_id))
                return []
            
            logger.info(
                UserRoleMessages.RETRIEVED_USER_ROLES_SUCCESS.format(
                    count=len(user_roles),
                    id=user_id
                )
            )
            return APIResponse(
                success=True,
                message=UserRoleMessages.RETRIEVED_USER_ROLES_SUCCESS.format(
                    count=len(user_roles),
                    id=user_id
                ),
                data=[UserRoleResponse.model_validate(role).model_dump() for role in user_roles]
            )

        except Exception as e:
            logger.error(UserRoleMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=UserRoleMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_role_id(role_id: UUID, db: AsyncSession):
        """Get all users with a role"""
        try:
            result = await db.execute(
                select(UserRoles).where(UserRoles.role_id == role_id)
            )
            user_roles = result.scalars().all()
            
            if not user_roles:
                logger.info(UserRoleMessages.NO_USERS_FOR_ROLE.format(id=role_id))
                return []
            
            logger.info(
                UserRoleMessages.RETRIEVED_ROLE_USERS_SUCCESS.format(
                    count=len(user_roles),
                    id=role_id
                )
            )
            return APIResponse(
                success=True,
                message=UserRoleMessages.RETRIEVED_ROLE_USERS_SUCCESS.format(
                    count=len(user_roles),
                    id=role_id
                ),
                data=[UserRoleResponse.model_validate(role).model_dump() for role in user_roles]
            )

        except Exception as e:
            logger.error(UserRoleMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=UserRoleMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def remove(user_id: UUID, role_id: UUID, db: AsyncSession):
        """Remove a role from a user"""
        try:
            result = await db.execute(
                select(UserRoles).where(
                    UserRoles.user_id == user_id,
                    UserRoles.role_id == role_id
                )
            )
            user_role = result.scalar_one_or_none()
            
            if not user_role:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=UserRoleMessages.ASSIGNMENT_NOT_FOUND.format(
                        user_id=user_id,
                        role_id=role_id
                    )
                )

            await db.delete(user_role)
            await db.commit()
            
            logger.info(
                UserRoleMessages.REMOVED_SUCCESS.format(
                    role_id=role_id,
                    user_id=user_id
                )
            )
            return APIResponse(
                success=True,
                message=UserRoleMessages.REMOVED_SUCCESS.format(
                    role_id=role_id,
                    user_id=user_id
                ),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(UserRoleMessages.REMOVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=UserRoleMessages.REMOVE_ERROR.format(error=str(e))
            )