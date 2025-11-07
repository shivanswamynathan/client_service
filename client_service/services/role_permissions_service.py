from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.user_models import RolePermissions, Roles, Permissions
from client_service.schemas.pydantic_schemas import RolePermissionCreate, RolePermissionResponse
from client_service.api.constants.messages import RolePermissionMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
import logging
from uuid import UUID
from typing import List

logger = logging.getLogger(__name__)


class RolePermissionService:
    """Service class for RolePermission business logic"""
    
    @staticmethod
    async def assign(role_permission_data: List[RolePermissionCreate], db: AsyncSession):
        """Assign a permission to a role"""
        assigned_permissions = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not role_permission_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Role permissions list cannot be empty. Provide at least one assignment."
                )
            
            logger.info(f"Processing assignment of {len(role_permission_data)} permission(s) to role(s)")
            
            # Validation Phase 2: Collect all unique role IDs and permission IDs
            unique_role_ids = set(rp.role_id for rp in role_permission_data)
            unique_permission_ids = set(rp.permission_id for rp in role_permission_data)

            # Validation Phase 3: Verify all roles exist
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
            
            # Validation Phase 4: Verify all permissions exist
            permissions_result = await db.execute(
                select(Permissions).where(Permissions.permission_id.in_(unique_permission_ids))
            )
            existing_permissions = permissions_result.scalars().all()
            existing_permission_ids = {perm.permission_id for perm in existing_permissions}
            missing_permission_ids = unique_permission_ids - existing_permission_ids
            
            if missing_permission_ids:
                logger.warning(f"Permission IDs not found: {missing_permission_ids}")
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"The following permission IDs do not exist: {', '.join(str(id) for id in missing_permission_ids)}"
                )
            
            # Create role and permission lookup maps for reference
            roles_map = {role.role_id: role for role in existing_roles}
            permissions_map = {perm.permission_id: perm for perm in existing_permissions}
            
            # Validation Phase 5: Check for duplicate assignments within batch
            batch_assignments = set()
            for idx, role_permission in enumerate(role_permission_data):
                assignment_key = (role_permission.role_id, role_permission.permission_id)
                if assignment_key in batch_assignments:
                    logger.warning(f"Duplicate assignment in batch: role {role_permission.role_id} to permission {role_permission.permission_id}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate assignment in batch: role {role_permission.role_id} with permission {role_permission.permission_id} (also at position {idx - 1})"
                    )
                batch_assignments.add(assignment_key)
            
            # Validation Phase 6: Check for existing assignments in database
            existing_result = await db.execute(
                select(RolePermissions).where(
                    (RolePermissions.role_id.in_(unique_role_ids)) &
                    (RolePermissions.permission_id.in_(unique_permission_ids))
                )
            )
            existing_assignments = existing_result.scalars().all()
            existing_assignment_keys = {(ap.role_id, ap.permission_id) for ap in existing_assignments}
            
            # Check if any new assignments already exist
            already_assigned = []
            for role_permission in role_permission_data:
                assignment_key = (role_permission.role_id, role_permission.permission_id)
                if assignment_key in existing_assignment_keys:
                    already_assigned.append(assignment_key)
            
            if already_assigned:
                already_assigned_str = [f"role {r[0]} with permission {r[1]}" for r in already_assigned]
                logger.warning(f"Assignments already exist: {already_assigned_str}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following role-permission assignments already exist: {', '.join(already_assigned_str)}"
                )
            
            # Validation Phase 7: Create all new role permission records
            new_role_permissions = []
            for role_permission_data in role_permission_data:
                try:
                    # Create new role permission instance
                    new_role_permission = RolePermissions(**role_permission_data.model_dump(exclude_unset=True))
                    new_role_permissions.append(new_role_permission)
                    
                except Exception as e:
                    logger.error(f"Error preparing role permission assignment: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing role permission assignment: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_role_permission in new_role_permissions:
                db.add(new_role_permission)
            
            await db.commit()
            
            # Refresh all assignments to get metadata and build response
            for new_role_permission in new_role_permissions:
                await db.refresh(new_role_permission)
                
                # Get role and permission names for logging
                role = roles_map.get(new_role_permission.role_id)
                permission = permissions_map.get(new_role_permission.permission_id)
                
                assigned_permissions.append(
                    RolePermissionResponse.model_validate(new_role_permission).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully assigned {len(assigned_permissions)} permission(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully assigned {len(assigned_permissions)} permission(s) to role(s)",
                data=assigned_permissions
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(RolePermissionMessages.ASSIGN_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=RolePermissionMessages.ASSIGN_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_role_id(role_id: UUID, db: AsyncSession):
        """Get all permissions for a role"""
        try:
            result = await db.execute(
                select(RolePermissions).where(RolePermissions.role_id == role_id)
            )
            role_permissions = result.scalars().all()
            
            if not role_permissions:
                logger.info(RolePermissionMessages.NO_PERMISSIONS_FOR_ROLE.format(id=role_id))
                return []
            
            logger.info(
                RolePermissionMessages.RETRIEVED_ROLE_PERMISSIONS_SUCCESS.format(
                    count=len(role_permissions),
                    id=role_id
                )
            )
            return APIResponse(
                success=True,
                message=RolePermissionMessages.RETRIEVED_ROLE_PERMISSIONS_SUCCESS.format(
                    count=len(role_permissions),
                    id=role_id
                ),
                data=RolePermissionResponse.model_validate(role_permissions, many=True).model_dump()
            )

        except Exception as e:
            logger.error(RolePermissionMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=RolePermissionMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_permission_id(permission_id: UUID, db: AsyncSession):
        """Get all roles with a permission"""
        try:
            result = await db.execute(
                select(RolePermissions).where(RolePermissions.permission_id == permission_id)
            )
            role_permissions = result.scalars().all()
            
            if not role_permissions:
                logger.info(RolePermissionMessages.NO_ROLES_FOR_PERMISSION.format(id=permission_id))
                return []
            
            logger.info(
                RolePermissionMessages.RETRIEVED_PERMISSION_ROLES_SUCCESS.format(
                    count=len(role_permissions),
                    id=permission_id
                )
            )
            return APIResponse(
                success=True,
                message=RolePermissionMessages.RETRIEVED_PERMISSION_ROLES_SUCCESS.format(
                    count=len(role_permissions),
                    id=permission_id
                ),
                data=[RolePermissionResponse.model_validate(rp).model_dump() for rp in role_permissions]
            )

        except Exception as e:
            logger.error(RolePermissionMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=RolePermissionMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def remove(role_id: UUID, permission_id: UUID, db: AsyncSession):
        """Remove a permission from a role"""
        try:
            result = await db.execute(
                select(RolePermissions).where(
                    RolePermissions.role_id == role_id,
                    RolePermissions.permission_id == permission_id
                )
            )
            role_permission = result.scalar_one_or_none()
            
            if not role_permission:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=RolePermissionMessages.ASSIGNMENT_NOT_FOUND.format(
                        role_id=role_id,
                        permission_id=permission_id
                    )
                )

            await db.delete(role_permission)
            await db.commit()
            
            logger.info(
                RolePermissionMessages.REMOVED_SUCCESS.format(
                    permission_id=permission_id,
                    role_id=role_id
                )
            )
            return APIResponse(
                success=True,
                message=RolePermissionMessages.REMOVED_SUCCESS.format(
                    permission_id=permission_id,
                    role_id=role_id
                ),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(RolePermissionMessages.REMOVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=RolePermissionMessages.REMOVE_ERROR.format(error=str(e))
            )