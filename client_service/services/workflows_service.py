from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.workflow_models import WorkflowRequestLedger
from client_service.schemas.client_db.client_models import Clients
from client_service.schemas.client_db.user_models import Users
from client_service.schemas.pydantic_schemas import WorkflowCreate, WorkflowUpdate, WorkflowResponse
from client_service.api.constants.messages import WorkflowMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from datetime import datetime, timezone
import logging
from uuid import UUID
from typing import List

logger = logging.getLogger(__name__)


class WorkflowService:
    """Service class for Workflow business logic"""
    
    @staticmethod
    async def create(workflow_data: List[WorkflowCreate], db: AsyncSession):
        """Create a new workflow ledger"""
        created_workflows = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not workflow_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Workflows list cannot be empty. Provide at least one workflow."
                )
            
            logger.info(f"Processing creation of {len(workflow_data)} workflow(s)")
            
            # Validation Phase 2: Collect all unique client IDs and user IDs
            unique_client_ids = set(workflow.client_id for workflow in workflow_data)
            unique_user_ids = set(workflow.user_id for workflow in workflow_data)

            # Validation Phase 3: Verify all clients exist
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
            
            # Validation Phase 4: Verify all users exist
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
            
            # Validation Phase 5: Create all new workflow records
            new_workflows = []
            for workflow_data in workflow_data:
                try:
                    # Create new workflow instance (UUID will be auto-generated)
                    new_workflow = WorkflowRequestLedger(**workflow_data.model_dump(exclude_unset=True))
                    new_workflows.append(new_workflow)
                    
                except Exception as e:
                    logger.error(f"Error preparing workflow {workflow_data.workflow_name}: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing workflow {workflow_data.workflow_name}: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_workflow in new_workflows:
                db.add(new_workflow)
            
            await db.commit()
            
            # Refresh all workflows to get generated IDs
            for new_workflow in new_workflows:
                await db.refresh(new_workflow)
                created_workflows.append(
                    WorkflowResponse.model_validate(new_workflow).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_workflows)} workflow(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_workflows)} workflow(s)",
                data=created_workflows
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(WorkflowMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id(ledger_id: UUID, db: AsyncSession):
        """Get a workflow ledger by ID"""
        try:
            result = await db.execute(
                select(WorkflowRequestLedger).where(WorkflowRequestLedger.ledger_id == ledger_id)
            )
            workflow = result.scalar_one_or_none()
            
            if not workflow:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=WorkflowMessages.NOT_FOUND.format(id=ledger_id)
                )
            
            logger.info(WorkflowMessages.RETRIEVED_SUCCESS.format(name=workflow.workflow_name))
            return APIResponse(
                success=True,
                message=WorkflowMessages.RETRIEVED_SUCCESS.format(name=workflow.workflow_name),
                data=WorkflowResponse.model_validate(workflow).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(WorkflowMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all workflow ledgers with pagination"""
        try:
            result = await db.execute(
                select(WorkflowRequestLedger).offset(skip).limit(limit)
            )
            workflows = result.scalars().all()
            
            logger.info(WorkflowMessages.RETRIEVED_ALL_SUCCESS.format(count=len(workflows)))
            return APIResponse(
                success=True,
                message=WorkflowMessages.RETRIEVED_ALL_SUCCESS.format(count=len(workflows)),
                data=[WorkflowResponse.model_validate(workflow).model_dump() for workflow in workflows]
            )

        except Exception as e:
            logger.error(WorkflowMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_client_id(client_id: UUID, db: AsyncSession):
        """Get all workflow ledgers by client ID"""
        try:
            result = await db.execute(
                select(WorkflowRequestLedger).where(WorkflowRequestLedger.client_id == client_id)
            )
            workflows = result.scalars().all()
            
            if not workflows:
                logger.info(WorkflowMessages.NO_WORKFLOWS_FOR_CLIENT.format(id=client_id))
                return []
            
            logger.info(WorkflowMessages.RETRIEVED_BY_CLIENT_SUCCESS.format(count=len(workflows), id=client_id))
            return APIResponse(
                success=True,
                message=WorkflowMessages.RETRIEVED_BY_CLIENT_SUCCESS.format(count=len(workflows), id=client_id),
                data=[WorkflowResponse.model_validate(workflow).model_dump() for workflow in workflows]
            )

        except Exception as e:
            logger.error(WorkflowMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_user_id(user_id: UUID, db: AsyncSession):
        """Get all workflow ledgers by user ID"""
        try:
            result = await db.execute(
                select(WorkflowRequestLedger).where(WorkflowRequestLedger.user_id == user_id)
            )
            workflows = result.scalars().all()
            
            if not workflows:
                logger.info(WorkflowMessages.NO_WORKFLOWS_FOR_USER.format(id=user_id))
                return []
            
            logger.info(WorkflowMessages.RETRIEVED_BY_USER_SUCCESS.format(count=len(workflows), id=user_id))
            return APIResponse(
                success=True,
                message=WorkflowMessages.RETRIEVED_BY_USER_SUCCESS.format(count=len(workflows), id=user_id),
                data=[WorkflowResponse.model_validate(workflow).model_dump() for workflow in workflows]
            )

        except Exception as e:
            logger.error(WorkflowMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(ledger_id: UUID, workflow_data: WorkflowUpdate, db: AsyncSession):
        """Update a workflow ledger"""
        try:
            result = await db.execute(
                select(WorkflowRequestLedger).where(WorkflowRequestLedger.ledger_id == ledger_id)
            )
            workflow = result.scalar_one_or_none()
            
            if not workflow:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=WorkflowMessages.NOT_FOUND.format(id=ledger_id)
                )

            # Update fields
            for key, value in workflow_data.model_dump(exclude_unset=True).items():
                setattr(workflow, key, value)
            
            workflow.updated_at = datetime.now(timezone.utc)

            await db.commit()
            await db.refresh(workflow)
            
            logger.info(WorkflowMessages.UPDATED_SUCCESS.format(name=workflow.workflow_name))
            return APIResponse(
                success=True,
                message=WorkflowMessages.UPDATED_SUCCESS.format(name=workflow.workflow_name),
                data=WorkflowResponse.model_validate(workflow).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(WorkflowMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def increment(ledger_id: UUID, db: AsyncSession):
        """Increment the request count for a workflow ledger"""
        try:
            result = await db.execute(
                select(WorkflowRequestLedger).where(WorkflowRequestLedger.ledger_id == ledger_id)
            )
            workflow = result.scalar_one_or_none()
            
            if not workflow:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=WorkflowMessages.NOT_FOUND.format(id=ledger_id)
                )

            # Increment count and update timestamps
            workflow.request_count += 1
            workflow.last_request_at = datetime.now(timezone.utc)
            workflow.updated_at = datetime.now(timezone.utc)

            await db.commit()
            await db.refresh(workflow)
            
            logger.info(WorkflowMessages.REQUEST_INCREMENTED)
            
            return APIResponse(
                success=True,
                message=WorkflowMessages.REQUEST_INCREMENTED,
                data=WorkflowResponse.model_validate(workflow).model_dump()
            )


        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(WorkflowMessages.INCREMENT_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowMessages.INCREMENT_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(ledger_id: UUID, db: AsyncSession):
        """Delete a workflow ledger"""
        try:
            result = await db.execute(
                select(WorkflowRequestLedger).where(WorkflowRequestLedger.ledger_id == ledger_id)
            )
            workflow = result.scalar_one_or_none()
            
            if not workflow:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=WorkflowMessages.NOT_FOUND.format(id=ledger_id)
                )

            await db.delete(workflow)
            await db.commit()
            
            logger.info(WorkflowMessages.DELETED_SUCCESS.format(id=ledger_id))
            return APIResponse(
                success=True,
                message=WorkflowMessages.DELETED_SUCCESS.format(id=ledger_id),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(WorkflowMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=WorkflowMessages.DELETE_ERROR.format(error=str(e))
            )