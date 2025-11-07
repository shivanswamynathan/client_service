from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.vendor_models import ActionLog, TransactionLog, VendorTransactions
from client_service.schemas.client_db.user_models import UserLog, Users
from client_service.schemas.pydantic_schemas import (
    ActionLogCreate, ActionLogResponse,
    TransactionLogCreate, TransactionLogResponse,
    UserLogCreate, UserLogResponse
)
from client_service.api.constants.messages import LogMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
import logging
from uuid import UUID
from typing import List 

logger = logging.getLogger(__name__)


class LogService:
    """Service class for Log business logic"""
    
    # ==================== ACTION LOG METHODS ====================
    
    @staticmethod
    async def create_action_log(action_log_data: List[ActionLogCreate]  , db: AsyncSession):
        """Create a new action log"""
        created_action_logs = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not action_log_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Action logs list cannot be empty. Provide at least one action log."
                )
            
            logger.info(f"Processing creation of {len(action_log_data)} action log(s)")
            
            # Validation Phase 2: Create all new action log records
            new_action_logs = []
            for action_log_data in action_log_data:
                try:
                    # Create new action log instance (UUID will be auto-generated)
                    new_action_log = ActionLog(**action_log_data.model_dump(exclude_unset=True))
                    new_action_logs.append(new_action_log)
                    
                except Exception as e:
                    logger.error(f"Error preparing action log: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing action log: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_action_log in new_action_logs:
                db.add(new_action_log)
            
            await db.commit()
            
            # Refresh all action logs to get generated IDs
            for new_action_log in new_action_logs:
                await db.refresh(new_action_log)
                created_action_logs.append(
                    ActionLogResponse.model_validate(new_action_log).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_action_logs)} action log(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_action_logs)} action log(s)",
                data=created_action_logs
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(LogMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id_action_log(log_id: UUID, db: AsyncSession):
        """Get an action log by ID"""
        try:
            result = await db.execute(
                select(ActionLog).where(ActionLog.log_id == log_id)
            )
            action_log = result.scalar_one_or_none()
            
            if not action_log:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=LogMessages.LOG_NOT_FOUND.format(id=log_id)
                )
            
            logger.info(LogMessages.LOG_RETRIEVED.format(id=action_log.log_id))
            return APIResponse(
                success=True,
                message=LogMessages.LOG_RETRIEVED.format(id=action_log.log_id),
                data=ActionLogResponse.model_validate(action_log).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(LogMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all_action_logs(skip: int, limit: int, db: AsyncSession):
        """Get all action logs with pagination"""
        try:
            result = await db.execute(
                select(ActionLog).offset(skip).limit(limit)
            )
            action_logs = result.scalars().all()
            
            logger.info(LogMessages.LOGS_RETRIEVED.format(count=len(action_logs)))
            return APIResponse(
                success=True,
                message=LogMessages.LOGS_RETRIEVED.format(count=len(action_logs)),
                data=[ActionLogResponse.model_validate(log).model_dump() for log in action_logs]
            )

        except Exception as e:
            logger.error(LogMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    # ==================== TRANSACTION LOG METHODS ====================
    
    @staticmethod
    async def create_transaction_log(transaction_log_data: List[TransactionLogCreate], db: AsyncSession):
        """Create a new transaction log"""
        created_transaction_logs = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not transaction_log_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Transaction logs list cannot be empty. Provide at least one transaction log."
                )
            
            logger.info(f"Processing creation of {len(transaction_log_data)} transaction log(s)")
            
            # Validation Phase 2: Collect all unique transaction IDs
            unique_transaction_ids = set(
                tl.transaction_id 
                for tl in transaction_log_data 
                if tl.transaction_id is not None
            )
            
            # Validation Phase 3: Verify all transactions exist (if provided)
            if unique_transaction_ids:
                transactions_result = await db.execute(
                    select(VendorTransactions).where(VendorTransactions.transaction_id.in_(unique_transaction_ids))
                )
                existing_transactions = transactions_result.scalars().all()
                existing_transaction_ids = {t.transaction_id for t in existing_transactions}
                missing_transaction_ids = unique_transaction_ids - existing_transaction_ids
                
                if missing_transaction_ids:
                    logger.warning(f"Transaction IDs not found: {missing_transaction_ids}")
                    raise HTTPException(
                        status_code=StatusCode.NOT_FOUND,
                        detail=f"The following transaction IDs do not exist: {', '.join(str(id) for id in missing_transaction_ids)}"
                    )
            
            # Validation Phase 4: Collect all unique action log IDs and user log IDs
            unique_action_log_ids = set(
                tl.action_log_id 
                for tl in transaction_log_data 
                if tl.action_log_id is not None
            )
            unique_user_log_ids = set(
                tl.user_log_id 
                for tl in transaction_log_data 
                if tl.user_log_id is not None
            )
            
            # Validation Phase 5: Verify all action logs exist (if provided)
            if unique_action_log_ids:
                action_logs_result = await db.execute(
                    select(ActionLog).where(ActionLog.log_id.in_(unique_action_log_ids))
                )
                existing_action_logs = action_logs_result.scalars().all()
                existing_action_log_ids = {al.log_id for al in existing_action_logs}
                missing_action_log_ids = unique_action_log_ids - existing_action_log_ids
                
                if missing_action_log_ids:
                    logger.warning(f"Action log IDs not found: {missing_action_log_ids}")
                    raise HTTPException(
                        status_code=StatusCode.NOT_FOUND,
                        detail=f"The following action log IDs do not exist: {', '.join(str(id) for id in missing_action_log_ids)}"
                    )
            
            # Validation Phase 6: Verify all user logs exist (if provided)
            if unique_user_log_ids:
                user_logs_result = await db.execute(
                    select(UserLog).where(UserLog.log_id.in_(unique_user_log_ids))
                )
                existing_user_logs = user_logs_result.scalars().all()
                existing_user_log_ids = {ul.log_id for ul in existing_user_logs}
                missing_user_log_ids = unique_user_log_ids - existing_user_log_ids
                
                if missing_user_log_ids:
                    logger.warning(f"User log IDs not found: {missing_user_log_ids}")
                    raise HTTPException(
                        status_code=StatusCode.NOT_FOUND,
                        detail=f"The following user log IDs do not exist: {', '.join(str(id) for id in missing_user_log_ids)}"
                    )
            
            # Validation Phase 7: Create all new transaction log records
            new_transaction_logs = []
            for transaction_log_data in transaction_log_data:
                try:
                    # Create new transaction log instance (UUID will be auto-generated)
                    new_transaction_log = TransactionLog(**transaction_log_data.model_dump(exclude_unset=True))
                    new_transaction_logs.append(new_transaction_log)
                    
                except Exception as e:
                    logger.error(f"Error preparing transaction log: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing transaction log: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_transaction_log in new_transaction_logs:
                db.add(new_transaction_log)
            
            await db.commit()
            
            # Refresh all transaction logs to get generated IDs
            for new_transaction_log in new_transaction_logs:
                await db.refresh(new_transaction_log)
                created_transaction_logs.append(
                    TransactionLogResponse.model_validate(new_transaction_log).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_transaction_logs)} transaction log(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_transaction_logs)} transaction log(s)",
                data=created_transaction_logs
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(LogMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id_transaction_log(log_id: UUID, db: AsyncSession):
        """Get a transaction log by ID"""
        try:
            result = await db.execute(
                select(TransactionLog).where(TransactionLog.log_id == log_id)
            )
            transaction_log = result.scalar_one_or_none()
            
            if not transaction_log:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=LogMessages.LOG_NOT_FOUND.format(id=log_id)
                )
            
            logger.info(LogMessages.LOG_RETRIEVED.format(id=transaction_log.log_id))
            return APIResponse(
                success=True,
                message=LogMessages.LOG_RETRIEVED.format(id=transaction_log.log_id),
                data=TransactionLogResponse.model_validate(transaction_log).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(LogMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_transaction_id(transaction_id: UUID, db: AsyncSession):
        """Get all transaction logs by transaction ID"""
        try:
            result = await db.execute(
                select(TransactionLog).where(TransactionLog.transaction_id == transaction_id)
            )
            transaction_logs = result.scalars().all()
            
            if not transaction_logs:
                logger.info(LogMessages.NO_LOGS_FOR_TRANSACTION.format(id=transaction_id))
                return []
            
            logger.info(LogMessages.LOGS_BY_TRANSACTION_RETRIEVED.format(count=len(transaction_logs), id=transaction_id))
            return APIResponse(
                success=True,
                message=LogMessages.LOGS_BY_TRANSACTION_RETRIEVED.format(count=len(transaction_logs), id=transaction_id),
                data=[TransactionLogResponse.model_validate(log).model_dump() for log in transaction_logs]
            )

        except Exception as e:
            logger.error(LogMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    # ==================== USER LOG METHODS ====================
    
    @staticmethod
    async def create_user_log(user_log_data: List[UserLogCreate], db: AsyncSession):
        """Create a new user log"""
        created_user_logs = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not user_log_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="User logs list cannot be empty. Provide at least one user log."
                )
            
            logger.info(f"Processing creation of {len(user_log_data)} user log(s)")
            
            # Validation Phase 2: Collect all unique user IDs
            unique_user_ids = set(ul.user_id for ul in user_log_data)
            
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
            
            # Validation Phase 4: Create all new user log records
            new_user_logs = []
            for user_log_data in user_log_data:
                try:
                    # Create new user log instance (UUID will be auto-generated)
                    new_user_log = UserLog(**user_log_data.model_dump(exclude_unset=True))
                    new_user_logs.append(new_user_log)
                    
                except Exception as e:
                    logger.error(f"Error preparing user log: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing user log: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_user_log in new_user_logs:
                db.add(new_user_log)
            
            await db.commit()
            
            # Refresh all user logs to get generated IDs
            for new_user_log in new_user_logs:
                await db.refresh(new_user_log)
                created_user_logs.append(
                    UserLogResponse.model_validate(new_user_log).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_user_logs)} user log(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_user_logs)} user log(s)",
                data=created_user_logs
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(LogMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id_user_log(log_id: UUID, db: AsyncSession):
        """Get a user log by ID"""
        try:
            result = await db.execute(
                select(UserLog).where(UserLog.log_id == log_id)
            )
            user_log = result.scalar_one_or_none()
            
            if not user_log:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=LogMessages.LOG_NOT_FOUND.format(id=log_id)
                )
            
            logger.info(LogMessages.LOG_RETRIEVED.format(id=user_log.log_id))
            return APIResponse(
                success=True,
                message=LogMessages.LOG_RETRIEVED.format(id=user_log.log_id),
                data=UserLogResponse.model_validate(user_log).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(LogMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_user_id(user_id: UUID, skip: int, limit: int, db: AsyncSession):
        """Get all user logs by user ID with pagination"""
        try:
            result = await db.execute(
                select(UserLog)
                .where(UserLog.user_id == user_id)
                .offset(skip)
                .limit(limit)
            )
            user_logs = result.scalars().all()
            
            if not user_logs:
                logger.info(LogMessages.NO_LOGS_FOR_USER.format(id=user_id))
                return []
            
            logger.info(LogMessages.LOGS_BY_USER_RETRIEVED.format(count=len(user_logs), id=user_id))
            return APIResponse(
                success=True,
                message=LogMessages.LOGS_BY_USER_RETRIEVED.format(count=len(user_logs), id=user_id),
                data=[UserLogResponse.model_validate(log).model_dump() for log in user_logs]
            )

        except Exception as e:
            logger.error(LogMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all_user_logs(skip: int, limit: int, db: AsyncSession):
        """Get all user logs with pagination"""
        try:
            result = await db.execute(
                select(UserLog).offset(skip).limit(limit)
            )
            user_logs = result.scalars().all()
            
            logger.info(LogMessages.LOGS_RETRIEVED.format(count=len(user_logs)))
            return APIResponse(
                success=True,
                message=LogMessages.LOGS_RETRIEVED.format(count=len(user_logs)),
                data=[UserLogResponse.model_validate(log).model_dump() for log in user_logs]
            )

        except Exception as e:
            logger.error(LogMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=LogMessages.RETRIEVE_ERROR.format(error=str(e))
            )