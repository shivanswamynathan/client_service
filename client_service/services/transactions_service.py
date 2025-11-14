from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.vendor_models import VendorTransactions, VendorMaster
from client_service.schemas.pydantic_schemas import TransactionCreate, TransactionUpdate, TransactionResponse
from client_service.api.constants.messages import TransactionMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from datetime import datetime, timezone
import logging
from uuid import UUID
from typing import List

logger = logging.getLogger(__name__)


class TransactionService:
    """Service class for Transaction business logic"""
    
    @staticmethod
    async def create(transaction_data: List[TransactionCreate], db: AsyncSession):
        """Create a new transaction"""
        created_transactions = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not transaction_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Transactions list cannot be empty. Provide at least one transaction."
                )
            
            logger.info(f"Processing creation of {len(transaction_data)} transaction(s)")
            
            # Validation Phase 2: Collect all invoice IDs for batch duplicate check
            batch_invoices = {}  # {invoice_id: index}
            for idx, transaction in enumerate(transaction_data):
                if transaction.invoice_id in batch_invoices:
                    logger.warning(f"Duplicate invoice ID in batch: {transaction.invoice_id}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate invoice ID in batch: {transaction.invoice_id} (also at position {batch_invoices[transaction.invoice_id]})"
                    )
                else:
                    batch_invoices[transaction.invoice_id] = idx
            
            # Validation Phase 3: Check for duplicate invoice IDs in existing database
            existing_invoices_result = await db.execute(
                select(VendorTransactions).where(VendorTransactions.invoice_id.in_(batch_invoices.keys()))
            )
            existing_transactions = existing_invoices_result.scalars().all()
            
            if existing_transactions:
                existing_invoice_ids = [t.invoice_id for t in existing_transactions]
                logger.warning(f"Invoice IDs already exist in DB: {existing_invoice_ids}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following invoice IDs already exist: {', '.join(existing_invoice_ids)}"
                )
            
            # Validation Phase 4: Collect all unique vendor IDs
            unique_vendor_ids = set(transaction.vendor_id for transaction in transaction_data)
            
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
            
            # Validation Phase 6: Create all new transaction records
            new_transactions = []
            for transaction_data in transaction_data:
                try:
                    # Create new transaction instance (UUID will be auto-generated)
                    new_transaction = VendorTransactions(**transaction_data.model_dump(exclude_unset=True))
                    new_transactions.append(new_transaction)
                    
                except Exception as e:
                    logger.error(f"Error preparing transaction {transaction_data.invoice_id}: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing transaction {transaction_data.invoice_id}: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_transaction in new_transactions:
                db.add(new_transaction)
            
            await db.commit()
            
            # Refresh all transactions to get generated IDs
            for new_transaction in new_transactions:
                await db.refresh(new_transaction)
                created_transactions.append(
                    TransactionResponse.model_validate(new_transaction).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_transactions)} transaction(s)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_transactions)} transaction(s)",
                data=created_transactions
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(TransactionMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=TransactionMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id(transaction_id: UUID, db: AsyncSession):
        """Get a transaction by ID"""
        try:
            result = await db.execute(
                select(VendorTransactions).where(VendorTransactions.transaction_id == transaction_id)
            )
            transaction = result.scalar_one_or_none()
            
            if not transaction:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=TransactionMessages.NOT_FOUND.format(id=transaction_id)
                )
            
            logger.info(TransactionMessages.RETRIEVED_SUCCESS.format(invoice=transaction.invoice_id))
            return APIResponse(
                success=True,
                message=TransactionMessages.RETRIEVED_SUCCESS.format(invoice=transaction.invoice_id),
                data=TransactionResponse.model_validate(transaction).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(TransactionMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=TransactionMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all transactions with pagination"""
        try:
            result = await db.execute(
                select(VendorTransactions).offset(skip).limit(limit)
            )
            transactions = result.scalars().all()
            
            logger.info(TransactionMessages.RETRIEVED_ALL_SUCCESS.format(count=len(transactions)))
            return APIResponse(
                success=True,
                message=TransactionMessages.RETRIEVED_ALL_SUCCESS.format(count=len(transactions)),
                data=[TransactionResponse.model_validate(txn).model_dump() for txn in transactions]
            )

        except Exception as e:
            logger.error(TransactionMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=TransactionMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_vendor_id(vendor_id: UUID, db: AsyncSession):
        """Get all transactions by vendor ID"""
        try:
            result = await db.execute(
                select(VendorTransactions).where(VendorTransactions.vendor_id == vendor_id)
            )
            transactions = result.scalars().all()
            
            if not transactions:
                logger.info(TransactionMessages.NO_TRANSACTIONS_FOR_VENDOR.format(id=vendor_id))
                return []
            
            logger.info(TransactionMessages.RETRIEVED_BY_VENDOR_SUCCESS.format(count=len(transactions), id=vendor_id))
            return APIResponse(
                success=True,
                message=TransactionMessages.RETRIEVED_BY_VENDOR_SUCCESS.format(count=len(transactions), id=vendor_id),
                data=[TransactionResponse.model_validate(txn).model_dump() for txn in transactions]
            )


        except Exception as e:
            logger.error(TransactionMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=TransactionMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(transaction_id: UUID, transaction_data: TransactionUpdate, db: AsyncSession):
        """Update a transaction"""
        try:
            result = await db.execute(
                select(VendorTransactions).where(VendorTransactions.transaction_id == transaction_id)
            )
            transaction = result.scalar_one_or_none()
            
            if not transaction:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=TransactionMessages.NOT_FOUND.format(id=transaction_id)
                )

            # Update fields
            for key, value in transaction_data.model_dump(exclude_unset=True).items():
                setattr(transaction, key, value)
            
            transaction.updated_at = datetime.now(timezone.utc)

            await db.commit()
            await db.refresh(transaction)
            
            logger.info(TransactionMessages.UPDATED_SUCCESS.format(invoice=transaction.invoice_id))
            return APIResponse(
                success=True,
                message=TransactionMessages.UPDATED_SUCCESS.format(invoice=transaction.invoice_id),
                data=TransactionResponse.model_validate(transaction).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(TransactionMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=TransactionMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(transaction_id: UUID, db: AsyncSession):
        """Delete a transaction"""
        try:
            result = await db.execute(
                select(VendorTransactions).where(VendorTransactions.transaction_id == transaction_id)
            )
            transaction = result.scalar_one_or_none()
            
            if not transaction:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=TransactionMessages.NOT_FOUND.format(id=transaction_id)
                )

            await db.delete(transaction)
            await db.commit()
            
            logger.info(TransactionMessages.DELETED_SUCCESS.format(id=transaction_id))
            return APIResponse(
                success=True,
                message=TransactionMessages.DELETED_SUCCESS.format(id=transaction_id),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(TransactionMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=TransactionMessages.DELETE_ERROR.format(error=str(e))
            )