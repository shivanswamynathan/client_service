from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.client_models import Clients
from client_service.schemas.pydantic_schemas import ClientCreate, ClientUpdate
from client_service.api.constants.messages import ClientMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from client_service.schemas.pydantic_schemas import ClientResponse  
from datetime import datetime, timezone
import logging
from uuid import UUID
from typing import List


logger = logging.getLogger(__name__)


class ClientService:
    """Service class for Client business logic"""
    
    @staticmethod
    async def create(client_data: List[ClientCreate], db: AsyncSession):
        """Create a new client"""

        created_clients = []
        failed_clients = []
        try:
            # Validation Phase 1: Check for empty list
            if not client_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Clients list cannot be empty. Provide at least one client."
                )
            
            # Validation Phase 2: Collect all client names for batch duplicate check
            batch_names = {}  # {name: index}
            for idx, client in enumerate(client_data):
                if client.client_name in batch_names:
                    failed_clients.append({
                        "client_name": client.client_name,
                        "error": f"Duplicate name in batch (also at position {batch_names[client.client_name]})"
                    })
                else:
                    batch_names[client.client_name] = idx
            
            # If duplicates found in batch, return error before any DB operation
            if failed_clients:
                logger.warning(f"Duplicate names found in batch: {failed_clients}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"Duplicate client names detected in batch. See data for details.",
                )
            
            # Validation Phase 3: Check for duplicates in existing database
            existing_result = await db.execute(
                select(Clients).where(Clients.client_name.in_(batch_names.keys()))
            )
            existing_clients = existing_result.scalars().all()
            
            if existing_clients:
                existing_names = [c.client_name for c in existing_clients]
                logger.warning(f"Clients already exist in DB: {existing_names}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following client names already exist: {', '.join(existing_names)}"
                )
            
            # Validation Phase 4: Create all new client records
            new_clients = []
            for client_data in client_data:
                try:
                    # Create new client instance (UUID will be auto-generated)
                    new_client = Clients(**client_data.model_dump(exclude_unset=True))
                    new_clients.append(new_client)
                    
                except Exception as e:
                    logger.error(f"Error preparing client {client_data.client_name}: {str(e)}")
                    failed_clients.append({
                        "client_name": client_data.client_name,
                        "error": f"Error preparing client: {str(e)}"
                    })
            
            # If any preparation failed, abort before committing
            if failed_clients:
                logger.warning(f"Preparation failed for some clients: {failed_clients}")
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Failed to prepare some clients. See data for details."
                )
            
            # Commit Phase: Add all to session and commit atomically
            for new_client in new_clients:
                db.add(new_client)
            
            await db.commit()
            
            # Refresh all clients to get generated IDs
            for new_client in new_clients:
                await db.refresh(new_client)
                created_clients.append(ClientResponse.model_validate(new_client).model_dump())
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_clients)} clients")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_clients)} client(s)",
                data=created_clients
            )

        except HTTPException:
            # Re-raise HTTP exceptions (validation errors)
            await db.rollback()
            raise

    @staticmethod
    async def get_by_id(client_id: UUID, db: AsyncSession):
        """Get a client by ID"""
        try:
            result = await db.execute(
                select(Clients).where(Clients.client_id == client_id)
            )
            client = result.scalar_one_or_none()
            
            if not client:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ClientMessages.NOT_FOUND.format(id=client_id)
                )
            
            logger.info(ClientMessages.RETRIEVED_SUCCESS.format(name=client.client_name))
            return APIResponse(
                success=True,
                message=ClientMessages.RETRIEVED_SUCCESS.format(name=client.client_name),
                data=ClientResponse.model_validate(client).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ClientMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all clients with pagination"""
        try:
            result = await db.execute(
                select(Clients).offset(skip).limit(limit)
            )
            clients = result.scalars().all()
            
            logger.info(ClientMessages.RETRIEVED_ALL_SUCCESS.format(count=len(clients)))
            return APIResponse(
                success=True,
                message=ClientMessages.RETRIEVED_ALL_SUCCESS.format(count=len(clients)),
                data=[ClientResponse.model_validate(client).model_dump() for client in clients]
            )

        except Exception as e:
            logger.error(ClientMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(client_id: UUID, client_data: ClientUpdate, db: AsyncSession):
        """Update a client"""
        try:
            result = await db.execute(
                select(Clients).where(Clients.client_id == client_id)
            )
            client = result.scalar_one_or_none()
            
            if not client:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ClientMessages.NOT_FOUND.format(id=client_id)
                )

            # Update fields
            for key, value in client_data.model_dump(exclude_unset=True).items():
                setattr(client, key, value)
            
            client.updated_at = datetime.now(timezone.utc)

            await db.commit()
            await db.refresh(client)
            
            logger.info(ClientMessages.UPDATED_SUCCESS.format(name=client.client_name))
            return APIResponse(
                success=True,
                message=ClientMessages.UPDATED_SUCCESS.format(name=client.client_name),
                data=ClientResponse.model_validate(client).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(ClientMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(client_id: UUID, db: AsyncSession):
        """Delete a client"""
        try:
            result = await db.execute(
                select(Clients).where(Clients.client_id == client_id)
            )
            client = result.scalar_one_or_none()
            
            if not client:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ClientMessages.NOT_FOUND.format(id=client_id)
                )

            await db.delete(client)
            await db.commit()
            
            logger.info(ClientMessages.DELETED_SUCCESS.format(id=client_id))
            return APIResponse(
                success=True,
                message=ClientMessages.DELETED_SUCCESS.format(id=client_id),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(ClientMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientMessages.DELETE_ERROR.format(error=str(e))
            )