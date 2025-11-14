import logging
from typing import List
from uuid import UUID

from client_service.api.constants.messages import CentralClientMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from client_service.schemas.client_db.client_models import CentralClients
from client_service.schemas.pydantic_schemas import (
    CentralClientCreate,
    CentralClientResponse,
    CentralClientUpdate,
)
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class CentralClientService:
    """Service class for Central Client business logic"""

    @staticmethod
    async def create(central_client_data: List[CentralClientCreate], db: AsyncSession):
        """Create a new central client"""

        created_central_clients = []
        try:
            # Validation Phase 1: Check for empty list
            if not central_client_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Central clients list cannot be empty. Provide at least one central client.",
                )

            logger.info(
                f"Processing creation of {len(central_client_data)} central client(s)"
            )

            # Validation Phase 2: Collect all central client names for batch duplicate check
            batch_names = {}  # {name: index}
            for idx, central_client in enumerate(central_client_data):
                if central_client.name in batch_names:
                    logger.warning(f"Duplicate name in batch: {central_client.name}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate central client name in batch: {central_client.name} (also at position {batch_names[central_client.name]})",
                    )
                else:
                    batch_names[central_client.name] = idx

            # Validation Phase 3: Check for duplicates in existing database
            existing_result = await db.execute(
                select(CentralClients).where(
                    CentralClients.name.in_(batch_names.keys())
                )
            )
            existing_central_clients = existing_result.scalars().all()

            if existing_central_clients:
                existing_names = [c.name for c in existing_central_clients]
                logger.warning(f"Central clients already exist in DB: {existing_names}")
                raise HTTPException(
                    status_code=StatusCode.CONFLICT,
                    detail=f"The following central client names already exist: {', '.join(existing_names)}",
                )

            # Validation Phase 4: Create all new central client records
            new_central_clients = []
            for central_client_data in central_client_data:
                try:
                    # Create new central client instance (UUID will be auto-generated)
                    new_central_client = CentralClients(
                        **central_client_data.model_dump(exclude_unset=True)
                    )
                    new_central_clients.append(new_central_client)

                except Exception as e:
                    logger.error(
                        f"Error preparing central client {central_client_data.name}: {str(e)}"
                    )
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing central client {central_client_data.name}: {str(e)}",
                    )

            # Commit Phase: Add all to session and commit atomically
            for new_central_client in new_central_clients:
                db.add(new_central_client)

            await db.commit()

            # Refresh all central clients to get generated IDs
            for new_central_client in new_central_clients:
                await db.refresh(new_central_client)
                created_central_clients.append(
                    CentralClientResponse.model_validate(
                        new_central_client
                    ).model_dump()
                )

            # Success logging and response
            logger.info(
                f"Successfully created {len(created_central_clients)} central client(s)"
            )

            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_central_clients)} central client(s)",
                data=created_central_clients,
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(CentralClientMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=CentralClientMessages.CREATE_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def get_by_id(client_id: UUID, db: AsyncSession):
        """Get a central client by ID"""
        try:
            result = await db.execute(
                select(CentralClients).where(
                    CentralClients.central_client_id == client_id
                )
            )
            central_client = result.scalar_one_or_none()

            if not central_client:
                return APIResponse(
                    success=True,
                    message=CentralClientMessages.RETRIEVED_SUCCESS.format(
                        name=client_id
                    ),
                    data=[],
                )
            # raise HTTPException(
            #     status_code=StatusCode.NOT_FOUND,
            #     detail=CentralClientMessages.NOT_FOUND.format(id=client_id)
            # )

            logger.info(
                CentralClientMessages.RETRIEVED_SUCCESS.format(name=central_client.name)
            )
            return APIResponse(
                success=True,
                message=CentralClientMessages.RETRIEVED_SUCCESS.format(
                    name=central_client.name
                ),
                data=CentralClientResponse.model_validate(central_client).model_dump(),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(CentralClientMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=CentralClientMessages.RETRIEVE_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all central clients with pagination"""
        try:
            result = await db.execute(select(CentralClients).offset(skip).limit(limit))
            central_clients = result.scalars().all()

            logger.info(
                CentralClientMessages.RETRIEVED_ALL_SUCCESS.format(
                    count=len(central_clients)
                )
            )
            return APIResponse(
                success=True,
                message=CentralClientMessages.RETRIEVED_ALL_SUCCESS.format(
                    count=len(central_clients)
                ),
                data=[
                    CentralClientResponse.model_validate(client).model_dump()
                    for client in central_clients
                ],
            )

        except Exception as e:
            logger.error(CentralClientMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=CentralClientMessages.RETRIEVE_ALL_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def update(
        client_id: UUID, central_client_data: CentralClientUpdate, db: AsyncSession
    ):
        """Update a central client"""
        try:
            result = await db.execute(
                select(CentralClients).where(
                    CentralClients.central_client_id == client_id
                )
            )
            central_client = result.scalar_one_or_none()

            if not central_client:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=CentralClientMessages.NOT_FOUND.format(id=client_id),
                )

            # Update fields
            for key, value in central_client_data.model_dump(
                exclude_unset=True
            ).items():
                setattr(central_client, key, value)

            await db.commit()
            await db.refresh(central_client)

            logger.info(
                CentralClientMessages.UPDATED_SUCCESS.format(name=central_client.name)
            )
            return APIResponse(
                success=True,
                message=CentralClientMessages.UPDATED_SUCCESS.format(
                    name=central_client.name
                ),
                data=CentralClientResponse.model_validate(central_client).model_dump(),
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(CentralClientMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=CentralClientMessages.UPDATE_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def delete(client_id: UUID, db: AsyncSession):
        """Delete a central client"""
        try:
            result = await db.execute(
                select(CentralClients).where(
                    CentralClients.central_client_id == client_id
                )
            )
            central_client = result.scalar_one_or_none()

            if not central_client:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=CentralClientMessages.NOT_FOUND.format(id=client_id),
                )

            await db.delete(central_client)
            await db.commit()

            logger.info(CentralClientMessages.DELETED_SUCCESS.format(id=client_id))
            return APIResponse(
                success=True,
                message=CentralClientMessages.DELETED_SUCCESS.format(id=client_id),
                data=None,
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(CentralClientMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=CentralClientMessages.DELETE_ERROR.format(error=str(e)),
            )
