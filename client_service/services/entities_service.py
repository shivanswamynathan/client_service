from fastapi import HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from client_service.schemas.client_db.client_models import ClientEntity, Clients
from client_service.schemas.pydantic_schemas import ClientEntityCreate, ClientEntityUpdate, ClientEntityResponse
from client_service.api.constants.messages import EntityMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from datetime import datetime, timezone
from typing import List
from rapidfuzz import fuzz
import logging
from uuid import UUID

logger = logging.getLogger(__name__)


class EntityService:
    """Service class for Entity business logic"""
    
    @staticmethod
    async def create(entity_data: List[ClientEntityCreate], db: AsyncSession):
        """Create a new entity"""
        created_entities = []
        
        try:
            # Validation Phase 1: Check for empty list
            if not entity_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Entities list cannot be empty. Provide at least one entity."
                )
            
            logger.info(f"Processing creation of {len(entity_data)} entity(ies)")
            
            # Validation Phase 2: Verify all clients exist (check each unique client_id)
            unique_client_ids = set(entity.client_id for entity in entity_data)
            
            result = await db.execute(
                select(Clients).where(Clients.client_id.in_(unique_client_ids))
            )
            existing_clients = result.scalars().all()
            existing_client_ids = {client.client_id for client in existing_clients}
            
            missing_client_ids = unique_client_ids - existing_client_ids
            if missing_client_ids:
                logger.warning(f"Client IDs not found: {missing_client_ids}")
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"The following client IDs do not exist: {', '.join(str(id) for id in missing_client_ids)}"
                )
            
            # Validation Phase 3: Check for duplicate entity names within batch for same client
            batch_entities = {}  # {client_id: {entity_name: index}}
            for idx, entity in enumerate(entity_data):
                if entity.client_id not in batch_entities:
                    batch_entities[entity.client_id] = {}
                
                if entity.entity_name in batch_entities[entity.client_id]:
                    logger.warning(f"Duplicate entity name in batch for client {entity.client_id}: {entity.entity_name}")
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate entity name '{entity.entity_name}' for client {entity.client_id} (also at position {batch_entities[entity.client_id][entity.entity_name]})"
                    )
                
                batch_entities[entity.client_id][entity.entity_name] = idx
            
            # Validation Phase 4: Create all new entity records
            new_entities = []
            for entity_data in entity_data:
                try:
                    # Create new entity instance (UUID will be auto-generated)
                    new_entity = ClientEntity(**entity_data.model_dump(exclude_unset=True))
                    new_entities.append(new_entity)
                    
                except Exception as e:
                    logger.error(f"Error preparing entity {entity_data.entity_name}: {str(e)}")
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing entity {entity_data.entity_name}: {str(e)}"
                    )
            
            # Commit Phase: Add all to session and commit atomically
            for new_entity in new_entities:
                db.add(new_entity)
            
            await db.commit()
            
            # Refresh all entities to get generated IDs
            for new_entity in new_entities:
                await db.refresh(new_entity)
                created_entities.append(
                    ClientEntityResponse.model_validate(new_entity).model_dump()
                )
            
            # Success logging and response
            logger.info(f"Successfully created {len(created_entities)} entity(ies)")
            
            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_entities)} entity(ies)",
                data=created_entities
            )

        except HTTPException:
            await db.rollback()
            raise
        except Exception as e:
            await db.rollback()
            logger.error(EntityMessages.CREATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=EntityMessages.CREATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_id(entity_id: UUID, db: AsyncSession):
        """Get an entity by ID"""
        try:
            result = await db.execute(
                select(ClientEntity).where(ClientEntity.entity_id == entity_id)
            )
            entity = result.scalar_one_or_none()
            
            if not entity:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=EntityMessages.NOT_FOUND.format(id=entity_id)
                )
            
            logger.info(EntityMessages.RETRIEVED_SUCCESS.format(name=entity.entity_name))
            return APIResponse(
                success=True,   
                message=EntityMessages.RETRIEVED_SUCCESS.format(name=entity.entity_name),
                data=ClientEntityResponse.model_validate(entity).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(EntityMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=EntityMessages.RETRIEVE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_all(skip: int, limit: int, db: AsyncSession):
        """Get all entities with pagination"""
        try:
            result = await db.execute(
                select(ClientEntity).offset(skip).limit(limit)
            )
            entities = result.scalars().all()
            
            logger.info(EntityMessages.RETRIEVED_ALL_SUCCESS.format(count=len(entities)))
            return APIResponse(
                success=True,
                message=EntityMessages.RETRIEVED_ALL_SUCCESS.format(count=len(entities)),
                data=[ClientEntityResponse.model_validate(entity).model_dump() for entity in entities]
            )


        except Exception as e:
            logger.error(EntityMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=EntityMessages.RETRIEVE_ALL_ERROR.format(error=str(e))
            )

    @staticmethod
    async def get_by_client_id(client_id: UUID, db: AsyncSession):
        """Get all entities by client ID"""
        try:
            result = await db.execute(
                select(ClientEntity).where(ClientEntity.client_id == client_id)
            )
            entities = result.scalars().all()
            
            if not entities:
                logger.info(EntityMessages.NO_ENTITIES_FOR_CLIENT.format(id=client_id))
                return []
            
            logger.info(EntityMessages.RETRIEVED_BY_CLIENT_SUCCESS.format(count=len(entities), id=client_id))
            return APIResponse(
                success=True,   
                message=EntityMessages.RETRIEVED_BY_CLIENT_SUCCESS.format(count=len(entities), id=client_id),
                data=[ClientEntityResponse.model_validate(entity).model_dump() for entity in entities]
            )

        except Exception as e:
            logger.error(EntityMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=EntityMessages.RETRIEVE_ERROR.format(error=str(e))
            )
    
    @staticmethod
    async def search(column: str, value: str, db: AsyncSession, threshold: int = 70):
        """Search entities by specific column and value"""
        try:
            # Dynamically get all column names from the model (excluding timestamps and IDs for search)
            excluded_columns = {'created_at', 'updated_at', 'entity_id', 'client_id', 'parent_client_id'}  # Columns we don't want to search
            
            # Get all columns from the ClientEntity model
            all_columns = {col.name: col for col in ClientEntity.__table__.columns}
            
            # Filter out excluded columns
            allowed_columns = {
                name: all_columns[name]
                for name in all_columns
                if name not in excluded_columns
            }
            
            # Validate column name
            if column not in allowed_columns:
                logger.warning(EntityMessages.INVALID_SEARCH_COLUMN.format(
                    column=column, 
                    allowed=', '.join(allowed_columns.keys())
                ))
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail=EntityMessages.INVALID_SEARCH_COLUMN.format(
                        column=column,
                        allowed=', '.join(allowed_columns.keys())
                    )
                )
            
            # Create search pattern for partial matching
            value = value.strip()
            

            # Get ALL entities from database
            result = await db.execute(select(ClientEntity))
            all_entities = result.scalars().all()
            
            best_match = None
            best_score = 0
            for entity in all_entities:
                # Get the value of the column we're searching
                entity_value = getattr(entity, column)
                
                if entity_value:
                    # Calculate similarity score
                    score = fuzz.partial_ratio(value.lower(), str(entity_value).lower())
                    
                    if score >= threshold and score > best_score:
                        best_match = entity
                        best_score = score
                        print(f"New best match: '{entity_value}' (score: {score})")
            
            if not best_match:
                logger.info(EntityMessages.NO_SEARCH_RESULTS.format(column=column, value=value))
                return APIResponse(
                    success=True,
                    message=EntityMessages.NO_SEARCH_RESULTS.format(column=column, value=value),
                    data=[]
                )
            
            print(f"Final best match score: {best_score}")
            logger.info(f"Found best match for {column}='{value}' with score {best_score}")
            return APIResponse(
                success=True,
                message=f"Found best match where {column} matches '{value}' (score: {best_score})",
                data=[ClientEntityResponse.model_validate(best_match).model_dump()]
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(EntityMessages.SEARCH_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=EntityMessages.SEARCH_ERROR.format(error=str(e))
            )

    @staticmethod
    async def update(entity_id: UUID, entity_data: ClientEntityUpdate, db: AsyncSession):
        """Update an entity"""
        try:
            result = await db.execute(
                select(ClientEntity).where(ClientEntity.entity_id == entity_id)
            )
            entity = result.scalar_one_or_none()
            
            if not entity:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=EntityMessages.NOT_FOUND.format(id=entity_id)
                )

            # Update fields
            for key, value in entity_data.model_dump(exclude_unset=True).items():
                setattr(entity, key, value)
            
            entity.updated_at = datetime.now(timezone.utc)

            await db.commit()
            await db.refresh(entity)
            
            logger.info(EntityMessages.UPDATED_SUCCESS.format(name=entity.entity_name))
            return APIResponse(
                success=True,
                message=EntityMessages.UPDATED_SUCCESS.format(name=entity.entity_name),
                data=ClientEntityResponse.model_validate(entity).model_dump()
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(EntityMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=EntityMessages.UPDATE_ERROR.format(error=str(e))
            )

    @staticmethod
    async def delete(entity_id: UUID, db: AsyncSession):
        """Delete an entity"""
        try:
            result = await db.execute(
                select(ClientEntity).where(ClientEntity.entity_id == entity_id)
            )
            entity = result.scalar_one_or_none()
            
            if not entity:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=EntityMessages.NOT_FOUND.format(id=entity_id)
                )

            await db.delete(entity)
            await db.commit()
            
            logger.info(EntityMessages.DELETED_SUCCESS.format(id=entity_id))
            return APIResponse(
                success=True,
                message=EntityMessages.DELETED_SUCCESS.format(id=entity_id),
                data=None
            )

        except HTTPException:
            raise
        except Exception as e:
            await db.rollback()
            logger.error(EntityMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=EntityMessages.DELETE_ERROR.format(error=str(e))
            )