import logging
from datetime import datetime, timezone
from typing import List, Optional
from uuid import UUID

from beanie import PydanticObjectId
from client_service.api.constants.messages import ClientSchemaMessages
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from client_service.schemas.client_db.client_models import Clients
from client_service.schemas.mongo_schemas.client_schema_model import (
    ClientSchema,
    SchemaField,
)
from client_service.schemas.pydantic_schemas import (
    ClientSchemaCreate,
    ClientSchemaResponse,
    ClientSchemaUpdate,
    SchemaFieldCreate,
)
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class ClientSchemaService:
    """Service class for Client Schema business logic"""

    @staticmethod
    async def create(
        schema_data: List[ClientSchemaCreate], db: AsyncSession
    ):  # ← ADDED db parameter
        """
        Create a new client schema
        - Validates client_id exists in PostgreSQL
        - Auto-generates version if not provided
        - Deactivates other versions if is_active=True
        """
        created_schemas = []

        try:
            # Validation Phase 1: Check for empty list
            if not schema_data:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail="Schemas list cannot be empty. Provide at least one schema.",
                )

            logger.info(f"Processing creation of {len(schema_data)} client schema(s)")

            # Validation Phase 2: Validate all client_id formats
            for idx, schema_item in enumerate(schema_data):
                try:
                    UUID(schema_item.client_id)
                except ValueError:
                    logger.warning(
                        f"Invalid client_id format at position {idx}: {schema_item.client_id}"
                    )
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Invalid client_id format at position {idx}. Must be a valid UUID, got: {schema_item.client_id}",
                    )

            # Validation Phase 3: Collect all unique client IDs
            unique_client_ids = {UUID(schema.client_id) for schema in schema_data}

            # Validation Phase 4: Verify all clients exist in PostgreSQL
            # clients_result = await db.execute(
            #     select(Clients).where(Clients.client_id.in_(unique_client_ids))
            # )
            # existing_clients = clients_result.scalars().all()
            # existing_client_ids = {client.client_id for client in existing_clients}
            # missing_client_ids = unique_client_ids - existing_client_ids

            # if missing_client_ids:
            #     logger.warning(f"Client IDs not found: {missing_client_ids}")
            #     raise HTTPException(
            #         status_code=StatusCode.NOT_FOUND,
            #         detail=f"The following client IDs do not exist: {', '.join(str(id) for id in missing_client_ids)}"
            #     )

            # Validation Phase 5: Check for duplicate schema names within batch per client
            batch_schemas = {}  # {client_id: {schema_name: index}}
            for idx, schema_item in enumerate(schema_data):
                if schema_item.client_id not in batch_schemas:
                    batch_schemas[schema_item.client_id] = {}

                if schema_item.schema_name in batch_schemas[schema_item.client_id]:
                    logger.warning(
                        f"Duplicate schema name in batch for client {schema_item.client_id}: {schema_item.schema_name}"
                    )
                    raise HTTPException(
                        status_code=StatusCode.CONFLICT,
                        detail=f"Duplicate schema name in batch for client {schema_item.client_id}: {schema_item.schema_name}",
                    )

                batch_schemas[schema_item.client_id][schema_item.schema_name] = idx

            # Validation Phase 6: Process each schema
            new_schemas = []
            for schema_item in schema_data:
                try:
                    # Check if schema with same name exists for this client
                    existing_schemas = (
                        await ClientSchema.find(
                            ClientSchema.client_id == schema_item.client_id,
                            ClientSchema.schema_name == schema_item.schema_name,
                        )
                        .sort(-ClientSchema.version)
                        .to_list()
                    )

                    # Determine version number
                    if schema_item.version:
                        version = schema_item.version
                        # Check if this version already exists
                        version_exists = any(
                            s.version == version for s in existing_schemas
                        )
                        if version_exists:
                            logger.warning(
                                f"Schema version already exists: {schema_item.schema_name} v{version}"
                            )
                            raise HTTPException(
                                status_code=StatusCode.CONFLICT,
                                detail=ClientSchemaMessages.DUPLICATE_SCHEMA.format(
                                    name=schema_item.schema_name,
                                    version=version,
                                    client_id=schema_item.client_id,
                                ),
                            )
                    else:
                        # Auto-generate version (max + 1)
                        version = (
                            max([s.version for s in existing_schemas], default=0) + 1
                        )

                    # If this version should be active, deactivate all other versions
                    if schema_item.is_active:
                        for schema in existing_schemas:
                            if schema.is_active:
                                schema.is_active = False
                                schema.updated_at = datetime.now(timezone.utc)
                                await schema.save()

                    # Convert SchemaFieldCreate to dict (not SchemaField objects)
                    fields = [field.model_dump() for field in schema_item.fields]

                    # Create new schema document
                    new_schema = ClientSchema(
                        client_id=schema_item.client_id,
                        schema_name=schema_item.schema_name,
                        version=version,
                        is_active=schema_item.is_active,
                        description=schema_item.description,
                        fields=fields,
                        created_by=schema_item.created_by,
                        updated_by=schema_item.created_by,
                        created_at=datetime.now(timezone.utc),
                        updated_at=datetime.now(timezone.utc),
                    )

                    new_schemas.append(new_schema)

                except HTTPException:
                    raise
                except Exception as e:
                    logger.error(
                        f"Error preparing schema {schema_item.schema_name}: {str(e)}"
                    )
                    raise HTTPException(
                        status_code=StatusCode.BAD_REQUEST,
                        detail=f"Error preparing schema {schema_item.schema_name}: {str(e)}",
                    )

            # Commit Phase: Insert all schemas
            for new_schema in new_schemas:
                await new_schema.insert()

            # Build response with created schemas
            for new_schema in new_schemas:
                created_schemas.append(
                    ClientSchemaResponse(
                        _id=str(new_schema.id),
                        client_id=new_schema.client_id,
                        schema_name=new_schema.schema_name,
                        version=new_schema.version,
                        is_active=new_schema.is_active,
                        description=new_schema.description,
                        fields=new_schema.fields,
                        created_by=new_schema.created_by,
                        updated_by=new_schema.updated_by,
                        created_at=new_schema.created_at,
                        updated_at=new_schema.updated_at,
                    ).model_dump(by_alias=True)
                )

            # Success logging and response
            logger.info(f"Successfully created {len(created_schemas)} client schema(s)")

            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_schemas)} client schema(s)",
                data=created_schemas,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating client schemas: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Error creating client schemas: {str(e)}",
            )

    @staticmethod
    async def get_by_id(schema_id: str):
        """Get a client schema by MongoDB ObjectId"""
        try:
            schema = await ClientSchema.get(PydanticObjectId(schema_id))

            if not schema:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ClientSchemaMessages.NOT_FOUND.format(id=schema_id),
                )

            logger.info(
                ClientSchemaMessages.RETRIEVED_SUCCESS.format(
                    name=schema.schema_name, version=schema.version
                )
            )

            return APIResponse(
                success=True,
                message=ClientSchemaMessages.RETRIEVED_SUCCESS.format(
                    name=schema.schema_name, version=schema.version
                ),
                data=ClientSchemaResponse(
                    _id=str(schema.id),
                    client_id=schema.client_id,
                    schema_name=schema.schema_name,
                    version=schema.version,
                    is_active=schema.is_active,
                    description=schema.description,
                    fields=schema.fields,
                    created_by=schema.created_by,
                    updated_by=schema.updated_by,
                    created_at=schema.created_at,
                    updated_at=schema.updated_at,
                ).model_dump(by_alias=True),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ClientSchemaMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientSchemaMessages.RETRIEVE_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def get_all(skip: int = 0, limit: int = 100):
        """Get all client schemas with pagination"""
        try:
            schemas = await ClientSchema.find_all().skip(skip).limit(limit).to_list()

            logger.info(
                ClientSchemaMessages.RETRIEVED_ALL_SUCCESS.format(count=len(schemas))
            )

            return APIResponse(
                success=True,
                message=ClientSchemaMessages.RETRIEVED_ALL_SUCCESS.format(
                    count=len(schemas)
                ),
                data=[
                    ClientSchemaResponse(
                        _id=str(schema.id),
                        client_id=schema.client_id,
                        schema_name=schema.schema_name,
                        version=schema.version,
                        is_active=schema.is_active,
                        description=schema.description,
                        fields=schema.fields,
                        created_at=schema.created_at,
                        updated_at=schema.updated_at,
                    ).model_dump(by_alias=True)
                    for schema in schemas
                ],
            )

        except Exception as e:
            logger.error(ClientSchemaMessages.RETRIEVE_ALL_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientSchemaMessages.RETRIEVE_ALL_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def get_by_client_id(client_id: str):
        """Get all schemas for a specific client"""
        try:
            schemas = await ClientSchema.find(
                ClientSchema.client_id == client_id
            ).to_list()

            if not schemas:
                logger.info(
                    ClientSchemaMessages.NO_SCHEMAS_FOR_CLIENT.format(id=client_id)
                )
                return APIResponse(
                    success=True,
                    message=ClientSchemaMessages.NO_SCHEMAS_FOR_CLIENT.format(
                        id=client_id
                    ),
                    data=[],
                )

            logger.info(
                ClientSchemaMessages.RETRIEVED_BY_CLIENT_SUCCESS.format(
                    count=len(schemas), id=client_id
                )
            )

            return APIResponse(
                success=True,
                message=ClientSchemaMessages.RETRIEVED_BY_CLIENT_SUCCESS.format(
                    count=len(schemas), id=client_id
                ),
                data=[
                    ClientSchemaResponse(
                        _id=str(schema.id),
                        client_id=schema.client_id,
                        schema_name=schema.schema_name,
                        version=schema.version,
                        is_active=schema.is_active,
                        description=schema.description,
                        fields=schema.fields,
                        created_at=schema.created_at,
                        updated_at=schema.updated_at,
                    ).model_dump(by_alias=True)
                    for schema in schemas
                ],
            )

        except Exception as e:
            logger.error(ClientSchemaMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientSchemaMessages.RETRIEVE_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def get_by_client_and_name(client_id: str, schema_name: str):
        """Get all versions of a specific schema for a client"""
        try:
            schemas = (
                await ClientSchema.find(
                    ClientSchema.client_id == client_id,
                    ClientSchema.schema_name == schema_name,
                )
                .sort(-ClientSchema.version)
                .to_list()
            )

            if not schemas:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ClientSchemaMessages.NOT_FOUND_BY_NAME.format(
                        name=schema_name, client_id=client_id
                    ),
                )

            logger.info(
                ClientSchemaMessages.RETRIEVED_BY_CLIENT_SUCCESS.format(
                    count=len(schemas), id=client_id
                )
            )

            return APIResponse(
                success=True,
                message=ClientSchemaMessages.RETRIEVED_BY_CLIENT_SUCCESS.format(
                    count=len(schemas), id=client_id
                ),
                data=[
                    ClientSchemaResponse(
                        _id=str(schema.id),
                        client_id=schema.client_id,
                        schema_name=schema.schema_name,
                        version=schema.version,
                        is_active=schema.is_active,
                        description=schema.description,
                        fields=schema.fields,
                        created_at=schema.created_at,
                        updated_at=schema.updated_at,
                    ).model_dump(by_alias=True)
                    for schema in schemas
                ],
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ClientSchemaMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientSchemaMessages.RETRIEVE_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def get_active_schema(client_id: str, schema_name: str):
        """Get the active version of a schema"""
        try:
            schema = await ClientSchema.find_one(
                ClientSchema.client_id == client_id,
                ClientSchema.schema_name == schema_name,
                ClientSchema.is_active == True,
            )

            if not schema:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ClientSchemaMessages.NO_ACTIVE_VERSION.format(
                        name=schema_name, client_id=client_id
                    ),
                )

            logger.info(
                ClientSchemaMessages.RETRIEVED_ACTIVE_SUCCESS.format(
                    name=schema.schema_name, version=schema.version
                )
            )

            return APIResponse(
                success=True,
                message=ClientSchemaMessages.RETRIEVED_ACTIVE_SUCCESS.format(
                    name=schema.schema_name, version=schema.version
                ),
                data=ClientSchemaResponse(
                    _id=str(schema.id),
                    client_id=schema.client_id,
                    schema_name=schema.schema_name,
                    version=schema.version,
                    is_active=schema.is_active,
                    description=schema.description,
                    fields=schema.fields,
                    created_by=schema.created_by,
                    updated_by=schema.updated_by,
                    created_at=schema.created_at,
                    updated_at=schema.updated_at,
                ).model_dump(by_alias=True),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ClientSchemaMessages.RETRIEVE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientSchemaMessages.RETRIEVE_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def update(schema_id: str, schema_data: ClientSchemaUpdate):
        """
        Update a client schema
        Updates the existing document in place
        """
        try:
            schema = await ClientSchema.get(PydanticObjectId(schema_id))

            if not schema:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ClientSchemaMessages.NOT_FOUND.format(id=schema_id),
                )

            # Update fields if provided
            if schema_data.description is not None:
                schema.description = schema_data.description

            if schema_data.fields is not None:
                schema.fields = [field.model_dump() for field in schema_data.fields]

            # Handle is_active flag
            if (
                schema_data.is_active is not None
                and schema_data.is_active != schema.is_active
            ):
                if schema_data.is_active:
                    # Deactivate all other versions
                    other_schemas = await ClientSchema.find(
                        ClientSchema.client_id == schema.client_id,
                        ClientSchema.schema_name == schema.schema_name,
                        ClientSchema.id != schema.id,
                    ).to_list()

                    for other in other_schemas:
                        if other.is_active:
                            other.is_active = False
                            other.updated_at = datetime.now(timezone.utc)
                            await other.save()

                schema.is_active = schema_data.is_active
            if schema_data.updated_by:
                schema.updated_by = schema_data.updated_by
            schema.updated_at = datetime.now(timezone.utc)
            await schema.save()

            logger.info(
                ClientSchemaMessages.UPDATED_SUCCESS.format(
                    name=schema.schema_name, version=schema.version
                )
            )

            return APIResponse(
                success=True,
                message=ClientSchemaMessages.UPDATED_SUCCESS.format(
                    name=schema.schema_name, version=schema.version
                ),
                data=ClientSchemaResponse(
                    _id=str(schema.id),
                    client_id=schema.client_id,
                    schema_name=schema.schema_name,
                    version=schema.version,
                    is_active=schema.is_active,
                    description=schema.description,
                    fields=schema.fields,
                    created_by=schema.created_by,
                    updated_by=schema.updated_by,
                    created_at=schema.created_at,
                    updated_at=schema.updated_at,
                ).model_dump(by_alias=True),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ClientSchemaMessages.UPDATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientSchemaMessages.UPDATE_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def activate_version(schema_id: str):
        """
        Activate a specific version of a schema
        Deactivates all other versions of the same schema
        """
        try:
            schema = await ClientSchema.get(PydanticObjectId(schema_id))

            if not schema:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ClientSchemaMessages.NOT_FOUND.format(id=schema_id),
                )

            # Deactivate all other versions
            other_schemas = await ClientSchema.find(
                ClientSchema.client_id == schema.client_id,
                ClientSchema.schema_name == schema.schema_name,
                ClientSchema.id != schema.id,
            ).to_list()

            for other in other_schemas:
                if other.is_active:
                    other.is_active = False
                    other.updated_at = datetime.now(timezone.utc)
                    await other.save()

            # Activate this version
            schema.is_active = True
            schema.updated_at = datetime.now(timezone.utc)
            await schema.save()

            logger.info(
                ClientSchemaMessages.ACTIVATED_SUCCESS.format(
                    name=schema.schema_name, version=schema.version
                )
            )

            return APIResponse(
                success=True,
                message=ClientSchemaMessages.ACTIVATED_SUCCESS.format(
                    name=schema.schema_name, version=schema.version
                ),
                data=ClientSchemaResponse(
                    _id=str(schema.id),
                    client_id=schema.client_id,
                    schema_name=schema.schema_name,
                    version=schema.version,
                    is_active=schema.is_active,
                    description=schema.description,
                    fields=schema.fields,
                    created_by=schema.created_by,
                    updated_by=schema.updated_by,
                    created_at=schema.created_at,
                    updated_at=schema.updated_at,
                ).model_dump(by_alias=True),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ClientSchemaMessages.ACTIVATE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientSchemaMessages.ACTIVATE_ERROR.format(error=str(e)),
            )

    @staticmethod
    async def delete(schema_id: str):
        """Delete a client schema"""
        try:
            schema = await ClientSchema.get(PydanticObjectId(schema_id))

            if not schema:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=ClientSchemaMessages.NOT_FOUND.format(id=schema_id),
                )

            await schema.delete()

            logger.info(ClientSchemaMessages.DELETED_SUCCESS.format(id=schema_id))

            return APIResponse(
                success=True,
                message=ClientSchemaMessages.DELETED_SUCCESS.format(id=schema_id),
                data=None,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(ClientSchemaMessages.DELETE_ERROR.format(error=str(e)))
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=ClientSchemaMessages.DELETE_ERROR.format(error=str(e)),
            )
