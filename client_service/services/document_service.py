import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from beanie import PydanticObjectId
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from client_service.schemas.client_db.client_models import Clients
from client_service.schemas.client_db.vendor_models import VendorMaster
from client_service.schemas.mongo_schemas.client_schema_model import ClientSchema
from rapidfuzz import fuzz
from client_service.schemas.mongo_schemas.dynamic_document_model import (
    BaseDynamicDocument,
    get_or_create_model,
)
from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)


class DocumentService:
    """Service for managing dynamic documents based on client schemas"""

    @staticmethod
    async def _validate_client(client_id: str, db: AsyncSession) -> bool:
        """Validate that client exists in PostgreSQL"""
        try:
            UUID(client_id)
        except ValueError:
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Invalid client_id format: {client_id}",
            )

        result = await db.execute(select(Clients).where(Clients.client_id == client_id))
        client = result.scalar_one_or_none()

        if not client:
            raise HTTPException(
                status_code=StatusCode.NOT_FOUND,
                detail=f"Client with ID {client_id} not found",
            )
        return True

    @staticmethod
    async def _validate_vendor(vendor_id: str, db: AsyncSession) -> bool:
        """Validate that vendor exists in PostgreSQL"""
        try:
            UUID(vendor_id)
        except ValueError:
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Invalid vendor_id format: {vendor_id}",
            )

        result = await db.execute(
            select(VendorMaster).where(VendorMaster.vendor_id == vendor_id)
        )
        vendor = result.scalar_one_or_none()

        if not vendor:
            raise HTTPException(
                status_code=StatusCode.NOT_FOUND,
                detail=f"Vendor with ID {vendor_id} not found",
            )
        return True

    @staticmethod
    async def _get_active_schema(client_id: str, schema_name: str) -> ClientSchema:
        """Get active schema for validation"""
        schema = await ClientSchema.find_one(
            # ClientSchema.client_id == client_id,
            ClientSchema.schema_name == schema_name,
            ClientSchema.is_active == True,
        )

        if not schema:
            raise HTTPException(
                status_code=StatusCode.NOT_FOUND,
                detail=f"No active schema found for '{schema_name}' and client {client_id}",
            )

        return schema

    @staticmethod
    async def _validate_document_data(
        data: Dict[str, Any], schema: ClientSchema
    ) -> None:
        """
        Validate document data against schema rules.

        Checks:
        - Required fields are present
        - Data types match
        - Allowed values (enums) are respected
        - Unique constraints (will be checked at DB level)
        """
        errors = []

        for field_def in schema.fields:
            field_name = field_def.name
            field_type = field_def.type
            required = field_def.required
            allowed_values = field_def.allowed_values

            # Check required fields
            if required and field_name not in data:
                errors.append(f"Required field '{field_name}' is missing")
                continue

            # Skip validation if field not provided and not required
            if field_name not in data:
                continue

            value = data[field_name]

            # Check data type
            type_checks = {
                "string": (str, "string"),
                "number": ((int, float), "number"),
                "boolean": (bool, "boolean"),
                "array": (list, "array"),
                "object": (dict, "object"),
                "date": (
                    str,
                    "date string (ISO format)",
                ),  # Dates come as strings in JSON
            }

            if field_type in type_checks:
                expected_type, type_name = type_checks[field_type]
                if not isinstance(value, expected_type):
                    errors.append(
                        f"Field '{field_name}' must be {type_name}, got {type(value).__name__}"
                    )

            # Check allowed values (enum)
            if allowed_values and value not in allowed_values:
                errors.append(
                    f"Field '{field_name}' must be one of {allowed_values}, got '{value}'"
                )

        if errors:
            raise HTTPException(
                status_code=StatusCode.UNPROCESSABLE_ENTITY,
                detail=f"Validation errors: {'; '.join(errors)}",
            )

    @staticmethod
    async def create(
        client_id: str,
        collection_name: str,
        documents: List[Dict[str, Any]],
        db: AsyncSession,
        created_by: Optional[str] = None,
    ) -> APIResponse:
        """
        Create a new document in a dynamic collection.
        """
        try:
            # Validate client exists
            # await DocumentService._validate_client(client_id, db)

            # Get active schema
            schema = await DocumentService._get_active_schema(
                client_id, collection_name
            )

            # Validate document data against schema
            for idx, data in enumerate(documents):
                await DocumentService._validate_document_data(data, schema)

            # Convert SchemaField objects to dicts for the model factory
            fields_as_dicts = [
                {
                    "name": field.name,
                    "type": field.type,
                    "required": field.required,
                    "unique": field.unique,
                    "default": field.default,
                    "allowed_values": field.allowed_values,
                    "ref_schema": field.ref_schema,
                    "description": field.description,
                }
                for field in schema.fields
            ]

            # Get or create dynamic model
            model_class = await get_or_create_model(
                schema_name=collection_name,
                fields=fields_as_dicts,  # Pass as dicts
                client_id=client_id,
            )

            # Create document instance
            created_docs = []
            for data in documents:
                doc_data = {
                    "client_id": client_id,
                    "created_by": created_by,
                    "updated_by": created_by,
                    **data,
                }

                document = model_class(**doc_data)
                await document.insert()

                created_docs.append(
                    {
                        "id": str(document.id),
                        "collection": collection_name,
                        "client_id": client_id,
                        "data": doc_data,
                        "created_at": document.created_at.isoformat(),
                        "created_by": created_by,
                    }
                )

            logger.info(f"Created {len(created_docs)} documents in {collection_name}")

            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_docs)} documents in {collection_name}",
                data=created_docs,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error creating document: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Error creating document: {str(e)}",
            )

    @staticmethod
    async def get_by_id(
        client_id: str, collection_name: str, document_id: str, db: AsyncSession
    ) -> APIResponse:
        """Get a document by ID from a dynamic collection"""
        try:
            # Validate client exists
            # await DocumentService._validate_client(client_id, db)

            # Get active schema
            schema = await DocumentService._get_active_schema(
                client_id, collection_name
            )

            fields_as_dicts = [
                {
                    "name": field.name,
                    "type": field.type,
                    "required": field.required,
                    "unique": field.unique,
                    "default": field.default,
                    "allowed_values": field.allowed_values,
                    "ref_schema": field.ref_schema,
                    "description": field.description,
                }
                for field in schema.fields
            ]

            # Get model
            model_class = await get_or_create_model(
                schema_name=collection_name, fields=fields_as_dicts, client_id=client_id
            )

            # Fetch document
            document = await model_class.get(PydanticObjectId(document_id))

            if not document:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"Document with ID {document_id} not found in {collection_name}",
                )

            # Verify document belongs to client
            if document.client_id != client_id:
                raise HTTPException(
                    status_code=StatusCode.FORBIDDEN,
                    detail="Document does not belong to this client",
                )

            logger.info(f"Retrieved document {document_id} from {collection_name}")

            return APIResponse(
                success=True,
                message=f"Document retrieved from {collection_name}",
                data=document.model_dump(mode="json"),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving document: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Error retrieving document: {str(e)}",
            )

    @staticmethod
    async def get_all(
        client_id: str,
        collection_name: str,
        db: AsyncSession,
        skip: int = 0,
        limit: int = 100,
    ) -> APIResponse:
        """Get all documents from a dynamic collection for a client"""
        try:
            # Validate client exists
            # await DocumentService._validate_client(client_id, db)

            # Get active schema
            schema = await DocumentService._get_active_schema(
                client_id, collection_name
            )

            fields_as_dicts = [
                {
                    "name": field.name,
                    "type": field.type,
                    "required": field.required,
                    "unique": field.unique,
                    "default": field.default,
                    "allowed_values": field.allowed_values,
                    "ref_schema": field.ref_schema,
                    "description": field.description,
                }
                for field in schema.fields
            ]

            # Get model
            model_class = await get_or_create_model(
                schema_name=collection_name, fields=fields_as_dicts, client_id=client_id
            )

            # Fetch documents for this client only
            documents = (
                await model_class.find(model_class.client_id == client_id)
                .skip(skip)
                .limit(limit)
                .to_list()
            )

            logger.info(f"Retrieved {len(documents)} documents from {collection_name}")

            return APIResponse(
                success=True,
                message=f"Retrieved {len(documents)} documents from {collection_name}",
                data=[doc.model_dump(mode="json") for doc in documents],
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error retrieving documents: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Error retrieving documents: {str(e)}",
            )
    
    @staticmethod
    async def search(
        client_id: str,
        collection_name: str,
        column: str,
        value: str,
        db: AsyncSession,
        threshold: int = 70,
    ) -> APIResponse:
        """
        Search documents in a dynamic collection by specific field and value.
        
        Args:
            client_id: UUID of the client
            collection_name: Name of the collection (schema_name)
            column: Field name to search in
            value: Value to search for (partial matching)
            db: Database session
            
        Returns:
            APIResponse with matching documents
        """
        try:
            # Get active schema
            schema = await DocumentService._get_active_schema(
                client_id, collection_name
            )

            # Get field names from schema
            field_names = [field.name for field in schema.fields]
            
            # Add base fields that are always searchable
            base_searchable_fields = ['client_id', 'created_by', 'updated_by']
            allowed_fields = field_names + base_searchable_fields
            
            # Validate column name
            if column not in allowed_fields:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail=f"Invalid search column: {column}. Allowed columns: {', '.join(allowed_fields)}"
                )

            # Convert schema fields to dicts
            fields_as_dicts = [
                {
                    "name": field.name,
                    "type": field.type,
                    "required": field.required,
                    "unique": field.unique,
                    "default": field.default,
                    "allowed_values": field.allowed_values,
                    "ref_schema": field.ref_schema,
                    "description": field.description,
                }
                for field in schema.fields
            ]

            # Get model
            model_class = await get_or_create_model(
                schema_name=collection_name, 
                fields=fields_as_dicts, 
                client_id=client_id
            )

            value = value.strip()

            # Get ALL documents for this client (without field filter)
            search_filter = {"client_id": client_id}
            all_documents = await model_class.find(search_filter).to_list()

            best_match = None
            best_score = 0

            for doc in all_documents:
                # Get the value of the column we're searching
                doc_dict = doc.model_dump(mode="json")
                doc_value = doc_dict.get(column)
                
                if doc_value:
                    # Calculate similarity score
                    score = fuzz.partial_ratio(value.lower(), str(doc_value).lower())
                    
                    if score >= threshold and score > best_score:
                        best_match = doc
                        best_score = score
                        print(f"New best match: '{doc_value}' (score: {score})")
            
            # Sort by score (highest first)
            if not best_match:
                logger.info(
                    f"No documents found in {collection_name} where {column} matches '{value}'"
                )
                return APIResponse(
                    success=True,
                    message=f"No documents found where {column} matches '{value}'",
                    data=[]
                )

            print(f"Final best match score: {best_score}")
            logger.info(
                f"Found best match in {collection_name} for {column}='{value}' with score {best_score}"
            )

            return APIResponse(
                success=True,
                message=f"Found best match where {column} matches '{value}' (score: {best_score})",
                data=[best_match.model_dump(mode="json")]
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error searching documents: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Error searching documents: {str(e)}"
            )

    @staticmethod
    async def update(
        client_id: str,
        collection_name: str,
        document_id: str,
        data: Dict[str, Any],
        db: AsyncSession,
        updated_by: Optional[str] = None,
    ) -> APIResponse:
        """Update a document in a dynamic collection"""
        try:
            # Validate client exists
            # await DocumentService._validate_client(client_id, db)

            # Get active schema
            schema = await DocumentService._get_active_schema(
                client_id, collection_name
            )

            # Validate update data
            await DocumentService._validate_document_data(data, schema)

            fields_as_dicts = [
                {
                    "name": field.name,
                    "type": field.type,
                    "required": field.required,
                    "unique": field.unique,
                    "default": field.default,
                    "allowed_values": field.allowed_values,
                    "ref_schema": field.ref_schema,
                    "description": field.description,
                }
                for field in schema.fields
            ]

            # Get model
            model_class = await get_or_create_model(
                schema_name=collection_name, fields=fields_as_dicts, client_id=client_id
            )

            # Fetch document
            document = await model_class.get(PydanticObjectId(document_id))

            if not document:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"Document with ID {document_id} not found in {collection_name}",
                )

            # Verify ownership
            if document.client_id != client_id:
                raise HTTPException(
                    status_code=StatusCode.FORBIDDEN,
                    detail="Document does not belong to this client",
                )

            # Update fields
            for key, value in data.items():
                setattr(document, key, value)

            document.updated_at = datetime.now(timezone.utc)
            document.updated_by = updated_by

            await document.save()

            logger.info(f"Updated document {document_id} in {collection_name}")

            return APIResponse(
                success=True,
                message=f"Document updated successfully in {collection_name}",
                data=document.model_dump(mode="json"),
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error updating document: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Error updating document: {str(e)}",
            )

    @staticmethod
    async def delete(
        client_id: str, collection_name: str, document_id: str, db: AsyncSession
    ) -> APIResponse:
        """Delete a document from a dynamic collection"""
        try:
            # Validate client exists
            # await DocumentService._validate_client(client_id, db)

            # Get active schema
            schema = await DocumentService._get_active_schema(
                client_id, collection_name
            )

            fields_as_dicts = [
                {
                    "name": field.name,
                    "type": field.type,
                    "required": field.required,
                    "unique": field.unique,
                    "default": field.default,
                    "allowed_values": field.allowed_values,
                    "ref_schema": field.ref_schema,
                    "description": field.description,
                }
                for field in schema.fields
            ]

            # Get model
            model_class = await get_or_create_model(
                schema_name=collection_name, fields=fields_as_dicts, client_id=client_id
            )

            # Fetch document
            document = await model_class.get(PydanticObjectId(document_id))

            if not document:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"Document with ID {document_id} not found in {collection_name}",
                )

            # Verify ownership
            if document.client_id != client_id:
                raise HTTPException(
                    status_code=StatusCode.FORBIDDEN,
                    detail="Document does not belong to this client",
                )

            await document.delete()

            logger.info(f"Deleted document {document_id} from {collection_name}")

            return APIResponse(
                success=True,
                message=f"Document deleted successfully from {collection_name}",
                data=None,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error deleting document: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Error deleting document: {str(e)}",
            )
