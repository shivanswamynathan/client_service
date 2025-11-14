import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import UUID

from bson import ObjectId
from client_service.api.constants.status_codes import StatusCode
from client_service.schemas.base_response import APIResponse
from client_service.schemas.client_db.client_models import Clients
from client_service.schemas.client_db.vendor_models import VendorMaster
from client_service.schemas.mongo_schemas.client_schema_model import ClientSchema
from rapidfuzz import fuzz
from client_service.schemas.mongo_schemas.dynamic_document_model import (
    get_or_create_collection_config,
    prepare_document_for_insert,
    prepare_document_for_update,
    serialize_document,
    create_indexes_for_schema,
    validate_document_against_config,
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
    async def create(
        client_id: str,
        collection_name: str,
        documents: List[Dict[str, Any]],
        db: AsyncSession,
        created_by: Optional[str] = None,
    ) -> APIResponse:
        """
        Create new documents in a dynamic collection using Motor.
        """
        try:
            # Get active schema
            schema = await DocumentService._get_active_schema(
                client_id, collection_name
            )

            # Convert SchemaField objects to dicts
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

            # Get or create collection config (Motor-based)
            config = await get_or_create_collection_config(
                schema_name=collection_name,
                fields=fields_as_dicts,
                client_id=client_id,
            )

            # Create indexes based on schema
            await create_indexes_for_schema(config.collection, fields_as_dicts)

            # Prepare and insert documents
            created_docs = []
            for data in documents:
                # Validate against schema
                await validate_document_against_config(data, config)

                # Prepare document with base fields
                doc_to_insert = prepare_document_for_insert(
                    data=data,
                    client_id=client_id,
                    created_by=created_by,
                    updated_by=created_by,
                )

                # Insert using Motor
                result = await config.collection.insert_one(doc_to_insert)

                # Prepare response
                created_doc = {
                    "id": str(result.inserted_id),
                    "collection": collection_name,
                    "client_id": client_id,
                    "data": data,
                    "created_at": doc_to_insert["created_at"].isoformat(),
                    "created_by": created_by,
                }

                created_docs.append(created_doc)

            logger.info(f"Created {len(created_docs)} documents in {collection_name}")

            return APIResponse(
                success=True,
                message=f"Successfully created {len(created_docs)} documents in {collection_name}",
                data=created_docs,
            )

        except ValueError as ve:
            # Validation errors
            raise HTTPException(
                status_code=StatusCode.UNPROCESSABLE_ENTITY,
                detail=str(ve),
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
        """Get a document by ID from a dynamic collection using Motor"""
        try:
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

            # Get collection config
            config = await get_or_create_collection_config(
                schema_name=collection_name, fields=fields_as_dicts, client_id=client_id
            )

            # Find document using Motor
            document = await config.collection.find_one(
                {"_id": ObjectId(document_id), "client_id": client_id}
            )

            if not document:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"Document with ID {document_id} not found in {collection_name}",
                )

            # Serialize for response
            document = serialize_document(document)

            logger.info(f"Retrieved document {document_id} from {collection_name}")

            return APIResponse(
                success=True,
                message=f"Document retrieved from {collection_name}",
                data=document,
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
        """Get all documents from a dynamic collection using Motor"""
        try:
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

            # Get collection config
            config = await get_or_create_collection_config(
                schema_name=collection_name, fields=fields_as_dicts, client_id=client_id
            )

            # Find documents using Motor with pagination
            cursor = (
                config.collection.find({"client_id": client_id}).skip(skip).limit(limit)
            )
            documents = await cursor.to_list(length=limit)

            # Serialize all documents
            documents = [serialize_document(doc) for doc in documents]

            logger.info(f"Retrieved {len(documents)} documents from {collection_name}")

            return APIResponse(
                success=True,
                message=f"Retrieved {len(documents)} documents from {collection_name}",
                data=documents,
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
        top_n: int = 3,
    ) -> APIResponse:
        """
        Search documents in a dynamic collection by specific field and value using Motor.

        Args:
            client_id: UUID of the client
            collection_name: Name of the collection (schema_name)
            column: Field name to search in
            value: Value to search for (fuzzy matching with rapidfuzz)
            db: Database session
            threshold: Minimum similarity score (0-100)

        Returns:
            APIResponse with best matching document
        """
        try:
            # Get active schema
            schema = await DocumentService._get_active_schema(
                client_id, collection_name
            )

            # Get field names from schema
            field_names = [field.name for field in schema.fields]

            # Add base fields that are always searchable
            base_searchable_fields = ["client_id", "created_by", "updated_by"]
            allowed_fields = field_names + base_searchable_fields

            # Validate column name
            if column not in allowed_fields:
                raise HTTPException(
                    status_code=StatusCode.BAD_REQUEST,
                    detail=f"Invalid search column: {column}. Allowed columns: {', '.join(allowed_fields)}",
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

            # Get collection config
            config = await get_or_create_collection_config(
                schema_name=collection_name, fields=fields_as_dicts, client_id=client_id
            )

            value = value.strip()

            # Get ALL documents for this client using Motor
            cursor = config.collection.find({"client_id": client_id})
            all_documents = await cursor.to_list(length=None)

            matches = []

            for doc in all_documents:
                # Get the value of the column we're searching
                doc_value = doc.get(column)

                if doc_value:
                    # Calculate similarity score using rapidfuzz
                    score = fuzz.partial_ratio(value.lower(), str(doc_value).lower())

                    if score >= threshold:
                        matches.append({"document": doc, "score": score})
                        logger.debug(f"Match found: '{doc_value}' (score: {score})")

            matches.sort(key=lambda x: x["score"], reverse=True)
            top_matches = matches[:top_n]

            if not top_matches:
                logger.info(
                    f"No documents found in {collection_name} where {column} matches '{value}'"
                )
                return APIResponse(
                    success=True,
                    message=f"No documents found where {column} matches '{value}'",
                    data=[],
                )

            # Serialize the best match
            serialized_matches = [
                {
                    **serialize_document(match["document"])
                }
                for match in top_matches
            ]

            logger.info(
                f"Found {len(top_matches)} match(es) in {collection_name} for {column}='{value}'"
            )

            return APIResponse(
                success=True,
                message=f"Found {len(top_matches)} match(es) where {column} matches '{value}'",
                data=serialized_matches,
            )

        except HTTPException:
            raise
        except Exception as e:
            logger.error(f"Error searching documents: {str(e)}", exc_info=True)
            raise HTTPException(
                status_code=StatusCode.BAD_REQUEST,
                detail=f"Error searching documents: {str(e)}",
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
        """Update a document in a dynamic collection using Motor"""
        try:
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

            # Get collection config
            config = await get_or_create_collection_config(
                schema_name=collection_name, fields=fields_as_dicts, client_id=client_id
            )

            # Validate update data (partial validation - only fields being updated)
            # Note: We're not enforcing required fields on update
            for field_name, value in data.items():
                field_config = next(
                    (f for f in fields_as_dicts if f["name"] == field_name), None
                )
                if field_config:
                    # Basic type and enum validation
                    field_type = field_config.get("type")
                    allowed_values = field_config.get("allowed_values")

                    type_checks = {
                        "string": (str, "string"),
                        "number": ((int, float), "number"),
                        "boolean": (bool, "boolean"),
                        "array": (list, "array"),
                        "object": (dict, "object"),
                        "date": (str, "date string (ISO format)"),
                    }

                    if field_type in type_checks:
                        expected_type, type_name = type_checks[field_type]
                        if not isinstance(value, expected_type):
                            raise ValueError(
                                f"Field '{field_name}' must be {type_name}, got {type(value).__name__}"
                            )

                    if allowed_values and value not in allowed_values:
                        raise ValueError(
                            f"Field '{field_name}' must be one of {allowed_values}, got '{value}'"
                        )

            # Check if document exists and belongs to client
            existing_doc = await config.collection.find_one(
                {"_id": ObjectId(document_id), "client_id": client_id}
            )

            if not existing_doc:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"Document with ID {document_id} not found in {collection_name}",
                )

            # Prepare update operation
            update_operation = prepare_document_for_update(data, updated_by)

            # Update using Motor
            result = await config.collection.update_one(
                {"_id": ObjectId(document_id), "client_id": client_id}, update_operation
            )

            if result.modified_count == 0:
                logger.warning(f"Document {document_id} was not modified")

            # Fetch updated document
            updated_doc = await config.collection.find_one(
                {"_id": ObjectId(document_id)}
            )
            updated_doc = serialize_document(updated_doc)

            logger.info(f"Updated document {document_id} in {collection_name}")

            return APIResponse(
                success=True,
                message=f"Document updated successfully in {collection_name}",
                data=updated_doc,
            )

        except ValueError as ve:
            raise HTTPException(
                status_code=StatusCode.UNPROCESSABLE_ENTITY,
                detail=str(ve),
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
        """Delete a document from a dynamic collection using Motor"""
        try:
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

            # Get collection config
            config = await get_or_create_collection_config(
                schema_name=collection_name, fields=fields_as_dicts, client_id=client_id
            )

            # Check if document exists and belongs to client
            existing_doc = await config.collection.find_one(
                {"_id": ObjectId(document_id), "client_id": client_id}
            )

            if not existing_doc:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"Document with ID {document_id} not found in {collection_name}",
                )

            # Delete using Motor
            result = await config.collection.delete_one(
                {"_id": ObjectId(document_id), "client_id": client_id}
            )

            if result.deleted_count == 0:
                raise HTTPException(
                    status_code=StatusCode.NOT_FOUND,
                    detail=f"Failed to delete document {document_id}",
                )

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
