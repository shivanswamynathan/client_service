import uuid
from typing import List

from client_service.api.dependencies import get_database_session
from client_service.db.mongo_db import get_db
from client_service.schemas.base_response import APIResponse
from client_service.schemas.client_db.vendor_models import VendorMaster
from client_service.schemas.pydantic_schemas import DocumentCreate, DocumentUpdate
from client_service.services.document_service import DocumentService
from fastapi import APIRouter, Depends, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.post(
    "/documents/create",
    response_model=APIResponse,
    status_code=status.HTTP_201_CREATED,
    operation_id="create_document",
    summary="Create a document in dynamic collection",
    description='Creates a new dynamic document (invoice, purchase order, GRN, etc.) in a MongoDB collection validated against the active schema. Call: POST /documents/create with body: {"client_id": "uuid", "collection_name": "invoice", "data": {"invoice_number": "INV-001", ...}, "created_by": "user-uuid"}. Validates against active schema.',
)
async def create_document(
    document_data: DocumentCreate, db: AsyncSession = Depends(get_database_session)
):
    """
    Create a new document in a dynamic collection.

    Steps:
    1. Validates client exists in PostgreSQL
    2. Fetches active schema for the collection
    3. Validates document data against schema rules
    4. Creates document in MongoDB collection

    Returns:
        APIResponse with created document details
    """
    return await DocumentService.create(
        client_id=document_data.client_id,
        collection_name=document_data.collection_name,
        documents=document_data.data,
        db=db,
        created_by=document_data.created_by,
    )

@router.get(
    "/documents/{client_id}/{collection_name}/search",
    response_model=APIResponse,
    operation_id="search_documents",
    summary="Search documents by field",
    description=(
        "Search documents in a dynamic collection by specific field and value. Returns all matching documents.\n\n"
        "Query params:\n"
        "- column: name of a field defined in the collection schema, or one of the base fields [client_id, created_by, updated_by]\n"
        "- value: text to match (case-insensitive, partial matches allowed)\n\n"
        "Example: GET /documents/{client_id}/{collection_name}/search?column=invoice_number&value=INV-001"
    ),
)
async def search_documents(
    client_id: str,
    collection_name: str,
    column: str,
    value: str,
    db: AsyncSession = Depends(get_database_session),
):
    """
    Search documents by specific field in a dynamic collection.
    
    Args:
        client_id: UUID of the client
        collection_name: Name of the collection (schema_name)
        column: Field name to search in (must exist in schema)
        value: Value to search for (partial, case-insensitive match)
        
    Returns:
        APIResponse with list of matching documents
        
    """
    return await DocumentService.search(
        client_id=client_id,
        collection_name=collection_name,
        column=column,
        value=value,
        db=db,
    )

@router.get(
    "/documents/{client_id}/{collection_name}/{document_id}",
    response_model=APIResponse,
    operation_id="get_document",
    summary="Get document by ID",
    description="Retrieves a specific document by its MongoDB ObjectId from a client's collection. Returns complete document data including all fields and metadata. Call: GET /documents/{client_id}/{collection_name}/{document_id} where all are strings (document_id is 24-char ObjectId).",
)
async def get_document(
    client_id: str,
    collection_name: str,
    document_id: str,
    db: AsyncSession = Depends(get_database_session),
):
    """
    Get a document by its MongoDB ObjectId.

    Args:
        client_id: UUID of the client
        collection_name: Name of the collection (schema_name)
        document_id: MongoDB ObjectId as string

    Returns:
        APIResponse with document data
    """
    return await DocumentService.get_by_id(
        client_id=client_id,
        collection_name=collection_name,
        document_id=document_id,
        db=db,
    )


@router.get(
    "/documents/{client_id}/{collection_name}",
    response_model=APIResponse,
    operation_id="list_documents",
    summary="List all documents in collection",
    description="Lists all documents in a specific collection for a client with pagination. Returns array of documents with all fields and metadata. Call: GET /documents/{client_id}/{collection_name}?skip=0&limit=100. Default: skip=0, limit=100 (max).",
)
async def get_all_documents(
    client_id: str,
    collection_name: str,
    skip: int = 0,
    limit: int = 100,
    db: AsyncSession = Depends(get_database_session),
):
    """
    Get all documents in a collection for a specific client.

    Args:
        client_id: UUID of the client
        collection_name: Name of the collection (schema_name)
        skip: Number of documents to skip (for pagination)
        limit: Maximum number of documents to return (max 100)

    Returns:
        APIResponse with list of documents
    """
    return await DocumentService.get_all(
        client_id=client_id,
        collection_name=collection_name,
        db=db,
        skip=skip,
        limit=limit,
    )


@router.put(
    "/documents/{client_id}/{collection_name}/{document_id}",
    response_model=APIResponse,
    operation_id="update_document",
    summary="Update document",
    description='Updates specific fields in an existing document by document_id. Validates updates against active schema. Call: PUT /documents/{client_id}/{collection_name}/{document_id} with body: {"data": {"status": "approved"}, "updated_by": "user-uuid"}. Partial update supported.',
)
async def update_document(
    client_id: str,
    collection_name: str,
    document_id: str,
    update_data: DocumentUpdate,
    db: AsyncSession = Depends(get_database_session),
):
    """
    Update a document in a dynamic collection.

    Steps:
    1. Validates client and document exist
    2. Validates update data against schema
    3. Updates only the provided fields
    4. Updates updated_at timestamp

    Args:
        client_id: UUID of the client
        collection_name: Name of the collection
        document_id: MongoDB ObjectId
        update_data: Updated field values

    Returns:
        APIResponse with updated document
    """
    return await DocumentService.update(
        client_id=client_id,
        collection_name=collection_name,
        document_id=document_id,
        data=update_data.data,
        db=db,
        updated_by=update_data.updated_by,
    )


@router.delete(
    "/documents/{client_id}/{collection_name}/{document_id}",
    response_model=APIResponse,
    operation_id="delete_document",
    summary="Delete document",
    description="Permanently deletes a document from a collection by document_id. WARNING: Cannot be undone. Call: DELETE /documents/{client_id}/{collection_name}/{document_id} (no body). Document completely removed from MongoDB. Consider soft delete for audit trails.",
)
async def delete_document(
    client_id: str,
    collection_name: str,
    document_id: str,
    db: AsyncSession = Depends(get_database_session),
):
    """
    Delete a document from a dynamic collection.

    Args:
        client_id: UUID of the client
        collection_name: Name of the collection
        document_id: MongoDB ObjectId

    Returns:
        APIResponse confirming deletion
    """
    return await DocumentService.delete(
        client_id=client_id,
        collection_name=collection_name,
        document_id=document_id,
        db=db,
    )
