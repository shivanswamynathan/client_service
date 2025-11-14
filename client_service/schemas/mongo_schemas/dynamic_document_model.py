import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Type
from uuid import UUID

from motor.motor_asyncio import AsyncIOMotorCollection
from client_service.db.mongo_db import get_mongo_db
from bson import ObjectId

logger = logging.getLogger(__name__)


class DynamicCollectionConfig:
    """
    Configuration class for dynamic collections.
    Stores field definitions and validation rules without Beanie dependency.
    """

    def __init__(
        self,
        schema_name: str,
        fields: List[Dict[str, Any]],
        client_id: str,
        collection: AsyncIOMotorCollection,
    ):
        self.schema_name = schema_name
        self.fields = fields
        self.client_id = client_id
        self.collection = collection
        self.field_map = {field["name"]: field for field in fields}

    def get_field_type(self, field_name: str) -> Optional[str]:
        """Get the type of a field"""
        field = self.field_map.get(field_name)
        return field.get("type") if field else None

    def is_required(self, field_name: str) -> bool:
        """Check if a field is required"""
        field = self.field_map.get(field_name)
        return field.get("required", False) if field else False

    def get_default(self, field_name: str) -> Any:
        """Get default value for a field"""
        field = self.field_map.get(field_name)
        return field.get("default") if field else None

    def get_allowed_values(self, field_name: str) -> Optional[List[Any]]:
        """Get allowed values (enum) for a field"""
        field = self.field_map.get(field_name)
        return field.get("allowed_values") if field else None


def get_python_type(field_type: str) -> Type:
    """
    Convert schema field type string to Python type.

    Args:
        field_type: Type string from schema (string, number, date, boolean, array, object)

    Returns:
        Python type class
    """
    type_mapping = {
        "string": str,
        "number": float,
        "date": datetime,
        "boolean": bool,
        "array": list,
        "object": dict,
    }

    return type_mapping.get(field_type, str)


def prepare_document_for_insert(
    data: Dict[str, Any],
    client_id: str,
    created_by: Optional[str] = None,
    updated_by: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Prepare a document dictionary for insertion into MongoDB.
    Adds base fields that every document should have.

    Args:
        data: User-provided document data
        client_id: UUID of the client
        created_by: UUID of user creating the document
        updated_by: UUID of user updating the document

    Returns:
        Complete document ready for MongoDB insertion
    """
    now = datetime.now(timezone.utc)

    return {
        "client_id": client_id,
        "created_at": now,
        "updated_at": now,
        "created_by": created_by,
        "updated_by": updated_by,
        **data,  # User data
    }


def prepare_document_for_update(
    data: Dict[str, Any], updated_by: Optional[str] = None
) -> Dict[str, Any]:
    """
    Prepare update data for MongoDB update operation.
    Adds updated_at and updated_by fields.

    Args:
        data: Fields to update
        updated_by: UUID of user updating the document

    Returns:
        Update operation dict for MongoDB
    """
    update_fields = {
        **data,
        "updated_at": datetime.now(timezone.utc),
        "updated_by": updated_by,
    }

    return {"$set": update_fields}


def serialize_document(doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Serialize a MongoDB document for API response.
    Converts ObjectId to string and handles datetime serialization.

    Args:
        doc: Raw document from MongoDB

    Returns:
        JSON-serializable dictionary
    """
    if not doc:
        return doc

    # Convert ObjectId to string
    if "_id" in doc:
        doc["_id"] = str(doc["_id"])

    # Convert datetime objects to ISO format strings
    for key, value in doc.items():
        if isinstance(value, datetime):
            doc[key] = value.isoformat()
        elif isinstance(value, ObjectId):
            doc[key] = str(value)

    return doc


async def get_collection(schema_name: str) -> AsyncIOMotorCollection:
    """
    Get Motor collection for a dynamic schema.
    Creates the collection if it doesn't exist.

    Args:
        schema_name: Name of the collection (e.g., 'purchase_order', 'BOE', 'GRN')

    Returns:
        AsyncIOMotorCollection instance
    """
    db = get_mongo_db()
    collection = db[schema_name]

    # Create indexes for common query patterns
    await collection.create_index("client_id")
    await collection.create_index("created_at")
    await collection.create_index("updated_at")

    logger.info(f"Retrieved/created Motor collection: {schema_name}")

    return collection


# Global registry to store collection configurations
_collection_registry: Dict[str, DynamicCollectionConfig] = {}


async def get_or_create_collection_config(
    schema_name: str, fields: List[Dict[str, Any]], client_id: str
) -> DynamicCollectionConfig:
    """
    Get existing collection config from registry or create new one.
    Uses Motor collections directly instead of Beanie models.

    Args:
        schema_name: Name of the schema/collection
        fields: List of field definitions
        client_id: Client ID

    Returns:
        DynamicCollectionConfig with Motor collection
    """
    # Create unique key for this configuration
    config_key = f"{client_id}_{schema_name}"

    if config_key not in _collection_registry:
        # Get Motor collection
        collection = await get_collection(schema_name)

        # Create configuration
        config = DynamicCollectionConfig(
            schema_name=schema_name,
            fields=fields,
            client_id=client_id,
            collection=collection,
        )

        _collection_registry[config_key] = config
        logger.info(f"Registered new collection config: {config_key}")
    else:
        logger.info(f"Using existing collection config: {config_key}")

    return _collection_registry[config_key]


def clear_collection_registry():
    """Clear the collection registry (useful for testing)"""
    global _collection_registry
    _collection_registry = {}
    logger.info("Collection registry cleared")


def get_registered_collections() -> List[str]:
    """Get list of all registered collection keys"""
    return list(_collection_registry.keys())


async def create_indexes_for_schema(
    collection: AsyncIOMotorCollection, fields: List[Dict[str, Any]]
):
    """
    Create indexes based on schema field definitions.
    Also removes old unique indexes that are no longer marked as unique.
    
    Args:
        collection: Motor collection
        fields: List of field definitions from schema
    """
    # Get existing indexes
    existing_indexes = await collection.list_indexes().to_list(length=None)
    
    # Track which unique indexes should exist
    required_unique_indexes = set()
    
    # Create indexes for fields marked as unique
    for field in fields:
        if field.get("unique", False):
            field_name = field["name"]
            index_name = f"{field_name}_1_client_id_1"
            required_unique_indexes.add(index_name)
            
            # Create compound index with client_id to ensure uniqueness per client
            try:
                await collection.create_index(
                    [(field_name, 1), ("client_id", 1)], 
                    unique=True,
                    name=index_name
                )
                logger.info(
                    f"Created unique compound index on ({field_name}, client_id) for collection {collection.name}"
                )
            except Exception as e:
                logger.warning(f"Index {index_name} may already exist: {e}")
    
    # Drop unique indexes that are no longer needed
    for index_info in existing_indexes:
        index_name = index_info.get("name")
        is_unique = index_info.get("unique", False)
        
        # Skip the default _id index
        if index_name == "_id_":
            continue
        
        # If it's a unique index with our naming pattern and not in required list, drop it
        if is_unique and "_1_client_id_1" in index_name and index_name not in required_unique_indexes:
            try:
                await collection.drop_index(index_name)
                logger.info(f"Dropped obsolete unique index: {index_name} from collection {collection.name}")
            except Exception as e:
                logger.warning(f"Could not drop index {index_name}: {e}")

async def validate_document_against_config(
    data: Dict[str, Any], config: DynamicCollectionConfig
) -> None:
    """
    Validate document data against collection configuration.
    Raises ValueError if validation fails.

    Args:
        data: Document data to validate
        config: Collection configuration with field definitions

    Raises:
        ValueError: If validation fails
    """
    errors = []

    for field in config.fields:
        field_name = field["name"]
        field_type = field["type"]
        required = field.get("required", False)
        allowed_values = field.get("allowed_values")

        # Check required fields
        if required and field_name not in data:
            errors.append(f"Required field '{field_name}' is missing")
            continue

        # Skip if field not provided and not required
        if field_name not in data:
            continue

        value = data[field_name]

        # Type validation
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
                errors.append(
                    f"Field '{field_name}' must be {type_name}, got {type(value).__name__}"
                )

        # Enum validation
        if allowed_values and value not in allowed_values:
            errors.append(
                f"Field '{field_name}' must be one of {allowed_values}, got '{value}'"
            )

    if errors:
        raise ValueError(f"Validation errors: {'; '.join(errors)}")
