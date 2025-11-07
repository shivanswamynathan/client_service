import logging
from datetime import datetime, timezone
from typing import Any, ClassVar, Dict, List, Optional, Type
from uuid import UUID

from beanie import Document, PydanticObjectId, init_beanie
from client_service.db.mongo_db import get_mongo_db
from pydantic import Field, field_validator, ConfigDict

logger = logging.getLogger(__name__)


class BaseDynamicDocument(Document):
    """
    Base class for all dynamically created documents.
    Contains common fields that every dynamic document should have.
    """

    client_id: str = Field(..., description="UUID of the client as string")
    # vendor_id: str = Field(..., description="UUID of vendor (required)")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    created_by: Optional[str] = Field(None, description="UUID of user who created this")
    updated_by: Optional[str] = Field(
        None, description="UUID of user who last updated this"
    )

    # @field_validator("client_id")
    # @classmethod
    # def validate_client_id(cls, v):
    #     """Validate that client_id is a valid UUID string"""
    #     try:
    #         UUID(v)
    #         return v
    #     except (ValueError, AttributeError):
    #         raise ValueError(f"client_id must be a valid UUID string, got: {v}")

    # @field_validator('vendor_id')
    # @classmethod
    # def validate_vendor_id(cls, v):
    #     """Validate that vendor_id is a valid UUID string"""
    #     try:
    #         UUID(v)
    #         return v
    #     except (ValueError, AttributeError):
    #         raise ValueError(f"vendor_id must be a valid UUID string, got: {v}")


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


async def initialize_model_collection(model_class: Type[BaseDynamicDocument]):
    """
    Initialize a dynamically created model with Beanie.

    Args:
        model_class: The Document model class to initialize
    """
    try:
        db = get_mongo_db()
        await init_beanie(database=db, document_models=[model_class])
        logger.info(f"Initialized Beanie collection for model: {model_class.__name__}")
    except Exception as e:
        logger.warning(f"Model {model_class.__name__} may already be initialized: {e}")


def create_dynamic_document_model(
    schema_name: str, fields: List[Dict[str, Any]], client_id: str
) -> Type[BaseDynamicDocument]:
    """
    Factory function to create a Beanie Document class dynamically.

    Args:
        schema_name: Name of the schema (e.g., 'purchase_order', 'grn', 'invoice')
        fields: List of field definitions from ClientSchema
        client_id: Client ID for namespacing

    Returns:
        Dynamically created Document class
    """

    attributes: Dict[str, Any] = {}
    annotations: Dict[str, Any] = {}

    for field in fields:
        field_name = field.get("name")
        field_type = field.get("type")
        required = field.get("required", False)
        unique = field.get("unique", False)
        default = field.get("default")
        description = field.get("description", "")
        allowed_values = field.get("allowed_values")

        # Get Python type
        python_type = get_python_type(field_type)

        # Handle required vs optional fields
        if required:
            if default is not None:
                # Required with default
                attributes[field_name] = Field(default=default, description=description)
                annotations[field_name] = python_type
            else:
                # Required without default
                attributes[field_name] = Field(..., description=description)
                annotations[field_name] = python_type
        else:
            # Optional field
            attributes[field_name] = Field(default=default, description=description)
            annotations[field_name] = Optional[python_type]

    # Add module and qualname for Pydantic
    attributes["__module__"] = __name__

    # Create the dynamic Document class name
    # Capitalize class name (e.g., 'purchase_order' -> 'PurchaseOrder', 'GRN' -> 'Grn')
    class_name = "".join(word.capitalize() for word in schema_name.split("_"))

    # Add qualname
    attributes["__qualname__"] = class_name

    # Create Settings class for collection configuration
    # IMPORTANT: This defines the MongoDB collection name
    class DynamicSettings:
        name = schema_name  # This will be 'GRN', 'purchase_order', etc.
        is_root = False

    # Add Settings as ClassVar to annotations
    annotations["Settings"] = ClassVar[type]
    attributes["Settings"] = DynamicSettings

    # Add annotations to attributes
    attributes["__annotations__"] = annotations

    # Pydantic v2: allow arbitrary types to prevent schema serializer issues on Beanie internals
    attributes["model_config"] = ConfigDict(arbitrary_types_allowed=True)

    # Create the dynamic model using type()
    DynamicModel = type(class_name, (BaseDynamicDocument,), attributes)

    logger.info(f"Created dynamic model: {class_name} for collection: {schema_name}")
    logger.info(f"Collection name will be: {DynamicModel.Settings.name}")

    return DynamicModel


# Global registry to store created models
_model_registry: Dict[str, Type[BaseDynamicDocument]] = {}


async def get_or_create_model(
    schema_name: str, fields: List[Dict[str, Any]], client_id: str
) -> Type[BaseDynamicDocument]:
    """
    Get existing model from registry or create new one.

    Args:
        schema_name: Name of the schema
        fields: List of field definitions
        client_id: Client ID

    Returns:
        Document model class
    """
    # Create unique key for this model
    model_key = f"{client_id}_{schema_name}"

    if model_key not in _model_registry:
        model = create_dynamic_document_model(schema_name, fields, client_id)

        # Initialize the model with Beanie
        await initialize_model_collection(model)

        _model_registry[model_key] = model
        logger.info(f"Registered new model: {model_key}")

    return _model_registry[model_key]


def clear_model_registry():
    """Clear the model registry (useful for testing)"""
    global _model_registry
    _model_registry = {}
    logger.info("Model registry cleared")


def get_registered_models() -> List[str]:
    """Get list of all registered model keys"""
    return list(_model_registry.keys())
