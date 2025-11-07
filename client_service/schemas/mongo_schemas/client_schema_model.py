from beanie import Document
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Any, Dict
from datetime import datetime, timezone
from uuid import UUID


class SchemaField(BaseModel):  # ← Must be BaseModel, NOT Document
    """
    Nested model for individual field definitions within a schema
    """
    name: str = Field(..., description="Field name (e.g., 'po_number')")
    type: str = Field(..., description="Data type: string, number, date, boolean, array, object")
    required: bool = Field(default=False, description="Whether field is mandatory")
    unique: bool = Field(default=False, description="Whether field must be unique")
    default: Optional[Any] = Field(None, description="Default value if not provided")
    allowed_values: Optional[List[Any]] = Field(None, description="Enum list of allowed values")
    ref_schema: Optional[str] = Field(None, description="Reference to another schema (e.g., 'invoice.po_number')")
    description: Optional[str] = Field(None, description="Human-readable field explanation")
    fields: Optional[List[Dict[str, Any]]] = Field(None, description="Nested fields for array/object types (stored as dictionaries)")

    @field_validator('type')
    @classmethod
    def validate_field_type(cls, v):
        """Validate that field type is one of the allowed types"""
        allowed_types = ['string', 'number', 'date', 'boolean', 'array', 'object']
        if v not in allowed_types:
            raise ValueError(f"Field type must be one of {allowed_types}")
        return v



class ClientSchema(Document):
    """
    Main Document Model for Client Schema Collection
    Stores schema definitions for various document types (PO, GRN, Invoice, etc.)
    """
    client_id: str = Field(..., description="UUID of the client as string")  
    schema_name: str = Field(..., description="Name of schema (e.g., 'purchase_order', 'grn', 'invoice')")
    version: int = Field(default=1, description="Version number (1, 2, 3...)")
    is_active: bool = Field(default=True, description="Whether this version is the active schema")
    description: Optional[str] = Field(None, description="Short description of schema purpose")
    fields: List[SchemaField] = Field(default_factory=list, description="Array of field definitions")
    created_by: Optional[str] = Field(None, description="UUID of user who created this schema")
    updated_by: Optional[str] = Field(None, description="UUID of user who last updated this schema")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator('client_id')
    @classmethod
    def validate_client_id(cls, v):
        """Validate that client_id is a valid UUID string"""
        if not v:
            raise ValueError("client_id is required")
        try:
            UUID(v)
            return v
        except (ValueError, AttributeError) as e:
            raise ValueError(f"client_id must be a valid UUID string, got: {v}")

    class Settings:
        name = "client_schemas"