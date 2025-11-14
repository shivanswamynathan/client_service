import uuid
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Dict, List, Optional
from uuid import UUID

from beanie import Document, Indexed
from pydantic import BaseModel, EmailStr, Field, field_validator

# ==================== CENTRAL CLIENT SCHEMAS ====================


class CentralClientBase(BaseModel):
    """Base schema for central client information"""

    name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Name of the central client organization",
        examples=["Acme Holdings Inc"],
    )


class CentralClientCreate(CentralClientBase):
    """Schema for creating a new central client"""

    class Config:
        json_schema_extra = {"example": {"name": "Acme Holdings Inc"}}


class CentralClientUpdate(CentralClientBase):
    """Schema for updating an existing central client"""

    pass


class CentralClientResponse(CentralClientBase):
    """Schema for central client response data"""

    central_client_id: UUID = Field(
        ..., description="Unique identifier for the central client"
    )
    created_at: datetime = Field(
        ..., description="Timestamp when central client was created"
    )
    updated_at: datetime = Field(
        ..., description="Timestamp when central client was last updated"
    )

    class Config:
        from_attributes = True


# ==================== CLIENT SCHEMAS ====================


class ClientBase(BaseModel):
    """Base schema for client information"""

    client_name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Unique name of the client organization",
        examples=["Acme Corporation"],
    )
    central_client_id: UUID = Field(
        ..., description="UUID of the parent central client"
    )
    central_api_key: Optional[str] = Field(
        None, max_length=512, description="API key for central client authentication"
    )


class ClientCreate(ClientBase):
    """Schema for creating a new client"""

    class Config:
        json_schema_extra = {
            "example": {
                "client_name": "Acme Corporation",
                "central_client_id": "123e4567-e89b-12d3-a456-426614174000",
                "central_api_key": "sk_test_1234567890abcdef",
            }
        }


class ClientUpdate(ClientBase):
    """Schema for updating an existing client"""

    pass


class ClientResponse(ClientBase):
    """Schema for client response data"""

    client_id: UUID = Field(..., description="Unique identifier for the client")
    created_at: datetime = Field(..., description="Timestamp when client was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when client was last updated"
    )

    class Config:
        from_attributes = True


# ==================== CLIENT ENTITY SCHEMAS ====================


class ClientEntityBase(BaseModel):
    """Base schema for client entity information"""

    client_id: UUID = Field(..., description="UUID of the parent client organization")
    entity_name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Name of the client entity (branch/subsidiary)",
        examples=["Acme Corp - Mumbai Branch"],
    )
    gst_id: Optional[str] = Field(
        None,
        max_length=15,
        description="GST identification number for this entity",
        examples=["29ABCDE1234F1Z5"],
    )
    company_pan: Optional[str] = Field(
        None,
        max_length=10,
        description="PAN card number of the entity",
        examples=["ABCDE1234F"],
    )
    tan: Optional[str] = Field(
        None,
        max_length=10,
        description="Tax deduction account number",
        examples=["ABCD12345E"],
    )
    parent_client_id: Optional[UUID] = Field(
        None,
        description="Optional UUID of parent client entity for hierarchical structure",
    )


class ClientEntityCreate(ClientEntityBase):
    """Schema for creating a new client entity"""

    class Config:
        json_schema_extra = {
            "example": {
                "client_id": "123e4567-e89b-12d3-a456-426614174000",
                "entity_name": "Acme Corp - Mumbai Branch",
                "gst_id": "29ABCDE1234F1Z5",
                "company_pan": "ABCDE1234F",
                "tan": "ABCD12345E",
                "parent_client_id": "987fcdeb-51a2-43d7-9876-543210fedcba",
            }
        }


class ClientEntityUpdate(ClientEntityBase):
    """Schema for updating an existing client entity"""

    pass


class ClientEntityResponse(ClientEntityBase):
    """Schema for client entity response data"""

    entity_id: UUID = Field(..., description="Unique identifier for the entity")
    created_at: datetime = Field(..., description="Timestamp when entity was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when entity was last updated"
    )

    class Config:
        from_attributes = True


# ==================== ROLE SCHEMAS ====================


class RoleBase(BaseModel):
    """Base schema for role information"""

    role_name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Name of the role",
        examples=["Admin", "Manager", "Accountant", "Viewer"],
    )
    description: Optional[str] = Field(
        None, description="Detailed description of the role and its responsibilities"
    )


class RoleCreate(RoleBase):
    """Schema for creating a new role"""

    class Config:
        json_schema_extra = {
            "example": {
                "role_name": "Accountant",
                "description": "Can view and manage financial transactions",
            }
        }


class RoleUpdate(RoleBase):
    """Schema for updating an existing role"""

    pass


class RoleResponse(RoleBase):
    """Schema for role response data"""

    role_id: UUID = Field(..., description="Unique identifier for the role")
    created_at: datetime = Field(..., description="Timestamp when role was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when role was last updated"
    )

    class Config:
        from_attributes = True


# ==================== PERMISSION SCHEMAS ====================


class PermissionBase(BaseModel):
    """Base schema for permission information"""

    permission_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Name of the permission",
        examples=[
            "create_user",
            "delete_transaction",
            "view_reports",
            "approve_invoice",
        ],
    )
    description: Optional[str] = Field(
        None, description="Detailed description of what this permission allows"
    )


class PermissionCreate(PermissionBase):
    """Schema for creating a new permission"""

    class Config:
        json_schema_extra = {
            "example": {
                "permission_name": "approve_invoice",
                "description": "Allows user to approve vendor invoices",
            }
        }


class PermissionUpdate(PermissionBase):
    """Schema for updating an existing permission"""

    pass


class PermissionResponse(PermissionBase):
    """Schema for permission response data"""

    permission_id: UUID = Field(..., description="Unique identifier for the permission")
    created_at: datetime = Field(
        ..., description="Timestamp when permission was created"
    )
    updated_at: datetime = Field(
        ..., description="Timestamp when permission was last updated"
    )

    class Config:
        from_attributes = True


# ==================== USER SCHEMAS ====================


class UserBase(BaseModel):
    """Base schema for user information"""

    user_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Full name of the user",
        examples=["John Doe"],
    )
    email: EmailStr = Field(
        ...,
        description="Unique email address for login and communication",
        examples=["john.doe@example.com"],
    )
    department: Optional[str] = Field(
        None, max_length=50, description="Department of the user", examples=["Finance"]
    )
    reporting_manager_id: Optional[UUID] = Field(
        None, description="UUID of the reporting manager role (from roles table)"
    )
    user_phone: Optional[str] = Field(
        None,
        max_length=15,
        description="Optional phone number with country code",
        examples=["+1234567890", "+919876543210"],
    )


class UserCreate(UserBase):
    """Schema for creating a new user account"""

    client_id: UUID = Field(
        ..., description="UUID of the client organization this user belongs to"
    )
    password_hash: str = Field(
        ...,
        min_length=8,
        description="Hashed password for authentication (should be pre-hashed on client side)",
    )

    class Config:
        json_schema_extra = {
            "example": {
                "client_id": "123e4567-e89b-12d3-a456-426614174000",
                "user_name": "John Doe",
                "email": "john.doe@example.com",
                "department": "Finance",
                "reporting_manager_id": "987fcdeb-51a2-43d7-9876-543210fedcba",
                "password_hash": "$2b$12$KIXqH9P1qF.yGZ0p7YxZ9O",
                "user_phone": "+919876543210",
            }
        }


class UserUpdate(UserBase):
    """Schema for updating an existing user"""

    password_hash: Optional[str] = Field(
        None,
        min_length=8,
        description="New hashed password (optional, only if changing password)",
    )


class UserResponse(UserBase):
    """Schema for user response data"""

    user_id: UUID = Field(..., description="Unique identifier for the user")
    client_id: UUID = Field(..., description="UUID of the client organization")
    created_at: datetime = Field(..., description="Timestamp when user was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when user was last updated"
    )

    class Config:
        from_attributes = True


# ==================== USER ROLE SCHEMAS ====================


class UserRoleCreate(BaseModel):
    """Schema for assigning a role to a user"""

    user_id: UUID = Field(..., description="UUID of the user to assign the role to")
    role_id: UUID = Field(..., description="UUID of the role to assign")

    class Config:
        json_schema_extra = {
            "example": {
                "user_id": "123e4567-e89b-12d3-a456-426614174000",
                "role_id": "987fcdeb-51a2-43d7-9876-543210fedcba",
            }
        }


class UserRoleResponse(BaseModel):
    """Schema for user role response data"""

    user_role_id: UUID = Field(
        ..., description="Unique identifier for the user-role mapping"
    )
    user_id: UUID = Field(..., description="UUID of the user")
    role_id: UUID = Field(..., description="UUID of the assigned role")
    assigned_at: datetime = Field(..., description="Timestamp when role was assigned")
    created_at: datetime = Field(
        ..., description="Timestamp when user role was created"
    )
    updated_at: datetime = Field(
        ..., description="Timestamp when user role was last updated"
    )

    class Config:
        from_attributes = True


# ==================== ROLE PERMISSION SCHEMAS ====================


class RolePermissionCreate(BaseModel):
    """Schema for assigning a permission to a role"""

    role_id: UUID = Field(..., description="UUID of the role to grant permission to")
    permission_id: UUID = Field(..., description="UUID of the permission to grant")

    class Config:
        json_schema_extra = {
            "example": {
                "role_id": "987fcdeb-51a2-43d7-9876-543210fedcba",
                "permission_id": "456e7890-a12b-34c5-d678-901234567890",
            }
        }


class RolePermissionResponse(BaseModel):
    """Schema for role permission response data"""

    role_permission_id: UUID = Field(
        ..., description="Unique identifier for the role-permission mapping"
    )
    role_id: UUID = Field(..., description="UUID of the role")
    permission_id: UUID = Field(..., description="UUID of the permission")
    created_at: datetime = Field(
        ..., description="Timestamp when role permission was created"
    )
    updated_at: datetime = Field(
        ..., description="Timestamp when role permission was last updated"
    )

    class Config:
        from_attributes = True


# ==================== USER LOG SCHEMAS ====================


class UserLogCreate(BaseModel):
    """Schema for creating a user activity log entry"""

    user_id: UUID = Field(..., description="UUID of the user who performed the action")
    action: dict = Field(
        ...,
        description="JSON object containing action details (type, description, metadata)",
        examples=[
            {
                "action_type": "login",
                "ip_address": "192.168.1.1",
                "device": "Chrome Browser",
            }
        ],
    )


class UserLogUpdate(BaseModel):
    """Schema for updating a user log entry"""

    action: dict = Field(..., description="Updated action details")


class UserLogResponse(BaseModel):
    """Schema for user log response data"""

    log_id: UUID = Field(..., description="Unique identifier for the log entry")
    user_id: UUID = Field(..., description="UUID of the user")
    action: dict = Field(..., description="Action details in JSON format")
    created_at: datetime = Field(..., description="Timestamp of log creation")
    updated_at: datetime = Field(..., description="Timestamp of the log entry")

    class Config:
        from_attributes = True


# ==================== VENDOR SCHEMAS ====================


class VendorBase(BaseModel):
    """Base schema for vendor information"""

    vendor_name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Name of the vendor/supplier company",
        examples=["ABC Suppliers Pvt Ltd"],
    )
    vendor_code: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Unique vendor identification code",
        examples=["VEND001", "SUPP-2025-001"],
    )
    email: Optional[EmailStr] = Field(None, description="Vendor contact email address")
    gst_id: Optional[str] = Field(
        None,
        max_length=15,
        description="GST identification number",
        examples=["29ABCDE1234F1Z5"],
    )
    company_pan: Optional[str] = Field(
        None,
        max_length=10,
        description="PAN card number of the vendor company",
        examples=["ABCDE1234F"],
    )
    tan: Optional[str] = Field(
        None, max_length=10, description="Tax deduction account number"
    )
    bank_acc_no: Optional[str] = Field(
        None,
        max_length=20,
        description="Bank account number for payments",
        examples=["1234567890123456"],
    )
    beneficiary_name: Optional[str] = Field(
        None,
        max_length=255,
        description="Name of the account holder as per bank records",
    )
    acc_verified: bool = Field(
        False, description="Whether the bank account has been verified (True/False)"
    )
    ifsc_code: Optional[str] = Field(
        None,
        max_length=11,
        description="IFSC code of the bank branch",
        examples=["SBIN0001234"],
    )
    payment_term_days: Optional[int] = Field(
        None,
        ge=0,
        le=365,
        description="Number of days for payment terms (e.g., Net 30, Net 45)",
        examples=[30, 45, 60, 90],
    )
    user_phone: Optional[str] = Field(
        None,
        max_length=15,
        description="Vendor contact phone number with country code",
        examples=["+919876543210"],
    )


class VendorCreate(VendorBase):
    """Schema for creating a new vendor"""

    class Config:
        json_schema_extra = {
            "example": {
                "vendor_name": "ABC Suppliers Pvt Ltd",
                "vendor_code": "VEND001",
                "email": "contact@abcsuppliers.com",
                "gst_id": "29ABCDE1234F1Z5",
                "company_pan": "ABCDE1234F",
                "bank_acc_no": "1234567890123456",
                "beneficiary_name": "ABC Suppliers Pvt Ltd",
                "ifsc_code": "SBIN0001234",
                "payment_term_days": 45,
                "user_phone": "+919876543210",
            }
        }


class VendorUpdate(VendorBase):
    """Schema for updating an existing vendor"""

    pass


class VendorResponse(VendorBase):
    """Schema for vendor response data"""

    vendor_id: UUID = Field(..., description="Unique identifier for the vendor")
    created_at: datetime = Field(..., description="Timestamp when vendor was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when vendor was last updated"
    )

    class Config:
        from_attributes = True


# ==================== TRANSACTION SCHEMAS ====================


class TransactionBase(BaseModel):
    """Base schema for transaction information"""

    vendor_id: UUID = Field(
        ..., description="UUID of the vendor associated with this transaction"
    )
    invoice_id: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Unique invoice identifier from the vendor",
        examples=["INV-2025-001", "BILL/2025/0123"],
    )
    client_entity_id: UUID = Field(
        ..., description="UUID of the client entity that received goods/services"
    )
    transaction_date: date = Field(
        ..., description="Date when the transaction occurred", examples=["2025-01-15"]
    )
    transaction_type: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Type of transaction",
        examples=["Purchase", "Payment", "Refund", "Credit Note", "Debit Note"],
    )
    amount: Decimal = Field(
        ...,
        gt=0,
        description="Transaction amount (must be positive)",
        examples=[1000.00, 2500.50, 15000.75],
    )
    currency: str = Field(
        "INR",
        min_length=3,
        max_length=4,
        description="ISO currency code for the transaction",
        examples=["INR", "USD", "EUR", "GBP"],
    )
    description: Optional[str] = Field(
        None, description="Detailed description of goods/services in the transaction"
    )
    notes: Optional[str] = Field(
        None, description="Additional notes, remarks, or internal comments"
    )
    status: int = Field(
        ...,
        ge=0,
        description="Transaction status code: 0=Pending, 1=Approved, 2=Rejected, 3=Processing, 4=Completed",
        examples=[0, 1, 2],
    )


class TransactionCreate(TransactionBase):
    """Schema for creating a new transaction"""

    class Config:
        json_schema_extra = {
            "example": {
                "vendor_id": "123e4567-e89b-12d3-a456-426614174000",
                "invoice_id": "INV-2025-001",
                "client_entity_id": "987fcdeb-51a2-43d7-9876-543210fedcba",
                "transaction_date": "2025-01-15",
                "transaction_type": "Purchase",
                "amount": 15000.50,
                "currency": "INR",
                "description": "Purchase of office supplies",
                "status": 0,
            }
        }


class TransactionUpdate(TransactionBase):
    """Schema for updating an existing transaction"""

    pass


class TransactionResponse(TransactionBase):
    """Schema for transaction response data"""

    transaction_id: UUID = Field(
        ..., description="Unique identifier for the transaction"
    )
    created_at: datetime = Field(
        ..., description="Timestamp when transaction was created"
    )
    updated_at: datetime = Field(
        ..., description="Timestamp when transaction was last updated"
    )

    class Config:
        from_attributes = True
        json_encoders = {Decimal: float}


# ==================== ACTION LOG SCHEMAS ====================


class ActionLogCreate(BaseModel):
    """Schema for creating an action log entry"""

    status: int = Field(
        ..., ge=0, description="Status code of the action", examples=[0, 1, 2]
    )
    action: dict = Field(
        ...,
        description="JSON object containing action details",
        examples=[
            {
                "action_type": "approval",
                "performed_by": "admin",
                "timestamp": "2025-01-15T10:30:00Z",
            }
        ],
    )


class ActionLogUpdate(BaseModel):
    """Schema for updating an action log entry"""

    status: int = Field(..., description="Updated status code")
    action: dict = Field(..., description="Updated action details")


class ActionLogResponse(BaseModel):
    """Schema for action log response data"""

    log_id: UUID = Field(..., description="Unique identifier for the action log")
    status: int = Field(..., description="Status code")
    action: dict = Field(..., description="Action details in JSON format")
    created_at: datetime = Field(..., description="Timestamp of log creation")
    updated_at: datetime = Field(..., description="Timestamp of the log entry")

    class Config:
        from_attributes = True


# ==================== TRANSACTION LOG SCHEMAS ====================


class TransactionLogCreate(BaseModel):
    """Schema for creating a transaction log entry"""

    transaction_id: UUID = Field(
        ..., description="UUID of the transaction this log entry belongs to"
    )
    action: dict = Field(
        ...,
        description="JSON object containing log action details",
        examples=[{"action": "status_change", "from": "pending", "to": "approved"}],
    )
    approval_time: Optional[datetime] = Field(
        None, description="Timestamp when the transaction was approved (if applicable)"
    )
    action_log_id: Optional[UUID] = Field(
        None, description="Optional reference to an action log entry"
    )
    user_log_id: Optional[UUID] = Field(
        None, description="Optional reference to a user log entry"
    )


class TransactionLogUpdate(BaseModel):
    """Schema for updating a transaction log entry"""

    action: dict = Field(..., description="Updated action details")
    approval_time: Optional[datetime] = Field(None, description="Updated approval time")
    action_log_id: Optional[UUID] = Field(
        None, description="Updated action log reference"
    )
    user_log_id: Optional[UUID] = Field(None, description="Updated user log reference")


class TransactionLogResponse(BaseModel):
    """Schema for transaction log response data"""

    log_id: UUID = Field(..., description="Unique identifier for the transaction log")
    transaction_id: UUID = Field(..., description="UUID of the associated transaction")
    action: dict = Field(..., description="Action details in JSON format")
    approval_time: Optional[datetime] = Field(None, description="Approval timestamp")
    action_log_id: Optional[UUID] = Field(None, description="Reference to action log")
    user_log_id: Optional[UUID] = Field(None, description="Reference to user log")
    created_at: datetime = Field(..., description="Timestamp of log creation")
    updated_at: datetime = Field(..., description="Timestamp of the log entry")

    class Config:
        from_attributes = True


# ==================== ITEM SCHEMAS ====================


class ItemBase(BaseModel):
    """Base schema for item master information"""

    item_code: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Unique item code/SKU",
        examples=["ITEM001", "SKU-2025-0123"],
    )
    item_name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Name of the item/product",
        examples=["Office Chair", "Laptop - Dell Inspiron"],
    )
    hsn_code: Optional[str] = Field(
        None,
        max_length=8,
        description="HSN (Harmonized System of Nomenclature) code for tax classification",
        examples=["94013090"],
    )
    expense_category_id: UUID = Field(
        ..., description="UUID of the expense category associated with this item"
    )
    description: Optional[str] = Field(
        None, description="Detailed description of the item"
    )
    unit_measurement: Optional[str] = Field(
        None,
        max_length=10,
        description="Unit of measurement for the item",
        examples=["PCS", "KG", "LITRE", "BOX", "METER"],
    )


class ItemCreate(ItemBase):
    """Schema for creating a new item"""

    class Config:
        json_schema_extra = {
            "example": {
                "item_code": "ITEM001",
                "item_name": "Office Chair - Ergonomic",
                "hsn_code": "94013090",
                "expense_category_id": "987fcdeb-51a2-43d7-9876-543210fedcba",
                "description": "Ergonomic office chair with lumbar support",
                "unit_measurement": "PCS",
            }
        }


class ItemUpdate(ItemBase):
    """Schema for updating an existing item"""

    pass


class ItemResponse(ItemBase):
    """Schema for item response data"""

    item_id: UUID = Field(..., description="Unique identifier for the item")
    created_at: datetime = Field(..., description="Timestamp when item was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when item was last updated"
    )

    class Config:
        from_attributes = True


# ==================== EXPENSE CATEGORY SCHEMAS ====================


class ExpenseCategoryBase(BaseModel):
    """Base schema for expense category master information"""

    category_name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Main category name for expenses",
        examples=["Travel"],
    )
    sub_category_name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Sub-category name",
        examples=["Flight"],
    )
    module_name: str = Field(
        ...,
        min_length=1,
        max_length=50,
        description="Module name associated with the category",
        examples=["Sales"],
    )
    description: Optional[str] = Field(
        None, description="Detailed description of the expense category"
    )


class ExpenseCategoryCreate(ExpenseCategoryBase):
    """Schema for creating a new expense category"""

    class Config:
        json_schema_extra = {
            "example": {
                "category_name": "Travel",
                "sub_category_name": "Flight",
                "module_name": "Sales",
                "description": "Flight expenses for sales team",
            }
        }


class ExpenseCategoryUpdate(ExpenseCategoryBase):
    """Schema for updating an existing expense category"""

    pass


class ExpenseCategoryResponse(ExpenseCategoryBase):
    """Schema for expense category response data"""

    expense_category_id: UUID = Field(
        ..., description="Unique identifier for the expense category"
    )
    created_at: datetime = Field(..., description="Timestamp when category was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when category was last updated"
    )

    class Config:
        from_attributes = True


# ==================== VENDOR CLASSIFICATION SCHEMAS ====================


class VendorClassificationBase(BaseModel):
    """Base schema for vendor classification"""

    client_entity_id: UUID = Field(..., description="UUID of the client entity")
    expense_category_id: UUID = Field(..., description="UUID of the expense category")
    vendor_id: UUID = Field(..., description="UUID of the vendor")


class VendorClassificationCreate(VendorClassificationBase):
    """Schema for creating a new vendor classification"""

    class Config:
        json_schema_extra = {
            "example": {
                "client_entity_id": "123e4567-e89b-12d3-a456-426614174000",
                "expense_category_id": "987fcdeb-51a2-43d7-9876-543210fedcba",
                "vendor_id": "456e7890-f12b-34d5-e678-901234567890",
            }
        }


class VendorClassificationUpdate(BaseModel):
    """Schema for updating an existing vendor classification (limited, as it's junction table)"""

    # Junction tables typically don't have updatable fields beyond the composite key
    # The composite key (client_entity_id, expense_category_id, vendor_id) is in the URL path
    # If you need to change the relationship, delete and recreate instead
    pass


class VendorClassificationResponse(VendorClassificationBase):
    """Schema for vendor classification response data"""

    vendor_classification_id: UUID = Field(
        ..., description="Unique identifier for the vendor classification"
    )
    created_at: datetime = Field(
        ..., description="Timestamp when classification was created"
    )
    updated_at: datetime = Field(
        ..., description="Timestamp when classification was last updated"
    )

    class Config:
        from_attributes = True


# ==================== WORKFLOW SCHEMAS ====================


class WorkflowBase(BaseModel):
    """Base schema for workflow ledger information"""

    client_id: UUID = Field(
        ..., description="UUID of the client organization for this workflow"
    )
    user_id: UUID = Field(
        ..., description="UUID of the user who initiated/owns this workflow"
    )
    workflow_name: str = Field(
        ...,
        min_length=1,
        max_length=255,
        description="Name of the workflow process",
        examples=["Invoice Approval", "Vendor Onboarding", "Payment Processing"],
    )
    request_count: int = Field(
        0, ge=0, description="Number of times this workflow has been requested/executed"
    )
    last_request_at: Optional[datetime] = Field(
        None, description="Timestamp of the most recent workflow execution"
    )


class WorkflowCreate(WorkflowBase):
    """Schema for creating a new workflow ledger"""

    class Config:
        json_schema_extra = {
            "example": {
                "client_id": "123e4567-e89b-12d3-a456-426614174000",
                "user_id": "987fcdeb-51a2-43d7-9876-543210fedcba",
                "workflow_name": "Invoice Approval Workflow",
                "request_count": 0,
            }
        }


class WorkflowUpdate(WorkflowBase):
    """Schema for updating an existing workflow ledger"""

    pass


class WorkflowResponse(WorkflowBase):
    """Schema for workflow response data"""

    ledger_id: UUID = Field(
        ..., description="Unique identifier for the workflow ledger"
    )
    created_at: datetime = Field(..., description="Timestamp when workflow was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when workflow was last updated"
    )

    class Config:
        from_attributes = True


class SchemaFieldBase(BaseModel):
    """Base schema for field definition"""

    name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Field name (e.g., 'po_number', 'vendor_id')",
        examples=["po_number", "total_amount", "status"],
    )
    type: str = Field(
        ...,
        description="Data type: string, number, date, boolean, array, object",
        examples=["string", "number", "date"],
    )
    required: bool = Field(default=False, description="Whether this field is mandatory")
    unique: bool = Field(default=False, description="Whether this field must be unique")
    default: Optional[Any] = Field(None, description="Default value if not provided")
    allowed_values: Optional[List[Any]] = Field(
        None,
        description="Enum list of allowed values",
        examples=[["Open", "Closed", "Cancelled"]],
    )
    ref_schema: Optional[str] = Field(
        None,
        max_length=200,
        description="Reference to another schema field (e.g., 'purchase_order.po_number')",
        examples=["purchase_order.po_number", "grn.grn_number"],
    )
    description: Optional[str] = Field(
        None, description="Human-readable explanation of the field"
    )
    fields: Optional[List[Dict[str, Any]]] = Field(
        None,
        description="Nested fields for array/object types (stored as dictionaries)",
    )

    @field_validator("type")
    @classmethod
    def validate_field_type(cls, v):
        """Validate field type"""
        allowed_types = ["string", "number", "date", "boolean", "array", "object"]
        if v not in allowed_types:
            raise ValueError(f"Field type must be one of: {', '.join(allowed_types)}")
        return v


class SchemaFieldCreate(SchemaFieldBase):
    """Schema for creating a field definition"""

    pass


class SchemaFieldResponse(SchemaFieldBase):
    """Schema for field response"""

    class Config:
        from_attributes = True


# ==================== CLIENT SCHEMA SCHEMAS ====================


class ClientSchemaBase(BaseModel):
    """Base schema for client schema information"""

    client_id: str = Field(
        ...,
        description="UUID of the client (as string)",
        examples=["184e06a1-319a-4a3b-9d2f-bb8ef879cbd1"],
    )
    schema_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Name of the schema (e.g., 'purchase_order', 'grn', 'invoice')",
        examples=["purchase_order", "grn", "invoice"],
    )
    description: Optional[str] = Field(
        None, description="Short description of the schema purpose"
    )
    fields: List[SchemaFieldCreate] = Field(
        ..., min_length=1, description="Array of field definitions for this schema"
    )

    @field_validator("client_id")
    @classmethod
    def validate_client_id(cls, v):
        """Validate that client_id is a valid UUID string"""
        if not v:
            raise ValueError("client_id is required")
        try:
            UUID(v)
            return v
        except ValueError:
            raise ValueError("client_id must be a valid UUID string")


class ClientSchemaCreate(ClientSchemaBase):
    """Schema for creating a new client schema"""

    version: Optional[int] = Field(
        None, ge=1, description="Version number (auto-generated if not provided)"
    )
    is_active: bool = Field(
        default=True, description="Whether this version should be active"
    )
    created_by: Optional[str] = Field(
        None, description="UUID of user creating this schema"
    )


class ClientSchemaUpdate(BaseModel):
    """Schema for updating an existing client schema (creates new version)"""

    description: Optional[str] = Field(None, description="Updated description")
    fields: Optional[List[SchemaFieldCreate]] = Field(
        None, min_length=1, description="Updated field definitions"
    )
    is_active: Optional[bool] = Field(
        None, description="Whether this version should be active"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user updating this schema"
    )


class ClientSchemaResponse(BaseModel):
    """Schema for client schema response data"""

    id: str = Field(..., description="MongoDB ObjectId as string", alias="_id")
    client_id: str = Field(..., description="UUID of the client")
    schema_name: str = Field(..., description="Name of the schema")
    version: int = Field(..., description="Version number")
    is_active: bool = Field(..., description="Whether this is the active version")
    description: Optional[str] = Field(None, description="Schema description")
    fields: List[SchemaFieldResponse] = Field(..., description="Field definitions")
    created_by: Optional[str] = Field(None, description="UUID of user who created this")
    updated_by: Optional[str] = Field(
        None, description="UUID of user who last updated this"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    class Config:
        from_attributes = True
        populate_by_name = True


# ==================== DYNAMIC DOCUMENT SCHEMAS ====================


class DocumentCreate(BaseModel):
    """Schema for creating a new document in a dynamic collection"""

    client_id: str = Field(
        ...,
        description="UUID of the client (as string)",
        examples=["184e06a1-319a-4a3b-9d2f-bb8ef879cbd1"],
    )
    # vendor_id: str = Field(
    #     ...,
    #     description="UUID of vendor (required)",
    #     examples=["123e4567-e89b-12d3-a456-426614174000"]
    # )
    collection_name: str = Field(
        ...,
        min_length=1,
        max_length=100,
        description="Name of the collection (must match an existing schema_name)",
        examples=["purchase_order", "grn", "invoice"],
    )
    data: List[Dict[str, Any]] = Field(
        ...,
        min_length=1,
        description="Array of document data. Each must match schema definition",
    )
    created_by: Optional[str] = Field(
        None, description="UUID of user creating this document"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "client_id": "184e06a1-319a-4a3b-9d2f-bb8ef879cbd1",
                "collection_name": "purchase_order",
                "data": [
                    {
                        "po_number": "PO-2025-001",
                        "total_amount": 15000.50,
                        "status": "Open",
                        "po_date": "2025-10-15",
                    }
                ],
                "created_by": "user-uuid-123",
            }
        }


class DocumentUpdate(BaseModel):
    """Schema for updating an existing document in a dynamic collection"""

    data: Dict[str, Any] = Field(
        ..., description="Updated document data (only fields to be changed)"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user updating this document"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "data": {"status": "Closed", "total_amount": 16000.00},
                "updated_by": "user-uuid-456",
            }
        }


class DocumentResponse(BaseModel):
    """Schema for document response data"""

    id: str = Field(..., description="MongoDB ObjectId as string", alias="_id")
    client_id: str = Field(..., description="UUID of the client")
    created_at: datetime = Field(..., description="Timestamp when document was created")
    updated_at: datetime = Field(
        ..., description="Timestamp when document was last updated"
    )
    created_by: Optional[str] = Field(None, description="UUID of user who created this")
    updated_by: Optional[str] = Field(
        None, description="UUID of user who last updated this"
    )
    data: Dict[str, Any] = Field(..., description="Document data")

    class Config:
        from_attributes = True
        populate_by_name = True


# ======================== CLIENT WORKFLOWS ===============================


class ClientWorkflowCreate(BaseModel):
    """Schema for creating a new client workflow"""

    client_workflow_id: Optional[str] = Field(
        default_factory=lambda: str(uuid.uuid4()),
        description="Unique identifier for the client workflow",
    )
    name: str = Field(
        ...,
        description="Name of the client workflow",
        example="Invoice Processing Workflow",
    )
    central_workflow_id: Optional[str] = Field(
        None, description="Reference to central workflow ID"
    )
    central_module_id: Optional[str] = Field(
        None, description="Reference to central module ID"
    )
    description: Optional[str] = Field(
        None, description="Short description of the workflow"
    )
    expense_categories: Optional[List[str]] = Field(
        default_factory=list, description="List of expense categories"
    )
    expense_filter: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Expense filter details"
    )
    agent_flow_definition: Optional[List[Dict[str, Any]]] = Field(
        default_factory=list, description="Agent flow configuration"
    )
    related_document_models: Optional[List[str]] = Field(
        default_factory=list, description="List of related document models"
    )
    created_by: Optional[str] = Field(
        None, description="UUID of user creating this workflow"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user updating this workflow"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Invoice Workflow",
                "central_workflow_id": "central_001",
                "central_module_id": "module_01",
                "description": "Workflow for handling invoice approvals",
                "expense_categories": ["Travel", "Supplies"],
                "expense_filter": {"category": "Travel", "limit": 1000},
                "agent_flow_definition": [{"agent": "validator", "step": 1}],
                "related_document_models": ["invoice", "payment"],
                "created_by": "user-uuid-123",
                "updated_by": "user-uuid-123",
            }
        }


class ClientWorkflowUpdate(BaseModel):
    """Schema for updating an existing client workflow"""

    name: Optional[str] = Field(None, description="Updated name of the client workflow")
    description: Optional[str] = Field(
        None, description="Updated description of the workflow"
    )
    expense_categories: Optional[List[str]] = Field(
        None, description="Updated list of expense categories"
    )
    expense_filter: Optional[Dict[str, Any]] = Field(
        None, description="Updated expense filter details"
    )
    agent_flow_definition: Optional[List[Dict[str, Any]]] = Field(
        None, description="Updated agent flow configuration"
    )
    related_document_models: Optional[List[str]] = Field(
        None, description="Updated list of related document models"
    )


class ClientWorkflowResponse(BaseModel):
    """Response schema for client workflows"""

    id: str = Field(..., alias="_id", description="MongoDB ObjectId as string")
    name: str = Field(..., description="Name of the client workflow")
    central_workflow_id: Optional[str] = Field(
        None, description="Linked central workflow ID"
    )
    central_module_id: Optional[str] = Field(
        None, description="Linked central module ID"
    )
    description: Optional[str] = Field(None, description="Description of the workflow")
    expense_categories: Optional[List[str]] = Field(
        None, description="List of expense categories"
    )
    expense_filter: Optional[Dict[str, Any]] = Field(
        None, description="Expense filter details"
    )
    agent_flow_definition: Optional[List[Dict[str, Any]]] = Field(
        None, description="Agent flow configuration"
    )
    related_document_models: Optional[List[str]] = Field(
        None, description="List of related document models"
    )
    created_by: Optional[str] = Field(
        None, description="UUID of user who created this workflow"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user who last updated this workflow"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    @field_validator("id", mode="before")
    @classmethod
    def convert_object_id_to_str(cls, v):
        """Convert MongoDB ObjectId to string"""
        return str(v)

    class Config:
        from_attributes = True
        populate_by_name = True


# ==================================== CLIENT RULES ========================================


class ClientRuleCreate(BaseModel):
    """Schema for creating a new client rule"""

    name: str = Field(
        ..., description="Name of the rule", example="Duplicate Invoice Check"
    )
    rule_category: Optional[str] = Field(
        None, description="Category of the rule", example="Validation"
    )
    relevant_agent: Optional[str] = Field(
        None,
        description="Agent responsible for executing this rule",
        example="invoice_validator_agent",
    )
    prompt: Optional[str] = Field(
        None, description="Prompt logic or condition for the rule"
    )
    suggested_resolution: Optional[str] = Field(
        None,
        description="Suggested resolution when rule is violated",
        example="Reject duplicate invoices",
    )
    breach_level: Optional[str] = Field(
        None, description="Severity of breach", example="High"
    )
    linked_tools: Optional[List[str]] = Field(
        default_factory=list,
        description="External tools or models used for rule execution",
        example=["OCRValidator", "DuplicateChecker"],
    )
    resolution_format: Optional[str] = Field(None, example="text")
    client_workflow_id: str = Field(..., description="Associated client workflow ID")
    created_by: Optional[str] = Field(
        None, description="UUID of user creating this rule"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user updating this rule"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "name": "Duplicate Invoice Check",
                "rule_category": "Validation",
                "relevant_agent": "invoice_agent",
                "prompt": "Check if invoice number already exists",
                "suggested_resolution": "Reject duplicate invoices",
                "breach_level": "High",
                "linked_tools": ["OCRValidator"],
                "resolution_format": "text",
                "client_workflow_id": "workflow123",
                "created_by": "user-uuid-123",
                "updated_by": "user-uuid-123",
            }
        }


class ClientRuleUpdate(BaseModel):
    """Schema for updating a client rule"""

    name: Optional[str] = Field(None, description="Updated rule name")
    prompt: Optional[str] = Field(None, description="Updated prompt logic or condition")
    suggested_resolution: Optional[str] = Field(
        None, description="Updated suggested resolution"
    )
    linked_tools: Optional[List[str]] = Field(
        None, description="Updated linked tools or models"
    )
    resolution_format: Optional[str] = Field(
        None, description="Updated resolution format"
    )
    breach_level: Optional[str] = Field(None, description="Updated severity of breach")


class ClientRuleResponse(BaseModel):
    """Response schema for client rules"""

    id: str = Field(..., alias="_id", description="MongoDB ObjectId")
    name: str = Field(..., description="Name of the rule")
    rule_category: Optional[str] = Field(None, description="Category of the rule")
    relevant_agent: Optional[str] = Field(
        None, description="Agent responsible for executing this rule"
    )
    prompt: Optional[str] = Field(
        None, description="Prompt logic or condition for the rule"
    )
    suggested_resolution: Optional[str] = Field(
        None, description="Suggested resolution when rule is violated"
    )
    breach_level: Optional[str] = Field(None, description="Severity of breach")
    linked_tools: Optional[List[str]] = Field(
        None, description="External tools or models used for rule execution"
    )
    resolution_format: Optional[str] = Field(
        None, description="Format of the resolution"
    )
    client_workflow_id: str = Field(..., description="Associated client workflow ID")
    created_by: Optional[str] = Field(
        None, description="UUID of user who created this rule"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user who last updated this rule"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    @field_validator("id", mode="before")
    @classmethod
    def convert_object_id_to_str(cls, v):
        """Convert MongoDB ObjectId to string"""
        return str(v)

    @field_validator("client_workflow_id", mode="before")
    @classmethod
    def convert_link_to_str(cls, v):
        """Convert Beanie Link object to string (ObjectId)"""
        if isinstance(v, str):
            return v
        return str(v.id) if hasattr(v, "id") else str(v)

    class Config:
        from_attributes = True
        populate_by_name = True


# =================================== WORKFLOW EXECUTION LOGS ==========================================


class WorkflowExecutionLogCreate(BaseModel):
    """Schema for creating a workflow execution log"""

    source_trigger: Optional[str] = Field(
        None, description="Trigger source", example="manual_upload"
    )
    context: Optional[Dict[str, Any]] = Field(
        default_factory=dict,
        description="Additional metadata or context information",
        example={"triggered_by": "admin_user"},
    )
    client_workflow_id: str = Field(
        ..., description="Associated client workflow ID", example="workflow123"
    )
    input_files: Optional[List[str]] = Field(
        default_factory=list,
        description="List of input files used for workflow execution",
        example=["invoice_2025.pdf"],
    )
    central_workflow_id: Optional[str] = Field(
        None, description="Central workflow reference ID", example="central_001"
    )
    created_by: Optional[str] = Field(
        None, description="UUID of user who created this log"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user who last updated this log"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "source_trigger": "manual_upload",
                "context": {"triggered_by": "system"},
                "client_workflow_id": "workflow123",
                "input_files": ["invoice_2025.pdf"],
                "central_workflow_id": "central_001",
                "created_by": "user-uuid-123",
                "updated_by": "user-uuid-123",
            }
        }


class WorkflowExecutionLogResponse(BaseModel):
    """Response schema for workflow execution logs"""

    id: str = Field(..., alias="_id", description="MongoDB ObjectId")
    source_trigger: Optional[str] = Field(None, description="Trigger source")
    context: Optional[Dict[str, Any]] = Field(
        None, description="Additional metadata or context information"
    )
    client_workflow_id: str = Field(..., description="Associated client workflow ID")
    input_files: Optional[List[str]] = Field(
        None, description="List of input files used for workflow execution"
    )
    central_workflow_id: Optional[str] = Field(
        None, description="Central workflow reference ID"
    )
    created_by: Optional[str] = Field(
        None, description="UUID of user who created this log"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user who last updated this log"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    @field_validator("id", mode="before")
    @classmethod
    def convert_object_id_to_str(cls, v):
        """Convert MongoDB ObjectId to string"""
        return str(v)

    @field_validator("client_workflow_id", mode="before")
    @classmethod
    def convert_link_to_str(cls, v):
        """Convert Beanie Link object to string (ObjectId)"""
        if isinstance(v, str):
            return v
        return str(v.id) if hasattr(v, "id") else str(v)

    class Config:
        from_attributes = True
        populate_by_name = True


# ==================================== AGENT EXECUTION LOGS ==========================================


class AgentExecutionLogCreate(BaseModel):
    """Schema for creating an agent execution log"""

    workflow_execution_log_id: str = Field(
        ...,
        description="Associated workflow execution log ID",
        example="workflow_log_001",
    )
    workflow_id: Optional[str] = Field(
        None,
        description="Workflow ID that this agent belongs to",
        example="workflow_01",
    )
    agent_id: Optional[str] = Field(
        None,
        description="Unique identifier for the executing agent",
        example="agent_001",
    )
    status: Optional[str] = Field(
        None, description="Execution status", example="success"
    )
    user_output: Optional[str] = Field(
        None,
        description="Readable output message generated by the agent",
        example="Invoice validated successfully",
    )
    error_output: Optional[str] = Field(
        None, description="Error details if the execution failed", example="None"
    )
    process_log: Optional[List[Dict[str, Any]]] = Field(
        default_factory=list,
        description="Step-by-step process log of the agent execution",
        example=[{"step": "validation", "result": "ok"}],
    )
    rule_wise_output: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Detailed rule-level execution results"
    )
    related_document_models: Optional[List[str]] = Field(
        default_factory=list,
        description="List of related document models",
        example=["invoice"],
    )
    user_feedback: Optional[str] = Field(
        None,
        description="Feedback provided by the user on the output",
        example="Looks good",
    )
    suggested_resolution: Optional[str] = Field(
        None,
        description="Recommended next action or resolution",
        example="No action required",
    )
    quick_response_actions: Optional[List[str]] = Field(
        default_factory=list,
        description="List of quick response actions suggested by the system",
        example=["notify_user"],
    )
    resolution_format: Optional[str] = Field(
        None, description="Format of the resolution", example="text"
    )
    created_by: Optional[str] = Field(
        None, description="UUID of user who created this log"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user who last updated this log"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "workflow_execution_log_id": "log001",
                "workflow_id": "wf001",
                "agent_id": "agent001",
                "status": "success",
                "user_output": "Validation passed",
                "error_output": "",
                "process_log": [{"step": "OCR", "status": "done"}],
                "rule_wise_output": {"rule_1": {"passed": True}},
                "related_document_models": ["invoice"],
                "user_feedback": "All good",
                "suggested_resolution": "Proceed to payment",
                "quick_response_actions": ["notify_user"],
                "resolution_format": "text",
                "created_by": "user-uuid-123",
                "updated_by": "user-uuid-123",
            }
        }


class AgentExecutionLogUpdate(BaseModel):
    """Schema for updating an agent execution log"""

    status: Optional[str] = Field(None, description="Updated execution status")
    user_output: Optional[str] = Field(
        None, description="Updated readable output message"
    )
    error_output: Optional[str] = Field(None, description="Updated error details")
    user_feedback: Optional[str] = Field(None, description="Updated user feedback")


class AgentExecutionLogResponse(BaseModel):
    """Response schema for agent execution logs"""

    id: str = Field(..., alias="_id", description="MongoDB ObjectId")
    workflow_execution_log_id: str = Field(
        ..., description="Associated workflow execution log ID"
    )
    workflow_id: Optional[str] = Field(
        None, description="Workflow ID that this agent belongs to"
    )
    agent_id: Optional[str] = Field(
        None, description="Unique identifier for the executing agent"
    )
    status: Optional[str] = Field(None, description="Execution status")
    user_output: Optional[str] = Field(
        None, description="Readable output message generated by the agent"
    )
    error_output: Optional[str] = Field(
        None, description="Error details if the execution failed"
    )
    process_log: Optional[List[Dict[str, Any]]] = Field(
        default_factory=list,
        description="Step-by-step process log of the agent execution",
    )
    rule_wise_output: Optional[Dict[str, Any]] = Field(
        default_factory=dict, description="Detailed rule-level execution results"
    )
    related_document_models: Optional[List[str]] = Field(
        default_factory=list, description="List of related document models"
    )
    user_feedback: Optional[str] = Field(
        None, description="Feedback provided by the user on the output"
    )
    suggested_resolution: Optional[str] = Field(
        None, description="Recommended next action or resolution"
    )
    quick_response_actions: Optional[List[str]] = Field(
        default_factory=list,
        description="List of quick response actions suggested by the system",
    )
    resolution_format: Optional[str] = Field(
        None, description="Format of the resolution"
    )
    created_by: Optional[str] = Field(
        None, description="UUID of user who created this log"
    )
    updated_by: Optional[str] = Field(
        None, description="UUID of user who last updated this log"
    )
    created_at: datetime = Field(..., description="Creation timestamp")
    updated_at: datetime = Field(..., description="Last update timestamp")

    @field_validator("id", mode="before")
    @classmethod
    def convert_object_id_to_str(cls, v):
        """Convert MongoDB ObjectId to string"""
        return str(v)

    @field_validator("workflow_execution_log_id", mode="before")
    @classmethod
    def convert_link_to_str(cls, v):
        """Convert Beanie Link object to string (ObjectId)"""
        if isinstance(v, str):
            return v
        return str(v.id) if hasattr(v, "id") else str(v)

    class Config:
        from_attributes = True
        populate_by_name = True
