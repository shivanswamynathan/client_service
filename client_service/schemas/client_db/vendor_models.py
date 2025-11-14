from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Boolean, Date, Numeric, Text
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import JSONB, UUID
from datetime import datetime, timezone
import uuid
from client_service.db.postgres_db import Base
from client_service.schemas.client_db.client_models import ClientEntity
from client_service.schemas.client_db.expense_models import ExpenseMaster


class VendorMaster(Base):
    __tablename__ = "vendor_master"

    vendor_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    vendor_code = Column(String(50), nullable=False, unique=True)
    vendor_name = Column(String(255), nullable=False)
    email = Column(String(255), nullable=True)
    gst_id = Column(String(15), nullable=True)
    company_pan = Column(String(10), nullable=True)
    tan = Column(String(10), nullable=True)
    bank_acc_no = Column(String(20), nullable=True)
    beneficiary_name = Column(String(255), nullable=True)
    acc_verified = Column(Boolean, default=False)
    ifsc_code = Column(String(11), nullable=True)
    payment_term_days = Column(Integer, nullable=True)
    user_phone = Column(String(15), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    transactions = relationship("VendorTransactions", back_populates="vendor", cascade="all, delete-orphan")
    classifications = relationship("VendorClassification", back_populates="vendor", cascade="all, delete-orphan")

class VendorClassification(Base):
    __tablename__ = "vendor_classification"

    vendor_classification_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True) 
    client_entity_id = Column(UUID(as_uuid=True), ForeignKey("client_entity.entity_id"), primary_key=True, index=True)
    expense_category_id = Column(UUID(as_uuid=True), ForeignKey("expense_master.expense_category_id"), primary_key=True, index=True)
    vendor_id = Column(UUID(as_uuid=True), ForeignKey("vendor_master.vendor_id"), primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    client_entity = relationship("ClientEntity", back_populates="vendor_classifications")
    expense_category = relationship("ExpenseMaster", back_populates="vendor_classifications")
    vendor = relationship("VendorMaster", back_populates="classifications")


class VendorTransactions(Base):
    __tablename__ = "vendor_transactions"

    transaction_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    vendor_id = Column(UUID(as_uuid=True), ForeignKey("vendor_master.vendor_id"), nullable=False, index=True)
    invoice_id = Column(String(50), nullable=False, unique=True)
    client_entity_id = Column(UUID(as_uuid=True), ForeignKey("client_entity.entity_id"), nullable=False, index=True)
    transaction_date = Column(Date, nullable=False)
    transaction_type = Column(String(50), nullable=False)
    amount = Column(Numeric(18, 4), nullable=False)
    currency = Column(String(4), default="INR")
    description = Column(Text, nullable=True)
    notes = Column(Text, nullable=True)
    status = Column(Integer, nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    vendor = relationship("VendorMaster", back_populates="transactions")
    client_entity = relationship("ClientEntity", back_populates="transactions")
    transaction_logs = relationship("TransactionLog", back_populates="transaction", cascade="all, delete-orphan")


class ActionLog(Base):
    __tablename__ = "action_log"

    log_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    status = Column(Integer, nullable=False)
    action = Column(JSONB, nullable=False)
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction_logs = relationship("TransactionLog", back_populates="action_log")


class TransactionLog(Base):
    __tablename__ = "transaction_log"

    log_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    transaction_id = Column(UUID(as_uuid=True), ForeignKey("vendor_transactions.transaction_id"), nullable=False, index=True)
    action = Column(JSONB, nullable=False)
    approval_time = Column(DateTime(timezone=True), nullable=True)
    action_log_id = Column(UUID(as_uuid=True), ForeignKey("action_log.log_id"), nullable=True, index=True)
    user_log_id = Column(UUID(as_uuid=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    transaction = relationship("VendorTransactions", back_populates="transaction_logs")
    action_log = relationship("ActionLog", back_populates="transaction_logs")