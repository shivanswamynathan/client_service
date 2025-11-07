from sqlalchemy import Column, Integer, String, DateTime, ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime, timezone
import uuid
from client_service.db.postgres_db import Base


class CentralClients(Base):
    __tablename__ = "central_clients"

    central_client_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    name = Column(String(255), nullable=False)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    clients = relationship("Clients", back_populates="central_client")


class Clients(Base):
    __tablename__ = "clients"

    client_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    client_name = Column(String(255), nullable=False, unique=True)
    central_client_id = Column(UUID(as_uuid=True), ForeignKey("central_clients.central_client_id"), nullable=True)
    central_api_key = Column(String(512), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    central_client = relationship("CentralClients", back_populates="clients")
    entities = relationship("ClientEntity", back_populates="client", cascade="all, delete-orphan")
    users = relationship("Users", back_populates="client", cascade="all, delete-orphan")
    workflows = relationship("WorkflowRequestLedger", back_populates="client", cascade="all, delete-orphan")


class ClientEntity(Base):
    __tablename__ = "client_entity"

    entity_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    client_id = Column(UUID(as_uuid=True), ForeignKey("clients.client_id"), nullable=False, index=True)
    gst_id = Column(String(15), nullable=True)
    company_pan = Column(String(10), nullable=True)
    entity_name = Column(String(255), nullable=False)
    tan = Column(String(10), nullable=True)
    parent_client_id = Column(UUID(as_uuid=True), nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    client = relationship("Clients", back_populates="entities")
    transactions = relationship("VendorTransactions", back_populates="client_entity", cascade="all, delete-orphan")
    vendor_classifications = relationship("VendorClassification", back_populates="client_entity", cascade="all, delete-orphan")