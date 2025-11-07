from sqlalchemy import Column, String, DateTime, Text
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import UUID
from datetime import datetime, timezone
import uuid
from client_service.db.postgres_db import Base


class ExpenseMaster(Base):
    __tablename__ = "expense_master"

    expense_category_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    category_name = Column(String(50), nullable=False)
    sub_category_name = Column(String(50), nullable=False)
    module_name = Column(String(50), nullable=False)
    description = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))

    # Relationships
    items = relationship("ItemMaster", back_populates="expense_category")
    vendor_classifications = relationship("VendorClassification", back_populates="expense_category", cascade="all, delete-orphan")