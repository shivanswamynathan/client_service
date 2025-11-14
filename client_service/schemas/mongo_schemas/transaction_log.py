from datetime import datetime
from typing import Optional, Any, Dict
from pydantic import BaseModel, Field


class TransactionLogModel(BaseModel):
    user: Optional[str] = Field(default="anonymous")
    ip: Optional[str] = None
    method: Optional[str] = None
    path: Optional[str] = None
    status_code: Optional[int] = None
    duration_ms: Optional[float] = None
    headers: Optional[Dict[str, Any]] = None
    request_body: Optional[Dict[str, Any]] = None
    response_body: Optional[Dict[str, Any]] = None
    service_name: Optional[str] = Field(default="client_service")
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    class Config:
        orm_mode = True
    class Settings:
        name = "transactions_logs"     