"""
Base Response Schema for Uniform API Responses
File: ginthi_agents/client_service/schemas/base_response.py
"""

from pydantic import BaseModel
from typing import Optional, Any


class APIResponse(BaseModel):
    """
    Standard API Response Schema
    
    Example Success:
        {
            "success": true,
            "message": "Client created successfully",
            "data": {...}
        }
    
    Example Error:
        {
            "success": false,
            "message": "Client not found",
            "data": null
        }
    """
    success: bool
    message: Optional[str] = None
    data: Optional[Any] = None