"""
Middleware Manager - Centralized registration for all middlewares
"""

from fastapi import FastAPI

# Import all middleware classes and setup functions
# from .auth_middleware import AuthMiddleware  # Temporarily commented out
from .cors_middleware import add_cors_middleware


def setup_middlewares(app: FastAPI) -> None:
    """
    Registers all middleware in a clean and ordered way.
    Order matters — Logging first, then timing, error handling, etc.
    """

    # CORS should be added first
    add_cors_middleware(app)

    # Validate JWT and enforce authentication rules
    # app.add_middleware(AuthMiddleware)  # Temporarily commented out


__all__ = ["setup_middlewares"]
