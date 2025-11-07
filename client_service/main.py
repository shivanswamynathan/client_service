import os

import uvicorn
from client_service.api.routes.routes import api_router
from client_service.utils import register_exception_handlers, setup_logging
from client_service.utils.lifespan import lifespan
from client_service.utils.middlewares.middleware_manager import setup_middlewares
from client_service.utils.security import security_dependency
from dotenv import load_dotenv
from fastapi import Depends, FastAPI
from fastapi.openapi.utils import get_openapi
from fastapi_mcp import FastApiMCP

load_dotenv()


# Setup logging
setup_logging(
    log_level=os.getenv("LOG_LEVEL", "INFO"), log_file=os.getenv("LOG_FILE", None)
)

# Create FastAPI application
app = FastAPI(
    title="Client Service API",
    version="1.0.0",
    description="PostgreSQL-based Client Service API",
    lifespan=lifespan,
    dependencies=[Depends(security_dependency)],
)

# Register exception handlers
register_exception_handlers(app)

# Register all middlewares in one go
setup_middlewares(app)

# Include all API routes
app.include_router(api_router)


# 👇 THIS PART makes the "Authorize" button appear
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    openapi_schema["components"]["securitySchemes"] = {
        "BearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
        }
    }
    openapi_schema["security"] = [{"BearerAuth": []}]
    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi  # ✅ attach our schema override


# Create MCP server and mount it (without authentication for open access)
mcp = FastApiMCP(app, name="Client Service MCP")
mcp.mount_http()


@app.get("/health-check", tags=["Health Check"])
async def health_check():
    return {
        "success": True,
        "message": "Service is healthy",
        "data": {"status": "healthy", "service": "Client Service API"},
    }


def main():
    """
    Main function to run the application with uvicorn.
    """
    uvicorn.run(
        "client_service.main:app",
        host=os.getenv("HOST", "0.0.0.0"),
        port=int(os.getenv("PORT", 8005)),
        reload=os.getenv("RELOAD", "true").lower() == "true",
        log_level=os.getenv("LOG_LEVEL", "info").lower(),
        workers=int(os.getenv("WORKERS", 1)),
    )


if __name__ == "__main__":
    main()
