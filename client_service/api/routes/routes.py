from fastapi import APIRouter
from . import (  # Import from the current package (api/routes/)
    central_clients_router, clients_router, entities_router, items_router,
    logs_router, permissions_router, role_permissions_router, roles_router,
    transactions_router, user_roles_router, users_router, vendors_router, workflows_router, expenses_router, vendor_classification_router,client_schema_router
)
from .openapi_router import router as openapi_router
from .client_schema_router import router as client_schema_router
from .documents_router import router as documents_router
from .client_workflow_router import router as client_workflows_router
from .client_rules_router import router as client_rules_router
from .workflow_executionlog_router import router as workflow_executionlog_router
from .agent_executionlog_router import router as agent_logs_router


api_router = APIRouter()

# Include all routers
api_router.include_router(openapi_router, prefix="/api/v1",tags=["API Documentation"])
api_router.include_router(clients_router, prefix="/api/v1", tags=["Clients"])
api_router.include_router(central_clients_router, prefix="/api/v1", tags=["Central Clients"])
api_router.include_router(entities_router, prefix="/api/v1", tags=["Client Entities"])
api_router.include_router(users_router, prefix="/api/v1", tags=["Users"])
api_router.include_router(roles_router, prefix="/api/v1", tags=["Roles"])
api_router.include_router(permissions_router, prefix="/api/v1", tags=["Permissions"])
api_router.include_router(user_roles_router, prefix="/api/v1", tags=["User Roles"])
api_router.include_router(role_permissions_router, prefix="/api/v1", tags=["Role Permissions"])
api_router.include_router(vendors_router, prefix="/api/v1", tags=["Vendors"])
api_router.include_router(transactions_router, prefix="/api/v1", tags=["Transactions"])
api_router.include_router(items_router, prefix="/api/v1", tags=["Items"])
api_router.include_router(expenses_router, prefix="/api/v1", tags=["Expenses"])
api_router.include_router(vendor_classification_router, prefix="/api/v1", tags=["Vendor Classifications"])
api_router.include_router(workflows_router, prefix="/api/v1", tags=["Workflows"])
api_router.include_router(logs_router, prefix="/api/v1", tags=["Logs"])
api_router.include_router(client_schema_router, prefix="/api/v1", tags=["Client Schemas"])
api_router.include_router(documents_router, prefix="/api/v1", tags=["Dynamic Documents"])
api_router.include_router(client_workflows_router, prefix="/api/v1/client_workflow", tags=["Client Workflows"])
api_router.include_router(client_rules_router, prefix="/api/v1/client_rules", tags=["Client Rules"])
api_router.include_router(workflow_executionlog_router, prefix="/api/v1/workflow_executionlog", tags=["Workflow Execution Logs"])
api_router.include_router(agent_logs_router, prefix="/api/v1/agent_executionlog", tags=["Agent Execution Logs"])