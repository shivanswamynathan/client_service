import os
import motor.motor_asyncio
from beanie import init_beanie
from dotenv import load_dotenv
from client_service.schemas.mongo_schemas.client_schema_model import ClientSchema
from client_service.schemas.mongo_schemas.client_workflow_execution import (
    ClientWorkflows,
    ClientRules,
    WorkflowExecutionLogs,
    AgentExecutionLogs
)
import logging

load_dotenv()

logger = logging.getLogger(__name__)

# MongoDB configuration
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB")

# Async MongoDB client
client = motor.motor_asyncio.AsyncIOMotorClient(MONGO_URI)
db = client[MONGO_DB]


async def init_db():
    """
    Initialize Beanie with all document models.
    Ensures each model uses its own collection as defined in Settings.name
    """
    document_models = [
        ClientSchema,
        ClientWorkflows,
        ClientRules,
        WorkflowExecutionLogs,
        AgentExecutionLogs
    ]

    logger.info(f"Initializing Beanie with {len(document_models)} models:")
    for model in document_models:
        collection_name = getattr(model.Settings, "name", model.__name__.lower())
        logger.info(f"  - {model.__name__} -> collection: {collection_name}")

    # Initialize Beanie
    await init_beanie(database=db, document_models=document_models)
    logger.info("Beanie initialization complete")
    return db


async def get_db():
    """Async: Get database instance"""
    return db


def get_mongo_db():
    """Sync wrapper to get database instance"""
    return db
