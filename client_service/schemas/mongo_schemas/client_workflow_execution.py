from beanie import Document, Link
from pydantic import BaseModel, Field, field_validator
from typing import Optional, List, Any, Dict
from datetime import datetime, timezone
import uuid
import logging

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# CLIENT_WORKFLOWS
# ─────────────────────────────────────────────

class ClientWorkflows(Document):

    name: str = Field(..., description="Name of the client workflow")
    central_workflow_id: Optional[str] = Field(None, description="Reference to central workflow ID")
    central_module_id: Optional[str] = Field(None, description="Reference to central module ID")
    description: Optional[str] = Field(None, description="Workflow description")
    expense_categories: Optional[List[str]] = Field(default_factory=list, description="List of expense categories")
    expense_filter: Optional[Dict[str, Any]] = Field(None, description="Expense filter conditions")
    agent_flow_definition: Optional[List[Dict[str, Any]]] = Field(default_factory=list, description="Definition of agent flow")
    related_document_models: Optional[List[str]] = Field(default_factory=list, description="List of related document model IDs")
    created_by: Optional[str] = Field(None, description="User who created the workflow")
    updated_by: Optional[str] = Field(None, description="User who last updated the workflow")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


    class Settings:
        name = "client_workflows"  # Collection name for client workflows


# ─────────────────────────────────────────────
# CLIENT_RULES
# ─────────────────────────────────────────────

class ClientRules(Document):

    client_workflow_id: Link[ClientWorkflows]
    name: str = Field(..., description="Rule name")
    rule_category: Optional[str] = Field(None, description="Category of the rule")
    relevant_agent: Optional[str] = Field(None, description="Relevant agent name or ID")
    prompt: Optional[str] = Field(None, description="Prompt for the rule logic")
    suggested_resolution: Optional[str] = Field(None, description="Suggested resolution message")
    breach_level: Optional[str] = Field(None, description="Severity or breach level")
    linked_tools: Optional[List[str]] = Field(default_factory=list, description="List of linked tool names or IDs")
    resolution_format: Optional[str] = Field(None, description="Expected output or resolution format")
    created_by: Optional[str] = Field(None, description="User who created the rule")
    updated_by: Optional[str] = Field(None, description="User who last updated the rule")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Settings:
        name = "client_rules"  # Collection name for client rules


# ─────────────────────────────────────────────
# WORKFLOW_EXECUTION_LOGS
# ─────────────────────────────────────────────

class WorkflowExecutionLogs(Document):
    
    source_trigger: Optional[str] = Field(None, description="Source that triggered the workflow")
    context: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Contextual data for execution")
    client_workflow_id: Link[ClientWorkflows] = Field(..., description="Associated client workflow ID")
    input_files: Optional[List[str]] = Field(default_factory=list, description="Input files or document references")
    central_workflow_id: Optional[str] = Field(None, description="Reference to central workflow ID")
    created_by: Optional[str] = Field(None, description="User who initiated the execution")
    updated_by: Optional[str] = Field(None, description="User who last updated the execution log")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Settings:
        name = "workflow_execution_logs"  # Collection name for workflow execution logs


# ─────────────────────────────────────────────
# AGENT_EXECUTION_LOGS
# ─────────────────────────────────────────────

class AgentExecutionLogs(Document):

    workflow_execution_log_id: Link[WorkflowExecutionLogs]
    workflow_id: Optional[str] = Field(None, description="Workflow ID reference")
    agent_id: Optional[str] = Field(..., description="Agent unique identifier")
    status: Optional[str] = Field(None, description="Execution status (success, failed, pending, etc.)")
    user_output: Optional[str] = Field(None, description="Readable output message for users")
    error_output: Optional[str] = Field(None, description="Error details if execution failed")
    process_log: Optional[List[Dict[str, Any]]] = Field(default_factory=list, description="Step-by-step execution log")
    related_document_models: Optional[List[str]] = Field(default_factory=list, description="List of related document model IDs")
    rule_wise_output: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Detailed rule-level execution results")
    user_feedback: Optional[str] = Field(None, description="Feedback provided by the user on the output",example="Looks good")
    suggested_resolution: Optional[str] = Field(None, description="Recommended next action or resolution",example="No action required")
    quick_response_actions: Optional[List[str]] = Field(default_factory=list,description="List of quick response actions suggested by the system", example=["notify_user"])
    resolution_format: Optional[str] = Field(None,description="Format of the resolution", example="text")
    created_by: Optional[str] = Field(None, description="User who created the log")
    updated_by: Optional[str] = Field(None, description="User who last updated the log")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    class Settings:
        name = "agent_execution_logs"  # Collection name for agent execution logs