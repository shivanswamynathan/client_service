"""
Constants file for all API response messages.
Similar to Django's approach of centralizing messages.
"""


class CentralClientMessages:
    """Messages for Central Client operations"""
    
    # Success messages
    CREATED_SUCCESS = "Central client created successfully: {name}"
    RETRIEVED_SUCCESS = "Central client retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} central clients"
    UPDATED_SUCCESS = "Central client updated: {name}"
    DELETED_SUCCESS = "Central client {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Central client with ID {id} not found"
    CREATE_ERROR = "Error creating central client: {error}"
    RETRIEVE_ERROR = "Error retrieving central client: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving central clients: {error}"
    UPDATE_ERROR = "Error updating central client: {error}"
    DELETE_ERROR = "Error deleting central client: {error}"


class ClientMessages:
    """Messages for Client operations"""
    
    # Success messages
    CREATED_SUCCESS = "Client created successfully: {name}"
    RETRIEVED_SUCCESS = "Client retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} clients"
    UPDATED_SUCCESS = "Client updated: {name}"
    DELETED_SUCCESS = "Client {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Client with ID {id} not found"
    DUPLICATE_NAME = "Client with name '{name}' already exists"
    CREATE_ERROR = "Error creating client: {error}"
    RETRIEVE_ERROR = "Error retrieving client: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving clients: {error}"
    UPDATE_ERROR = "Error updating client: {error}"
    DELETE_ERROR = "Error deleting client: {error}"


class EntityMessages:
    """Messages for Client Entity operations"""
    
    # Success messages
    CREATED_SUCCESS = "Entity created successfully: {name}"
    RETRIEVED_SUCCESS = "Entity retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} entities"
    RETRIEVED_BY_CLIENT_SUCCESS = "Retrieved {count} entities for client {id}"
    SEARCH_SUCCESS = "Found {count} entities where {column} matches '{value}'"
    NO_SEARCH_RESULTS = "No entities found where {column} matches '{value}'"
    UPDATED_SUCCESS = "Entity updated: {name}"
    DELETED_SUCCESS = "Entity {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Entity with ID {id} not found"
    CLIENT_NOT_FOUND = "Client with ID {id} not found"
    NO_ENTITIES_FOR_CLIENT = "No entities found for client {id}"
    INVALID_SEARCH_COLUMN = "Invalid search column: {column}. Allowed columns: {allowed}"
    CREATE_ERROR = "Error creating entity: {error}"
    RETRIEVE_ERROR = "Error retrieving entity: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving entities: {error}"
    SEARCH_ERROR = "Error searching entities: {error}"
    UPDATE_ERROR = "Error updating entity: {error}"
    DELETE_ERROR = "Error deleting entity: {error}"


class UserMessages:
    """Messages for User operations"""
    
    # Success messages
    CREATED_SUCCESS = "User created successfully: {name}"
    RETRIEVED_SUCCESS = "User retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} users"
    UPDATED_SUCCESS = "User updated: {name}"
    DELETED_SUCCESS = "User {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "User with ID {id} not found"
    DUPLICATE_EMAIL = "User with email '{email}' already exists"
    REPORTING_MANAGER_NOT_FOUND = "Reporting manager role with ID {role_id} not found"
    CREATE_ERROR = "Error creating user: {error}"
    RETRIEVE_ERROR = "Error retrieving user: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving users: {error}"
    UPDATE_ERROR = "Error updating user: {error}"
    DELETE_ERROR = "Error deleting user: {error}"


class RoleMessages:
    """Messages for Role operations"""
    
    # Success messages
    CREATED_SUCCESS = "Role created successfully: {name}"
    RETRIEVED_SUCCESS = "Role retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} roles"
    UPDATED_SUCCESS = "Role updated: {name}"
    DELETED_SUCCESS = "Role {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Role with ID {id} not found"
    DUPLICATE_NAME = "Role with name '{name}' already exists"
    CREATE_ERROR = "Error creating role: {error}"
    RETRIEVE_ERROR = "Error retrieving role: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving roles: {error}"
    UPDATE_ERROR = "Error updating role: {error}"
    DELETE_ERROR = "Error deleting role: {error}"


class PermissionMessages:
    """Messages for Permission operations"""
    
    # Success messages
    CREATED_SUCCESS = "Permission created successfully: {name}"
    RETRIEVED_SUCCESS = "Permission retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} permissions"
    UPDATED_SUCCESS = "Permission updated: {name}"
    DELETED_SUCCESS = "Permission {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Permission with ID {id} not found"
    DUPLICATE_NAME = "Permission with name '{name}' already exists"
    CREATE_ERROR = "Error creating permission: {error}"
    RETRIEVE_ERROR = "Error retrieving permission: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving permissions: {error}"
    UPDATE_ERROR = "Error updating permission: {error}"
    DELETE_ERROR = "Error deleting permission: {error}"


class VendorMessages:
    """Messages for Vendor operations"""
    
    # Success messages
    CREATED_SUCCESS = "Vendor created successfully: {name}"
    RETRIEVED_SUCCESS = "Vendor retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} vendors"
    SEARCH_SUCCESS = "Found {count} vendors where {column} matches '{value}'"
    NO_SEARCH_RESULTS = "No vendors found where {column} matches '{value}'"
    UPDATED_SUCCESS = "Vendor updated: {name}"
    DELETED_SUCCESS = "Vendor {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Vendor with ID {id} not found"
    DUPLICATE_CODE = "Vendor with code '{code}' already exists"
    CREATE_ERROR = "Error creating vendor: {error}"
    INVALID_SEARCH_COLUMN = "Invalid search column: {column}. Allowed columns: {allowed}"
    RETRIEVE_ERROR = "Error retrieving vendor: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving vendors: {error}"
    SEARCH_ERROR = "Error searching vendors: {error}"
    UPDATE_ERROR = "Error updating vendor: {error}"
    DELETE_ERROR = "Error deleting vendor: {error}"


class TransactionMessages:
    """Messages for Transaction operations"""
    
    # Success messages
    CREATED_SUCCESS = "Transaction created successfully: {invoice}"
    RETRIEVED_SUCCESS = "Transaction retrieved: {invoice}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} transactions"
    RETRIEVED_BY_VENDOR_SUCCESS = "Retrieved {count} transactions for vendor {id}"
    UPDATED_SUCCESS = "Transaction updated: {invoice}"
    DELETED_SUCCESS = "Transaction {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Transaction with ID {id} not found"
    VENDOR_NOT_FOUND = "Vendor with ID {id} not found"
    DUPLICATE_INVOICE = "Transaction with InvoiceID '{invoice}' already exists"
    NO_TRANSACTIONS_FOR_VENDOR = "No transactions found for vendor {id}"
    CREATE_ERROR = "Error creating transaction: {error}"
    RETRIEVE_ERROR = "Error retrieving transaction: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving transactions: {error}"
    UPDATE_ERROR = "Error updating transaction: {error}"
    DELETE_ERROR = "Error deleting transaction: {error}"


class ItemMessages:
    """Messages for Item operations"""
    
    # Success messages
    CREATED_SUCCESS = "Item created successfully: {name}"
    RETRIEVED_SUCCESS = "Item retrieved: {name}"
    RETRIEVED_BY_CODE_SUCCESS = "Item retrieved by code: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} items"
    UPDATED_SUCCESS = "Item updated: {name}"
    DELETED_SUCCESS = "Item {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Item with ID {id} not found"
    NOT_FOUND_BY_CODE = "Item with code '{code}' not found"
    DUPLICATE_CODE = "Item with code '{code}' already exists"
    CREATE_ERROR = "Error creating item: {error}"
    RETRIEVE_ERROR = "Error retrieving item: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving items: {error}"
    UPDATE_ERROR = "Error updating item: {error}"
    DELETE_ERROR = "Error deleting item: {error}"

class ExpenseCategoryMessages:
    """Messages for Expense Category operations"""
    
    # Success messages
    CREATED_SUCCESS = "Expense category created successfully: {name}"
    RETRIEVED_SUCCESS = "Expense category retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} expense categories"
    UPDATED_SUCCESS = "Expense category updated: {name}"
    DELETED_SUCCESS = "Expense category {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Expense category with ID {id} not found"
    DUPLICATE_NAME = "Expense category with name '{name}' already exists"
    CREATE_ERROR = "Error creating expense category: {error}"
    RETRIEVE_ERROR = "Error retrieving expense category: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving expense categories: {error}"
    UPDATE_ERROR = "Error updating expense category: {error}"
    DELETE_ERROR = "Error deleting expense category: {error}"


class VendorClassificationMessages:
    """Messages for Vendor Classification operations"""
    
    # Success messages
    CREATED_SUCCESS = "Vendor classification created: Vendor {vendor_name} for category {category_name}"
    RETRIEVED_SUCCESS = "Vendor classification retrieved: Vendor {vendor_name} for category {category_name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} vendor classifications"
    UPDATED_SUCCESS = "Vendor classification updated"
    DELETED_SUCCESS = "Vendor classification deleted: Vendor {vendor_id} for category {category_id}"
    
    # Error messages
    NOT_FOUND = "Vendor classification with keys (entity_id={entity_id}, category_id={category_id}, vendor_id={vendor_id}) not found"
    DUPLICATE_CLASSIFICATION = "Vendor classification already exists for these keys"
    CLIENT_ENTITY_NOT_FOUND = "Client entity with ID {entity_id} not found"
    CATEGORY_NOT_FOUND = "Expense category with ID {category_id} not found"
    VENDOR_NOT_FOUND = "Vendor with ID {vendor_id} not found"
    CREATE_ERROR = "Error creating vendor classification: {error}"
    RETRIEVE_ERROR = "Error retrieving vendor classification: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving vendor classifications: {error}"
    UPDATE_ERROR = "Error updating vendor classification: {error}"
    DELETE_ERROR = "Error deleting vendor classification: {error}"

class WorkflowMessages:
    """Messages for Workflow operations"""
    
    # Success messages
    CREATED_SUCCESS = "Workflow ledger created successfully: {name}"
    RETRIEVED_SUCCESS = "Workflow ledger retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} workflow ledgers"
    RETRIEVED_BY_CLIENT_SUCCESS = "Retrieved {count} workflow ledgers for client {id}"
    RETRIEVED_BY_USER_SUCCESS = "Retrieved {count} workflow ledgers for user {id}"
    UPDATED_SUCCESS = "Workflow ledger updated: {name}"
    DELETED_SUCCESS = "Workflow ledger {id} deleted successfully"
    REQUEST_INCREMENTED = "Request count incremented"
    
    # Error messages
    NOT_FOUND = "Workflow ledger with ID {id} not found"
    CLIENT_NOT_FOUND = "Client with ID {id} not found"
    USER_NOT_FOUND = "User with ID {id} not found"
    NO_WORKFLOWS_FOR_CLIENT = "No workflow ledgers found for client {id}"
    NO_WORKFLOWS_FOR_USER = "No workflow ledgers found for user {id}"
    CREATE_ERROR = "Error creating workflow ledger: {error}"
    RETRIEVE_ERROR = "Error retrieving workflow ledger: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving workflow ledgers: {error}"
    UPDATE_ERROR = "Error updating workflow ledger: {error}"
    DELETE_ERROR = "Error deleting workflow ledger: {error}"
    INCREMENT_ERROR = "Error incrementing workflow request: {error}"


class UserRoleMessages:
    """Messages for User Role operations"""
    
    # Success messages
    ASSIGNED_SUCCESS = "Role {role_name} assigned to user {user_name}"
    RETRIEVED_USER_ROLES_SUCCESS = "Retrieved {count} roles for user {id}"
    RETRIEVED_ROLE_USERS_SUCCESS = "Retrieved {count} users with role {id}"
    REMOVED_SUCCESS = "Role {role_id} removed from user {user_id} successfully"
    
    # Error messages
    USER_NOT_FOUND = "User with ID {id} not found"
    ROLE_NOT_FOUND = "Role with ID {id} not found"
    ALREADY_ASSIGNED = "User {user_id} already has role {role_id}"
    ASSIGNMENT_NOT_FOUND = "Role assignment not found for user {user_id} and role {role_id}"
    NO_ROLES_FOR_USER = "No roles found for user {id}"
    NO_USERS_FOR_ROLE = "No users found with role {id}"
    ASSIGN_ERROR = "Error assigning role to user: {error}"
    RETRIEVE_ERROR = "Error retrieving user roles: {error}"
    REMOVE_ERROR = "Error removing role from user: {error}"


class RolePermissionMessages:
    """Messages for Role Permission operations"""
    
    # Success messages
    ASSIGNED_SUCCESS = "Permission {permission_name} assigned to role {role_name}"
    RETRIEVED_ROLE_PERMISSIONS_SUCCESS = "Retrieved {count} permissions for role {id}"
    RETRIEVED_PERMISSION_ROLES_SUCCESS = "Retrieved {count} roles with permission {id}"
    REMOVED_SUCCESS = "Permission {permission_id} removed from role {role_id} successfully"
    
    # Error messages
    ROLE_NOT_FOUND = "Role with ID {id} not found"
    PERMISSION_NOT_FOUND = "Permission with ID {id} not found"
    ALREADY_ASSIGNED = "Role {role_id} already has permission {permission_id}"
    ASSIGNMENT_NOT_FOUND = "Permission assignment not found for role {role_id} and permission {permission_id}"
    NO_PERMISSIONS_FOR_ROLE = "No permissions found for role {id}"
    NO_ROLES_FOR_PERMISSION = "No roles found with permission {id}"
    ASSIGN_ERROR = "Error assigning permission to role: {error}"
    RETRIEVE_ERROR = "Error retrieving role permissions: {error}"
    REMOVE_ERROR = "Error removing permission from role: {error}"


class LogMessages:
    """Messages for Log operations"""
    
    # Success messages
    ACTION_LOG_CREATED = "Action log created successfully: LogId {id}"
    TRANSACTION_LOG_CREATED = "Transaction log created successfully: LogID {id}"
    USER_LOG_CREATED = "User log created successfully: LogId {id}"
    LOG_RETRIEVED = "Log retrieved: {id}"
    LOGS_RETRIEVED = "Retrieved {count} logs"
    LOGS_BY_TRANSACTION_RETRIEVED = "Retrieved {count} logs for transaction {id}"
    LOGS_BY_USER_RETRIEVED = "Retrieved {count} logs for user {id}"
    
    # Error messages
    LOG_NOT_FOUND = "Log with ID {id} not found"
    NO_LOGS_FOR_TRANSACTION = "No logs found for transaction {id}"
    NO_LOGS_FOR_USER = "No logs found for user {id}"
    CREATE_ERROR = "Error creating log: {error}"
    RETRIEVE_ERROR = "Error retrieving log: {error}"


class ClientSchemaMessages:
    """Messages for Client Schema operations"""
    
    # Success messages
    CREATED_SUCCESS = "Client schema created successfully: {name} v{version}"
    RETRIEVED_SUCCESS = "Client schema retrieved: {name} v{version}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} client schemas"
    RETRIEVED_BY_CLIENT_SUCCESS = "Retrieved {count} schemas for client {id}"
    RETRIEVED_ACTIVE_SUCCESS = "Retrieved active schema: {name} v{version}"
    UPDATED_SUCCESS = "Client schema updated: {name} v{version}"
    ACTIVATED_SUCCESS = "Schema version activated: {name} v{version}"
    DELETED_SUCCESS = "Client schema {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Client schema with ID {id} not found"
    NOT_FOUND_BY_NAME = "Schema '{name}' not found for client {client_id}"
    NO_ACTIVE_VERSION = "No active version found for schema '{name}' and client {client_id}"
    NO_SCHEMAS_FOR_CLIENT = "No schemas found for client {id}"
    DUPLICATE_SCHEMA = "Schema '{name}' v{version} already exists for client {client_id}"
    CREATE_ERROR = "Error creating client schema: {error}"
    RETRIEVE_ERROR = "Error retrieving client schema: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving client schemas: {error}"
    UPDATE_ERROR = "Error updating client schema: {error}"
    ACTIVATE_ERROR = "Error activating schema version: {error}"
    DELETE_ERROR = "Error deleting client schema: {error}"

class ClientWorkflowMessages:
    """Messages for Client Workflow operations"""
    
    # Success messages
    CREATED_SUCCESS = "Client workflow created successfully: {name}"
    RETRIEVED_SUCCESS = "Client workflow retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} client workflows"
    RETRIEVED_BY_CLIENT_SUCCESS = "Retrieved {count} workflows for client {id}"
    UPDATED_SUCCESS = "Client workflow updated: {name}"
    ACTIVATED_SUCCESS = "Client workflow activated: {name}"
    DELETED_SUCCESS = "Client workflow {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Client workflow with ID {id} not found"
    CREATE_ERROR = "Error creating client workflow: {error}"
    RETRIEVE_ERROR = "Error retrieving client workflow: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving client workflows: {error}"
    UPDATE_ERROR = "Error updating client workflow: {error}"
    DELETE_ERROR = "Error deleting client workflow: {error}"

class ClientRuleMessages:
    """Messages for Client Rule operations"""
    
    # Success messages
    CREATED_SUCCESS = "Client rule created successfully: {name}"
    RETRIEVED_SUCCESS = "Client rule retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} client rules"
    RETRIEVED_BY_CLIENT_SUCCESS = "Retrieved {count} rules for client {id}"
    UPDATED_SUCCESS = "Client rule updated: {name}"
    DELETED_SUCCESS = "Client rule {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Client rule with ID {id} not found"
    CREATE_ERROR = "Error creating client rule: {error}"
    RETRIEVE_ERROR = "Error retrieving client rule: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving client rules: {error}"
    UPDATE_ERROR = "Error updating client rule: {error}"
    DELETE_ERROR = "Error deleting client rule: {error}"

class WorkflowExecutionLogMessages:
    """Messages for Workflow Execution Log operations"""
    
    # Success messages
    CREATED_SUCCESS = "Workflow execution log created successfully: {name}"
    RETRIEVED_SUCCESS = "Workflow execution log retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} workflow execution logs"
    UPDATED_SUCCESS = "Workflow execution log updated: {name}"
    DELETED_SUCCESS = "Workflow execution log {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Workflow execution log with ID {id} not found"
    CREATE_ERROR = "Error creating workflow execution log: {error}"
    RETRIEVE_ERROR = "Error retrieving workflow execution log: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving workflow execution logs: {error}"
    UPDATE_ERROR = "Error updating workflow execution log: {error}"
    DELETE_ERROR = "Error deleting workflow execution log: {error}"

class AgentExecutionLogMessages:
    """Messages for Agent Execution Log operations"""
    
    # Success messages
    CREATED_SUCCESS = "Agent execution log created successfully: {name}"
    RETRIEVED_SUCCESS = "Agent execution log retrieved: {name}"
    RETRIEVED_ALL_SUCCESS = "Retrieved {count} agent execution logs"
    UPDATED_SUCCESS = "Agent execution log updated: {name}"
    DELETED_SUCCESS = "Agent execution log {id} deleted successfully"
    
    # Error messages
    NOT_FOUND = "Agent execution log with ID {id} not found"
    CREATE_ERROR = "Error creating agent execution log: {error}"
    RETRIEVE_ERROR = "Error retrieving agent execution log: {error}"
    RETRIEVE_ALL_ERROR = "Error retrieving agent execution logs: {error}"
    UPDATE_ERROR = "Error updating agent execution log: {error}"
    DELETE_ERROR = "Error deleting agent execution log: {error}"