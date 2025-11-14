"""
Script to add operation_id to all router endpoints for Bedrock compatibility (< 64 chars)
"""
import re
from pathlib import Path

# Define operation IDs for each router
OPERATION_IDS = {
    "users_router.py": {
        "/users/create": "create_user",
        "/users/{user_id}": "get_user",
        "/users": "list_users",
        "/users/{user_id}": "update_user",
        "/users/{user_id}": "delete_user",
    },
    "roles_router.py": {
        "/roles/create": "create_role",
        "/roles/{role_id}": "get_role",
        "/roles": "list_roles",
        "/roles/{role_id}": "update_role",
        "/roles/{role_id}": "delete_role",
    },
    "permissions_router.py": {
        "/permissions/create": "create_permission",
        "/permissions/{permission_id}": "get_permission",
        "/permissions": "list_permissions",
        "/permissions/{permission_id}": "update_permission",
        "/permissions/{permission_id}": "delete_permission",
    },
    "user_roles_router.py": {
        "/user-roles/assign": "assign_user_role",
        "/user-roles/{user_role_id}": "get_user_role",
        "/user-roles": "list_user_roles",
        "/user-roles/user/{user_id}": "get_user_roles",
        "/user-roles/{user_role_id}": "delete_user_role",
    },
    "role_permissions_router.py": {
        "/role-permissions/assign": "assign_role_permission",
        "/role-permissions/{role_permission_id}": "get_role_permission",
        "/role-permissions": "list_role_permissions",
        "/role-permissions/role/{role_id}": "get_role_permissions",
        "/role-permissions/{role_permission_id}": "delete_role_permission",
    },
    "vendors_router.py": {
        "/vendors/create": "create_vendor",
        "/vendors/{vendor_id}": "get_vendor",
        "/vendors": "list_vendors",
        "/vendors/search/{vendor_code}": "search_vendor",
        "/vendors/{vendor_id}": "update_vendor",
        "/vendors/{vendor_id}": "delete_vendor",
    },
    "transactions_router.py": {
        "/transactions/create": "create_transaction",
        "/transactions/{transaction_id}": "get_transaction",
        "/transactions": "list_transactions",
        "/transactions/entity/{entity_id}": "get_entity_transactions",
        "/transactions/{transaction_id}": "update_transaction",
        "/transactions/{transaction_id}": "delete_transaction",
    },
    "items_router.py": {
        "/items/create": "create_item",
        "/items/{item_id}": "get_item",
        "/items": "list_items",
        "/items/search/{item_code}": "search_item",
        "/items/{item_id}": "update_item",
        "/items/{item_id}": "delete_item",
    },
    "expenses_router.py": {
        "/expenses/categories/create": "create_expense_category",
        "/expenses/categories/{category_id}": "get_expense_category",
        "/expenses/categories": "list_expense_categories",
        "/expenses/categories/{category_id}": "update_expense_category",
        "/expenses/categories/{category_id}": "delete_expense_category",
    },
    "vendor_classification_router.py": {
        "/vendor-classifications/create": "create_vendor_class",
        "/vendor-classifications/{classification_id}": "get_vendor_class",
        "/vendor-classifications": "list_vendor_classes",
        "/vendor-classifications/{classification_id}": "update_vendor_class",
        "/vendor-classifications/{classification_id}": "delete_vendor_class",
    },
    "workflows_router.py": {
        "/workflows/create": "create_workflow",
        "/workflows/{workflow_id}": "get_workflow",
        "/workflows": "list_workflows",
        "/workflows/client/{client_id}": "get_client_workflows",
        "/workflows/{workflow_id}": "update_workflow",
        "/workflows/{workflow_id}": "delete_workflow",
    },
    "logs_router.py": {
        "/logs/create": "create_log",
        "/logs/{log_id}": "get_log",
        "/logs": "list_logs",
        "/logs/entity/{entity_id}": "get_entity_logs",
    },
    "documents_router.py": {
        "/documents/create": "create_document",
        "/documents/{client_id}/{collection_name}/{document_id}": "get_document",
        "/documents/{client_id}/{collection_name}": "list_documents",
        "/documents/{client_id}/{collection_name}/{document_id}": "update_document",
        "/documents/{client_id}/{collection_name}/{document_id}": "delete_document",
    },
}

def add_operation_id_to_decorator(content, method, path, operation_id):
    """Add operation_id to a router decorator"""
    # Pattern to match the decorator
    pattern = rf'(@router\.{method}\(\s*"{re.escape(path)}",\s*response_model=APIResponse,)'
    
    # Check if operation_id already exists
    if f'operation_id="{operation_id}"' in content:
        return content
    
    # Add operation_id after response_model
    replacement = rf'\1\n    operation_id="{operation_id}",'
    
    content = re.sub(pattern, replacement, content, count=1)
    return content

def process_router_file(file_path):
    """Process a single router file"""
    print(f"Processing {file_path.name}...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Simple mapping based on common patterns
    operations = [
        ("post", "/create", "create"),
        ("get", "/{", "get"),
        ("get", '",', "list"),
        ("put", "/{", "update"),
        ("patch", "/{", "patch"),
        ("delete", "/{", "delete"),
    ]
    
    # Count modifications
    modified = False
    
    # Add operation_id where missing
    lines = content.split('\n')
    new_lines = []
    i = 0
    
    while i < len(lines):
        line = lines[i]
        
        # Check if this is a router decorator
        if '@router.' in line and '(' in line:
            # Check if next few lines have operation_id
            has_operation_id = False
            for j in range(i, min(i+10, len(lines))):
                if 'operation_id=' in lines[j]:
                    has_operation_id = True
                    break
                if 'async def' in lines[j]:
                    break
            
            if not has_operation_id and 'response_model=APIResponse' in '\n'.join(lines[i:i+10]):
                # Find the line with response_model
                for j in range(i, min(i+10, len(lines))):
                    if 'response_model=APIResponse' in lines[j]:
                        new_lines.append(lines[i:j+1])
                        # Add operation_id line
                        indent = '    '
                        new_lines.append(f'{indent}operation_id="TODO",')
                        modified = True
                        i = j + 1
                        continue
        
        new_lines.append(line)
        i += 1
    
    if modified:
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(new_lines))
        print(f"  ✓ Modified {file_path.name}")
    else:
        print(f"  - No changes needed for {file_path.name}")

def main():
    routes_dir = Path(__file__).parent / "api" / "routes"
    
    router_files = list(routes_dir.glob("*_router.py"))
    
    print(f"Found {len(router_files)} router files\n")
    
    for router_file in router_files:
        if router_file.name not in ["openapi_router.py", "routes.py"]:
            process_router_file(router_file)
    
    print("\n✓ Done! Please review and update TODO operation_ids manually.")

if __name__ == "__main__":
    main()
