from fastapi import Security
from fastapi.security import HTTPBearer

# For OpenAPI schema and Swagger UI
bearer_scheme = HTTPBearer(auto_error=False)


def security_dependency():
    """
    This does NOT authenticate anything — it just makes Swagger docs
    show the Authorize button and include the Bearer token in requests.
    """
    return Security(bearer_scheme)
