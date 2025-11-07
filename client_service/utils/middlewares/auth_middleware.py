import os

from fastapi import HTTPException, Request
from jose import JWTError, jwt
from starlette.middleware.base import BaseHTTPMiddleware

# You can store this in .env and load via os.getenv()
SECRET_KEY = os.getenv("JWT_SECRET_KEY", "secret")
ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")


# Optional: replace this with actual DB/service user lookup
async def get_user_from_db(user_id: int):
    # Mocked user lookup (replace with DB query)
    fake_users = {1: {"id": 1, "name": "Poobal", "role": "Admin"}}
    return fake_users.get(user_id)


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip auth for login or open endpoints
        if request.url.path.startswith(("/auth/login", "/health", "/docs", "/open")):
            return await call_next(request)

        auth_header = request.headers.get("Authorization")

        if not auth_header:
            raise HTTPException(status_code=401, detail="Missing Authorization header")

        # Expect header format: "Bearer <token>"
        try:
            scheme, token = auth_header.split()
            if scheme.lower() != "bearer":
                raise ValueError
        except ValueError:
            raise HTTPException(
                status_code=401, detail="Invalid Authorization header format"
            )

        # Decode JWT
        try:
            # payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
            # user_id = payload.get("user_id", 1)

            user_id = 1

            if not user_id:
                raise HTTPException(status_code=401, detail="Token missing user_id")

            # Check if user exists
            user = await get_user_from_db(user_id)
            if not user:
                raise HTTPException(
                    status_code=401, detail="User not found or inactive"
                )

            # Attach user to request
            request.state.user = user

        except JWTError as e:
            raise HTTPException(
                status_code=401, detail=f"Invalid or expired token: {str(e)}"
            )

        # Continue to next handler
        response = await call_next(request)
        return response
