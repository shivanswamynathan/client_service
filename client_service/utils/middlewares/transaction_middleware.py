import os
import time
import json
import logging
import datetime
import asyncio
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import boto3
from io import BytesIO
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
from dotenv import load_dotenv
from botocore.exceptions import ClientError

# Load environment variables
load_dotenv()

# Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "ginthi-audit-logs")
SERVICE_NAME = os.getenv("SERVICE_NAME", "client_service_logs")
LOG_DIR = os.getenv("LOG_DIR", "s3_buffer_logs")
AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")

# Initialize logger
logger = logging.getLogger(__name__)
os.makedirs(LOG_DIR, exist_ok=True)

# Initialize S3 client
s3_client = boto3.client(
    "s3",
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=AWS_REGION,
)


class TransactionLogMiddleware(BaseHTTPMiddleware):
    """Middleware to capture and log all HTTP transactions."""

    async def dispatch(self, request: Request, call_next):
        start_time = time.time()
        client_ip = request.client.host if request.client else "unknown"
        headers = dict(request.headers)
        method = request.method
        path = request.url.path

        # Read request body safely
        try:
            body_bytes = await request.body()
            body_str = body_bytes.decode("utf-8") if body_bytes else None
            request_body = json.loads(body_str) if body_str else None
        except Exception:
            request_body = None

        response = await call_next(request)
        duration_ms = round((time.time() - start_time) * 1000, 2)

        # Log entry
        log_entry = {
            "timestamp": datetime.datetime.utcnow().isoformat(),
            "service": SERVICE_NAME,
            "method": method,
            "path": path,
            "ip": client_ip,
            "status_code": response.status_code,
            "duration_ms": duration_ms,
            "headers": json.dumps(headers),
            "request_body": json.dumps(request_body) if request_body else None,
        }

        await self.write_log_locally(log_entry)
        return response

    async def write_log_locally(self, log_entry: dict):
        """Write logs locally as JSONL files for later Parquet conversion."""
        today = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        file_path = os.path.join(LOG_DIR, f"{SERVICE_NAME}_{today}.jsonl")

        async with asyncio.Lock():
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(log_entry) + "\n")


async def upload_logs_to_s3():
    """Convert local JSONL logs to Parquet and upload to S3."""
    try:
        now = datetime.datetime.utcnow()
        today = now.strftime("%Y-%m-%d")
        file_path = os.path.join(LOG_DIR, f"{SERVICE_NAME}_{today}.jsonl")

        if not os.path.exists(file_path):
            logger.info("No local logs found to upload.")
            return

        logger.info(f"Found local log file: {file_path}")

        # Read JSONL
        with open(file_path, "r", encoding="utf-8") as f:
            lines = [json.loads(line) for line in f if line.strip()]

        if not lines:
            logger.warning("Log file is empty — skipping upload.")
            return

        # Convert to Parquet in-memory
        df = pd.DataFrame(lines)
        table = pa.Table.from_pandas(df)
        buffer = BytesIO()
        pq.write_table(table, buffer, compression="snappy")
        buffer.seek(0)

        # S3 Key path
        s3_key = (
            f"{SERVICE_NAME}/{now.year}/{now.month:02}/{now.day:02}/"
            f"{SERVICE_NAME}_{today}.parquet"
        )

        # Upload
        logger.info(f"Uploading Parquet to S3 bucket: {S3_BUCKET_NAME}")
        logger.info(f"Object path: {s3_key}")

        s3_client.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=buffer.getvalue(),
            ContentType="application/octet-stream",
        )

        logger.info(f"Uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")

        os.remove(file_path)
        logger.info(f"Deleted local log file: {file_path}")

    except ClientError as e:
        logger.error(f"AWS S3 ClientError: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"[S3 Upload Error] {e}", exc_info=True)


