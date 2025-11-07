import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any

import requests
from dotenv import load_dotenv
import re
import uuid
from html import unescape


def chunked(iterable: List[Any], size: int):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def load_seed(seed_path: Path) -> List[Dict[str, Any]]:
    if not seed_path.exists():
        raise FileNotFoundError(f"Seed file not found: {seed_path}")
    with seed_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError("Seed file must contain a JSON array of vendors")
    return data


def post_batch(session: requests.Session, url: str, batch: List[Dict[str, Any]]):
    resp = session.post(url, json=batch, timeout=60)
    return resp


PLACEHOLDER_VALUES = {"-", "", None}


def _generate_vendor_code(name: str | None) -> str:
    base = (name or "VENDOR").upper()
    # Keep only letters, numbers, and spaces for base, then compress spaces to '-'
    base = re.sub(r"[^A-Z0-9 ]+", "", base).strip()
    base = re.sub(r"\s+", "-", base)
    if not base:
        base = "VENDOR"
    # Ensure base length leaves room for suffix
    suffix = uuid.uuid4().hex[:8].upper()
    max_base_len = 50 - (1 + len(suffix))  # base + '-' + suffix
    base = base[:max_base_len]
    return f"{base}-{suffix}"


def _sanitize_vendor(v: Dict[str, Any]) -> Dict[str, Any]:
    v = dict(v)  # shallow copy

    # Normalize placeholders to None for optional fields
    optional_fields = [
        "email",
        "gst_id",
        "company_pan",
        "tan",
        "bank_acc_no",
        "beneficiary_name",
        "ifsc_code",
        "payment_term_days",
        "user_phone",
    ]

    for key in optional_fields:
        if key in v and v[key] in PLACEHOLDER_VALUES:
            v[key] = None

    # Email: unescape, clean wrappers, and validate or drop
    email = v.get("email")
    if isinstance(email, str):
        cleaned = unescape(email).strip()
        # Strip common wrappers
        for ch in ['"', "'", '<', '>', '`']:
            cleaned = cleaned.strip(ch)
        # Drop if it contains disallowed chars
        if any(c in cleaned for c in [';', ',', ' ', '(', ')']):
            v["email"] = None
        else:
            # Basic RFC-lite check: local@domain.tld without spaces/quotes
            email_pattern = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
            if not email_pattern.match(cleaned):
                v["email"] = None
            else:
                v["email"] = cleaned

    # Enforce max lengths defined in schema
    max_len = {
        "vendor_name": 255,
        "vendor_code": 50,
        "gst_id": 15,
        "company_pan": 10,
        "tan": 10,
        "bank_acc_no": 20,
        "beneficiary_name": 255,
        "ifsc_code": 11,
        "user_phone": 15,
    }
    for k, m in max_len.items():
        if k in v and isinstance(v[k], str) and v[k] is not None:
            v[k] = v[k][:m]

    # acc_verified to bool if present
    if "acc_verified" in v and not isinstance(v["acc_verified"], bool):
        v["acc_verified"] = str(v["acc_verified"]).lower() in {"1", "true", "yes", "y"}

    # Generate vendor_code when missing/placeholder
    code = v.get("vendor_code")
    if code in PLACEHOLDER_VALUES or (isinstance(code, str) and code.strip() == "-"):
        v["vendor_code"] = _generate_vendor_code(v.get("vendor_name"))

    return v


def main():
    # Load env (if present)
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    # Config
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8005"))
    base_url = os.getenv("BASE_URL", f"http://{host}:{port}")

    # Normalize non-routable listener addresses for client requests
    def _normalize_url(url: str) -> str:
        return (
            url.replace("0.0.0.0", "127.0.0.1")
               .replace("[::]", "127.0.0.1")
        )

    base_url = _normalize_url(base_url)

    endpoint = "/api/v1/vendors/create"
    url = base_url.rstrip("/") + endpoint

    seed_file = os.getenv("VENDOR_SEED", str(Path(__file__).resolve().parents[1] / "vendor_seed.json"))
    batch_size = int(os.getenv("SEED_BATCH_SIZE", "100"))
    token = os.getenv("API_TOKEN")

    # Load seed data
    vendors = load_seed(Path(seed_file))
    if not vendors:
        print("No vendors found in seed file. Nothing to do.")
        return

    # Sanitize upfront to minimize 422s
    vendors = [_sanitize_vendor(v) for v in vendors]

    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"

    session = requests.Session()
    session.headers.update(headers)

    created_count = 0
    skipped_count = 0
    failed_items: List[Dict[str, Any]] = []

    print(f"Seeding {len(vendors)} vendors to {url} (batch_size={batch_size})")

    for batch in chunked(vendors, batch_size):
        resp = post_batch(session, url, batch)
        if resp.status_code in (200, 201):
            data = resp.json()
            # APIResponse: { success, message, data: [vendors] }
            created = len(data.get("data") or [])
            created_count += created
            print(f"Created {created} vendors (batch)")
            continue

        # If conflict or validation error on batch, try item-by-item
        if resp.status_code in (409, 422):
            print("Batch conflict detected, retrying items individually...")
            for item in batch:
                single = _sanitize_vendor(item)
                r = post_batch(session, url, [single])
                if r.status_code in (200, 201):
                    created_count += 1
                elif r.status_code == 409:
                    # Try regenerating vendor_code once and retry
                    retry_item = dict(single)
                    retry_item["vendor_code"] = _generate_vendor_code(retry_item.get("vendor_name"))
                    r2 = post_batch(session, url, [retry_item])
                    if r2.status_code in (200, 201):
                        created_count += 1
                    else:
                        skipped_count += 1
                        try:
                            detail = r2.json().get("detail")
                        except Exception:
                            detail = r2.text
                        code = single.get("vendor_code")
                        print(f"Skip duplicate vendor_code={code} after retry: {detail}")
                elif r.status_code == 422:
                    try:
                        err = r.json()
                    except Exception:
                        err = r.text
                    print(f"Validation failed for vendor_code={item.get('vendor_code')}: {err}")
                    failed_items.append({
                        "vendor_code": item.get("vendor_code"),
                        "vendor_name": item.get("vendor_name"),
                        "error": err,
                        "record": item,
                    })
                else:
                    try:
                        body = r.json()
                    except Exception:
                        body = r.text
                    print(f"Failed to create vendor_code={item.get('vendor_code')}: {r.status_code} {body}")
        else:
            try:
                body = resp.json()
            except Exception:
                body = resp.text
            print(f"Batch failed: {resp.status_code} {body}")

    print(f"Done. Created={created_count}, SkippedDuplicates={skipped_count}")
    if failed_items:
        out_path = Path(__file__).resolve().parent / "seed_failures.json"
        try:
            with out_path.open("w", encoding="utf-8") as f:
                json.dump(failed_items, f, ensure_ascii=False, indent=2)
            print(f"Wrote {len(failed_items)} failed items to {out_path}")
        except Exception as e:
            print(f"Could not write failure report: {e}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
