# billing_service.py local
# ChakoraHub Billing Service  ─  FastAPI Microservice
# Port : 8010
# Run  : uvicorn billing_service:app --host 0.0.0.0 --port 8010
#
# Cache and stream acceleration are disabled in this build.

import os
import base64
import uuid
import json
import re
import random
import string
import hmac
import hashlib
import pathlib
import boto3
import threading
import time
import traceback
import requests as http_requests
import secrets 
from html import escape
from io import BytesIO
from datetime import datetime, timedelta
from decimal import Decimal
from threading import Thread
from typing import List, Optional, Dict, Any
from botocore.exceptions import ClientError
from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Form, Request
from fastapi.responses import RedirectResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel
import oracledb
from werkzeug.security import check_password_hash
from werkzeug.utils import secure_filename
from dotenv import load_dotenv, dotenv_values
from werkzeug.security import generate_password_hash
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from kafka import KafkaProducer as _KafkaProducer
from kafka import KafkaConsumer

_esc = escape

# =====================================================
# FASTAPI APP
# =====================================================
app = FastAPI(title="ChakoraHub Billing Service")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://www.chakorahub.com", "http://127.0.0.1:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

security = HTTPBasic()
_SERVICE_ENV_PATH = pathlib.Path(__file__).parent / ".env"
load_dotenv(dotenv_path=_SERVICE_ENV_PATH, override=False)
_SERVICE_ENV = dotenv_values(_SERVICE_ENV_PATH) if _SERVICE_ENV_PATH.exists() else {}

# =====================================================
# Caching and stream tooling are intentionally disabled.
# =====================================================

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
PRICING_HISTORY_TABLE = os.getenv("PRICING_HISTORY_TABLE", "CHAKORAHUB_PRICING_HISTORY")
_pricing_dynamodb = boto3.resource("dynamodb", region_name=os.getenv("AWS_REGION", "eu-north-1"))
_pricing_history_table = _pricing_dynamodb.Table(PRICING_HISTORY_TABLE)
_kafka_producer = None
try:
    _kafka_producer = _KafkaProducer(
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
        retries=3,
    )
    print("✅ Billing Kafka producer connected")
except Exception as _e:
    print(f"⚠️  Billing Kafka producer unavailable @ {KAFKA_BOOTSTRAP_SERVERS}: {_e}")
    _kafka_producer = None

consumer = None
try:
    consumer = KafkaConsumer(
        "teams.link.created",
        bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        group_id="billing-service-group"
    )
    print("✅ Billing teams.link.created consumer connected")
except Exception as _e:
    print(f"⚠️  Billing teams.link.created consumer unavailable @ {KAFKA_BOOTSTRAP_SERVERS}: {_e}")
    consumer = None

# ── TTL constants (billing namespace) ──────────────
TTL_INVOICE         = 300    # 5 min   — invoice:{transaction_id}
TTL_PAYMENT_STATUS  = 180    # 3 min   — payment_status:{transaction_id}
TTL_BILLING_HISTORY = 300    # 5 min   — billing:history:{phone}
TTL_USER_PHONE      = 1_800  # 30 min  — user:phone:{phone}
TTL_COURSES         = 900    # 15 min  — courses:all

# ── Redis Streams (registration async billing) ─────
REGISTRATION_STREAM_NAME    = os.getenv("REGISTRATION_STREAM_NAME", "registration_stream")
REGISTRATION_CONSUMER_GROUP = os.getenv("REGISTRATION_CONSUMER_GROUP", "billing_group")
REGISTRATION_CONSUMER_NAME  = os.getenv("REGISTRATION_CONSUMER_NAME", f"billing-consumer-{os.getpid()}")

_registration_consumer_stop   = threading.Event()
_registration_consumer_thread = None

DEFAULT_SHOP_PASSWORD = "changeme123"
ORACLE_HOST = os.getenv("ORACLE_HOST", "56.228.73.210")
ORACLE_PORT = int(os.getenv("ORACLE_PORT", "1521"))
ORACLE_SERVICE_NAME = os.getenv("ORACLE_SERVICE_NAME", "FREEPDB1")
ORACLE_USER = os.getenv("ORACLE_USER", "SUPPORT")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "Welcome123")

# Keep existing cursor call sites unchanged.
DictCursor = object()

# =====================================================
# S3 CONFIGURATION
# =====================================================
S3_BUCKET_NAME = "backup-receipt"
AWS_REGION     = "eu-north-1"

# Helper functions

def _service_env_value(*names: str) -> Optional[str]:
    for name in names:
        value = _SERVICE_ENV.get(name)
        if value:
            return str(value).strip()
    for name in names:
        value = os.getenv(name)
        if value:
            return str(value).strip()
    return None

def _kafka_publish(topic: str, payload: dict) -> None:
    """Fire-and-forget. Falls back silently if Kafka is down."""
    if _kafka_producer is None:
        print(f"⚠️  Kafka publish skipped [{topic}] because producer is unavailable")
        return
    try:
        print(f"📤 Kafka publish request → {topic} | keys={list(payload.keys())}")
        _kafka_producer.send(topic, value=payload)
        _kafka_producer.flush(timeout=2)
        print(f"📤 Kafka → {topic}: {payload}")
    except Exception as e:
        print(f"⚠️  Kafka publish failed [{topic}]: {e}")

def consume_teams_link_created():
    if consumer is None:
        print("⚠️  teams.link.created consumer is disabled because Kafka is unavailable")
        return

    for msg in consumer:
        print(f"📥 Kafka consume ← {msg.topic} | partition={msg.partition} offset={msg.offset}")
        payload = msg.value
        print("Received:", payload)

        student_email = payload.get("student_email") or payload.get("attendee")
        booking_id = payload.get("booking_id")
        meeting_link = payload.get("meeting_link") or payload.get("teams_link") or ""

        if not student_email or not booking_id:
            print(f"⚠️ Invalid teams.link.created payload (missing student_email/booking_id): {payload}")
            continue

        # Send email
        send_meeting_booking_email(
            BookingEmailRequest(
                student_email=student_email,
                booking_id=booking_id,
                student_name=payload.get("student_name", "Student"),
                date=payload.get("date", ""),
                start_time=payload.get("start_time", ""),
                duration_minutes=int(payload.get("duration_minutes", 0) or 0),
                price=float(payload.get("price", 0) or 0),
                complexity=payload.get("complexity", "Medium"),
                booking_type=payload.get("booking_type", "external"),
                meeting_link=meeting_link,
                payment_id=payload.get("payment_id", ""),
                order_id=payload.get("order_id", "")
            )
        )

        # Publish email.sent
        _kafka_publish("email.sent", {
            "correlation_id": payload.get("correlation_id", booking_id),
            "booking_id": booking_id,
            "student_email": student_email,
            "status": "sent",
            "timestamp": datetime.utcnow().isoformat()
        })

def _aws_client_kwargs(region_name: str) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {"region_name": region_name}
    if AWS_ACCESS_KEY and AWS_SECRET_KEY:
        kwargs["aws_access_key_id"] = AWS_ACCESS_KEY
        kwargs["aws_secret_access_key"] = AWS_SECRET_KEY
        if AWS_SESSION_TOKEN:
            kwargs["aws_session_token"] = AWS_SESSION_TOKEN
    return kwargs

# =====================================================
# AWS & SES CONFIGURATION
# =====================================================

AWS_ACCESS_KEY = _service_env_value("AWS_ACCESS_KEY_ID", "AWS_ACCESS_KEY")
AWS_SECRET_KEY = _service_env_value("AWS_SECRET_ACCESS_KEY", "AWS_SECRET_KEY")
AWS_SESSION_TOKEN = _service_env_value("AWS_SESSION_TOKEN")
ADMIN_EMAIL    = os.getenv("ADMIN_EMAIL",            "admin@chakorahub.com")
LOGIN_URL      = os.getenv("CHAKORA_LOGIN_URL",      "https://www.chakorahub.com/")

SES_REGION = "eu-north-1"
ses = boto3.client(
    "ses",
    **_aws_client_kwargs(SES_REGION),
)

s3_client = boto3.client(
    "s3",
    **_aws_client_kwargs(AWS_REGION),
)

_ses = None
try:
    _ses = boto3.client(
        "ses",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        verify=False,   # ← matches app.py; prevents SSL cert failures on EC2
    )
    print("✅ billing_service SES client ready")
except Exception as _se:
    print(f"⚠️  SES unavailable: {_se}")

# =====================================================
# Cache helper stubs (cache disabled)
# =====================================================
def cache_get(key: str):
    _ = key
    return None


def cache_set(key: str, value: Any, ttl: int) -> bool:
    _ = (key, value, ttl)
    return True


def cache_delete(key: str) -> bool:
    _ = key
    return True


def cache_delete_pattern(pattern: str) -> bool:
    _ = pattern
    return True


def increment_counter(key: str, ttl: int = 86400):
    _ = (key, ttl)
    return 1

# =====================================================
# BASIC AUTH
# =====================================================
def verify_credentials(credentials: HTTPBasicCredentials = Depends(security)):
    expected_user = os.getenv("BILLING_ADMIN_USER", "")
    expected_pass = os.getenv("BILLING_ADMIN_PASS", "")
    ok = (
        secrets.compare_digest(credentials.username, expected_user)
        and secrets.compare_digest(credentials.password, expected_pass)
    )
    if not ok:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    return credentials.username

# =====================================================
# ORACLE CONNECTION
# =====================================================
class _OracleCursorCompat:
    def __init__(self, raw_cursor, dict_mode=False):
        self._cursor = raw_cursor
        self._dict_mode = dict_mode

    @staticmethod
    def _rewrite_sql(sql, params):
        if params is None:
            return sql
        if isinstance(params, dict):
            return re.sub(r"%\((\w+)\)s", r":\1", sql)
        if "%s" in sql:
            parts = sql.split("%s")
            rebuilt = parts[0]
            for idx, tail in enumerate(parts[1:], start=1):
                rebuilt += f":{idx}{tail}"
            return rebuilt
        return sql

    def execute(self, sql, params=None):
        rewritten_sql = self._rewrite_sql(sql, params)
        if params is None:
            self._cursor.execute(rewritten_sql)
        else:
            self._cursor.execute(rewritten_sql, params)

        if self._dict_mode and self._cursor.description:
            columns = [d[0] for d in self._cursor.description]
            self._cursor.rowfactory = lambda *vals, cols=columns: dict(zip(cols, vals))
        return self

    def __getattr__(self, name):
        return getattr(self._cursor, name)


class _OracleConnectionCompat:
    def __init__(self, raw_conn):
        self._conn = raw_conn

    def cursor(self, *args, **kwargs):
        dict_mode = bool(args)
        return _OracleCursorCompat(self._conn.cursor(), dict_mode=dict_mode)

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_db_connection():
    try:
        dsn = oracledb.makedsn(
            host=ORACLE_HOST,
            port=ORACLE_PORT,
            service_name=ORACLE_SERVICE_NAME,
        )
        raw_conn = oracledb.connect(
            user=ORACLE_USER,
            password=ORACLE_PASSWORD,
            dsn=dsn,
        )

        cursor = raw_conn.cursor()
        cursor.execute("ALTER SESSION SET CURRENT_SCHEMA = CHAKORA")
        cursor.close()

        conn = _OracleConnectionCompat(raw_conn)
        print("✅ Connected to Oracle")
        return conn
    except Exception as exc:
        print("❌ Oracle connection error:", exc)
        return None


# =====================================================
# S3 RECEIPT UPLOAD
# =====================================================
def upload_receipt_to_s3(file: UploadFile, phone: str) -> str:
    """Upload billing receipt to S3. Returns the s3:// path. Does NOT cache the URL."""
    try:
        file.file.seek(0)
        timestamp      = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        file_extension = file.filename.split(".")[-1] if "." in file.filename else "pdf"
        safe_filename  = secure_filename(file.filename)
        s3_filename    = f"{phone}_{timestamp}.{file_extension}"
        s3_key         = f"billing-receipts/{phone[:4]}/{phone}/{s3_filename}"

        s3_client.upload_fileobj(
            file.file,
            S3_BUCKET_NAME,
            s3_key,
            ExtraArgs={
                "ContentType": file.content_type or "application/pdf",
                "ContentDisposition": f"attachment; filename={safe_filename}",
                "Metadata": {
                    "phone": phone,
                    "upload_time": timestamp,
                    "original_filename": safe_filename,
                },
            },
        )
        s3_path = f"s3://{S3_BUCKET_NAME}/{s3_key}"
        print(f"✅ Receipt uploaded to: {s3_path}")
        return s3_path

    except Exception as exc:
        print(f"❌ S3 upload error: {exc}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to upload receipt: {exc}")


def get_receipt_presigned_url(s3_path: str, expiration: int = 3600) -> str:
    """
    Generate a fresh presigned URL every time.
    Presigned URLs embed credentials and have a hard expiry —
    caching them would serve expired or credential-leaked URLs.
    """
    try:
        s3_key = s3_path.replace(f"s3://{S3_BUCKET_NAME}/", "")
        url = s3_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": s3_key},
            ExpiresIn=expiration,
        )
        return url
    except Exception as exc:
        print(f"❌ Presigned URL error: {exc}")
        return None


# =====================================================
# EMAIL HELPERS
# =====================================================
def send_billing_email(user_name, user_email, phone, course_name, amount, payment_method, transaction_uuid):
    body = f"""
New Billing Entry Created

Student Name: {user_name}
Email: {user_email}
Phone: {phone}
Course: {course_name}
Amount Paid: ₹{amount}
Payment Method: {payment_method}
Transaction UUID: {transaction_uuid}

Status: PENDING

Please verify the payment in admin panel.
"""
    try:
        ses.send_email(
            Source="admin@chakorahub.com",
            Destination={"ToAddresses": ["admin@chakorahub.com"]},
            Message={
                "Subject": {"Data": "New ChakoraHub Billing Entry"},
                "Body": {"Text": {"Data": body}},
            },
        )
        print("✅ Billing email sent")
    except Exception as exc:
        print("❌ Billing email failed:", exc)


def send_registration_completion_email(student_name, student_email, registration_id, course_name, amount):
    subject = f"✅ Registration Successful - {course_name}"
    safe_name = escape(student_name or "Student")
    safe_email = escape(student_email or "")
    safe_registration_id = escape(registration_id or "")
    safe_course_name = escape(course_name or "Registration")
    login_url = "https://www.chakorahub.com/login"

    text_body = (
        f"Dear {student_name},\n\n"
        f"Your registration for {course_name} has been completed successfully.\n\n"
        f"Registration ID: {registration_id}\n"
        f"Email: {student_email}\n"
        f"Default Password: changeme123\n\n"
        f"Important: Please change your password after your first login.\n\n"
        f"Visit {login_url} to get started.\n\n"
        f"Regards,\n"
        f"ChakoraHub Team"
    )

    html_body = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>{escape(subject)}</title>
</head>
<body style="margin:0; padding:24px; background-color:#f4f5f7; font-family:Arial, sans-serif; color:#202124;">
    <div style="max-width:1140px; margin:0 auto;">
        <div style="background-color:#4caf50; color:#ffffff; padding:48px 28px; border-radius:6px 6px 0 0;">
            <div style="font-size:22px; font-weight:700; line-height:1.3;">
                <span style="margin-right:10px;">🎉</span>Registration Successful!
            </div>
        </div>

        <div style="background-color:#f7f7f7; padding:32px 26px 28px; border-radius:0 0 6px 6px; border:1px solid #ececec; border-top:none;">
            <p style="margin:0 0 18px; font-size:18px; line-height:1.6;">Dear <strong>{safe_name}</strong>,</p>

            <p style="margin:0 0 28px; font-size:17px; line-height:1.7;">
                Your registration for <strong>{safe_course_name}</strong> has been completed successfully.
            </p>

            <table style="width:100%; border-collapse:collapse; margin:0 0 24px; border:1px solid #d8d8d8; font-size:16px;">
                <tr>
                    <td style="width:46%; padding:14px 12px; border:1px solid #d8d8d8; background-color:#eaf5e8; font-weight:700;">Registration ID:</td>
                    <td style="padding:14px 12px; border:1px solid #d8d8d8; background-color:#ffffff;">{safe_registration_id}</td>
                </tr>
                <tr>
                    <td style="padding:14px 12px; border:1px solid #d8d8d8; background-color:#ffffff; font-weight:700;">Email:</td>
                    <td style="padding:14px 12px; border:1px solid #d8d8d8; background-color:#ffffff;">
                        <a href="mailto:{safe_email}" style="color:#1565c0; text-decoration:underline;">{safe_email}</a>
                    </td>
                </tr>
                <tr>
                    <td style="padding:14px 12px; border:1px solid #d8d8d8; background-color:#eaf5e8; font-weight:700;">Default Password:</td>
                    <td style="padding:14px 12px; border:1px solid #d8d8d8; background-color:#eaf5e8;">changeme123</td>
                </tr>
            </table>

            <p style="margin:0 0 18px; font-size:16px; line-height:1.6; color:#d93025;">
                <strong>⚠ Important:</strong> Please change your password after your first login.
            </p>

            <p style="margin:0 0 22px; font-size:16px; line-height:1.6;">
                Visit <a href="{login_url}" style="color:#1565c0; text-decoration:underline;">ChakoraHub Login</a> to get started.
            </p>

            <p style="margin:0; font-size:16px; line-height:1.6;">
                Regards,<br>
                <strong>ChakoraHub Team</strong>
            </p>
        </div>
    </div>
</body>
</html>
"""

    try:
        ses.send_email(
            Source="admin@chakorahub.com",
            Destination={
                "ToAddresses": [student_email],
                "CcAddresses": ["admin@chakorahub.com"],
            },
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Html": {"Data": html_body},
                    "Text": {"Data": text_body},
                },
            },
        )
        print(f"✅ Registration email sent to {student_email}")
    except Exception as exc:
        print(f"❌ Registration email failed for {student_email}: {exc}")


# =====================================================
# REGISTRATION STREAM CONSUMER
# =====================================================
def _process_registration_event(payload: Dict[str, Any]) -> None:
    """Persist payment info and send registration email from registration event."""
    registration_id = str(payload.get("registration_id") or "").strip()
    user_id         = int(payload.get("user_id") or 0)
    payment_id      = str(payload.get("payment_id") or "").strip()
    order_id        = str(payload.get("order_id") or "").strip()
    course_name     = str(payload.get("course_name") or "Registration").strip()
    amount          = float(payload.get("payment_amount") or 0)
    student_name    = str(payload.get("student_name") or "Student").strip()
    student_email   = str(payload.get("student_email") or "").strip()

    if not registration_id or not user_id:
        print(f"⚠️  Skipping invalid registration event: missing registration_id/user_id | payload={payload}")
        return

    # Redis-based stream idempotency is disabled.

    conn = get_db_connection()
    if not conn:
        raise Exception("Database connection failed in billing consumer")

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT 1 FROM NRM_PAYMENTS
            WHERE USER_ID = %s
              AND RAZORPAY_PAYMENT_ID = %s
              AND RAZORPAY_ORDER_ID = %s
            FETCH FIRST 1 ROWS ONLY
            """,
            (user_id, payment_id, order_id),
        )
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(
                """
                INSERT INTO NRM_PAYMENTS
                    (ID, USER_ID, RAZORPAY_PAYMENT_ID, RAZORPAY_ORDER_ID, COURSE, PAYMENT_AMOUNT, CREATED_AT)
                SELECT COALESCE(MAX(ID), 0) + 1, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP
                FROM NRM_PAYMENTS
                """,
                (user_id, payment_id, order_id, course_name, amount),
            )
            conn.commit()
            print(f"✅ Billing consumer inserted NRM_PAYMENTS for registration_id={registration_id}")

        else:
            print(f"ℹ️  NRM_PAYMENTS already exists for registration_id={registration_id}")

        if student_email:
            send_registration_completion_email(
                student_name=student_name,
                student_email=student_email,
                registration_id=registration_id,
                course_name=course_name,
                amount=amount,
            )
    finally:
        cursor.close()
        conn.close()


def _registration_consumer_loop() -> None:
    consumer = None
    try:
        consumer = KafkaConsumer(
            "registration.completed",
            bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
            group_id="billing-registration-completed-consumer",
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        )
        print("✅ Billing registration.completed consumer connected")
    except Exception as e:
        print(f"⚠️  Billing registration.completed consumer unavailable @ {KAFKA_BOOTSTRAP_SERVERS}: {e}")
        return

    for msg in consumer:
        payload = msg.value or {}
        print(
            f"📥 registration.completed received | "
            f"partition={msg.partition} offset={msg.offset} reg_id={payload.get('registration_id')}"
        )
        try:
            _process_registration_event(payload)
        except Exception as e:
            print(f"❌ registration.completed processing failed: {e}")


@app.on_event("startup")
def start_registration_consumer() -> None:
    thread = threading.Thread(target=_registration_consumer_loop, daemon=True)
    thread.start()
    print("✅ registration.completed consumer thread started")

def start_kafka_consumer():
    if consumer is None:
        print("⚠️  Kafka consumer thread not started (Kafka unavailable)")
        return

    thread = threading.Thread(
        target=consume_teams_link_created,
        daemon=True
    )
    thread.start()
    print("Kafka consumer started")


@app.on_event("startup")
def start_kafka_consumer_on_startup():
    start_kafka_consumer()


@app.on_event("shutdown")
def stop_registration_consumer() -> None:
    _registration_consumer_stop.set()


# =====================================================
# AUTHENTICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Password hashes are NEVER cached — caching a hash
# means a stale entry can authenticate after a password
# change. The DB is always the authoritative source.
# We cache only the resolved role (no credential data)
# for a short TTL so repeated requests in the same
# session don't hammer Oracle.
# =====================================================
def get_current_user(credentials: HTTPBasicCredentials = Depends(security)):
    username = credentials.username
    password = credentials.password

    # We do NOT cache password hashes. The cached entry only stores role
    # and is intentionally short-lived (TTL_PAYMENT_STATUS = 3 min).
    # On every request we still re-verify the password against DB so that
    # a changed password takes effect within one TTL window.
    conn = get_db_connection()
    if conn is None:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT u.EMAIL, u.PHONE, u.USERTYPE, l.PASSWORD
            FROM nrm_users u
            JOIN nrm_logins l ON u.ID = l.USER_ID
            WHERE u.EMAIL = %(username)s OR u.PHONE = %(username)s
            FETCH FIRST 1 ROWS ONLY
            """,
            {"username": username},
        )
        user = cursor.fetchone()

        if not user:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

        db_password = user.get("PASSWORD") or ""
        if db_password.startswith("scrypt:"):
            valid = check_password_hash(db_password, password)
        else:
            valid = db_password == password

        if not valid:
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

        usertype = (user.get("USERTYPE") or "student").lower()
        role = "admin" if usertype == "admin" else "student"
        return {"username": username, "role": role}

    except HTTPException:
        raise
    except Exception as exc:
        print(f"❌ Error in get_current_user: {exc}")
        raise HTTPException(status_code=400, detail="Authentication error")
    finally:
        cursor.close()
        conn.close()


# =====================================================
# PYDANTIC MODELS
# =====================================================
class BillingRequest(BaseModel):
    billing_type: str
    billing_category: str
    payment_method: str
    amount: float
    phone: str
    currency: str = "INR"
    upi_txn_id: Optional[str] = None
    receipt_file_path: Optional[str] = None
    payload: Optional[dict] = {}


class BillingHistoryResponse(BaseModel):
    status: str
    entries: List[dict]


class CourseListResponse(BaseModel):
    status: str
    courses: List[dict]


class StatusUpdate(BaseModel):
    status: str


class PaymentCreateOrderRequest(BaseModel):
    amount: int
    currency: str = "INR"
    notes: Optional[str] = "Meeting Booking"


class PaymentVerifyRequest(BaseModel):
    order_id: str
    payment_id: str
    signature: str


class BookingEmailRequest(BaseModel):
    student_email: str
    booking_id: str
    student_name: str
    date: str
    start_time: str
    duration_minutes: int
    price: float
    complexity: str
    booking_type: str
    meeting_link: Optional[str] = ""
    purpose: Optional[str] = ""
    payment_id: Optional[str] = ""
    order_id: Optional[str] = ""

class PaymentWebhookPayload(BaseModel):
    order_id:          str
    payment_id:        str
    payment_status:    str
    gateway_ref:       Optional[str] = None
    upi_txn_id:        Optional[str] = None
    failure_reason:    Optional[str] = None
    razorpay_order_id: Optional[str] = None


# =====================================================
# STM (Secure Token Manager) — integrated in billing_service
# =====================================================
class STMPaymentLinkCreateRequest(BaseModel):
    article_id: str
    amount: float
    currency: str = "USD"
    client_reference: Optional[str] = None


class STMPaymentLinkCreateResponse(BaseModel):
    token_id: str
    payment_url: str
    expires_at: str


RAZORPAY_PAYMENT_LINKS_URL = "https://api.razorpay.com/v1/payment_links"
STM_PUBLIC_BASE_URL = (os.getenv("STM_PUBLIC_BASE_URL") or "https://www.chakorahub.com").rstrip("/")


def _razorpay_auth_tuple() -> tuple[str, str]:
    key_id = (os.getenv("RZP_KEY_ID") or "").strip()
    key_secret = (os.getenv("RZP_KEY_SECRET") or "").strip()
    if not key_id or not key_secret:
        raise HTTPException(status_code=500, detail="Razorpay credentials are not configured")
    return key_id, key_secret


def _verify_razorpay_webhook_signature(raw_body: bytes, signature_header: str) -> bool:
    secret = (os.getenv("RZP_WEBHOOK_SECRET") or "").strip()
    if not secret:
        return False
    expected = hmac.new(secret.encode("utf-8"), raw_body, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature_header or "")


def _get_currency_lookup(currency_code: str) -> Dict[str, Any]:
    code = (currency_code or "").strip().upper()
    if not code:
        raise HTTPException(status_code=400, detail="currency is required")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT CURRENCY_CODE, DECIMAL_PLACES, IS_RAZORPAY_SUPPORTED
            FROM CURRENCY_LOOKUP
            WHERE CURRENCY_CODE = %s
            """,
            (code,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=400, detail=f"Unsupported currency code: {code}")

        decimal_places = int(row.get("DECIMAL_PLACES") if row.get("DECIMAL_PLACES") is not None else 2)
        if decimal_places < 0:
            raise HTTPException(status_code=400, detail=f"Invalid decimal places configured for currency: {code}")

        supported_flag = (row.get("IS_RAZORPAY_SUPPORTED") or "").strip().upper()
        if supported_flag == "N":
            raise HTTPException(status_code=400, detail=f"Currency {code} is marked unsupported for Razorpay")

        if supported_flag not in {"Y", "N", ""}:
            raise HTTPException(status_code=500, detail=f"Invalid support flag configured for currency: {code}")

        return {
            "currency_code": code,
            "decimal_places": decimal_places,
            "is_razorpay_supported": supported_flag,
        }
    except HTTPException:
        raise
    except Exception as exc:
        err_text = str(exc)
        if "ORA-00942" in err_text:
            raise HTTPException(
                status_code=500,
                detail="CURRENCY_LOOKUP table not found in CHAKORA schema",
            )
        raise HTTPException(status_code=500, detail=f"Currency lookup failed: {err_text}")
    finally:
        cursor.close()
        conn.close()


@app.post("/stm/generate", response_model=STMPaymentLinkCreateResponse)
def stm_generate_payment_link(payload: STMPaymentLinkCreateRequest):
    if not payload.article_id.strip():
        raise HTTPException(status_code=400, detail="article_id is required")
    if payload.amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be greater than 0")

    token_id = str(uuid.uuid4())
    expires_at = datetime.utcnow() + timedelta(minutes=int(os.getenv("STM_TOKEN_TTL_MIN", "1440")))

    currency_meta = _get_currency_lookup(payload.currency or "USD")
    currency_code = currency_meta["currency_code"]
    decimal_places = int(currency_meta["decimal_places"])
    multiplier = Decimal(10) ** decimal_places
    amount_minor = int((Decimal(str(payload.amount)) * multiplier).to_integral_value())
    if amount_minor <= 0:
        raise HTTPException(status_code=400, detail="amount is too small for selected currency")

    auth = _razorpay_auth_tuple()

    req_body = {
        "amount": amount_minor,
        "currency": currency_code,
        "reference_id": token_id,
        "description": f"Payment for {payload.article_id}",
        "expire_by": int(expires_at.timestamp()),
        "accept_partial": False,
        "notes": {
            "article_id": payload.article_id,
            "client_reference": payload.client_reference or "",
        },
    }

    try:
        rp_resp = http_requests.post(
            RAZORPAY_PAYMENT_LINKS_URL,
            json=req_body,
            auth=auth,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
    except http_requests.RequestException as exc:
        print(f"❌ STM Razorpay payment link request failed: {exc}")
        raise HTTPException(status_code=502, detail="Could not reach Razorpay")

    if not rp_resp.ok:
        try:
            err_payload = rp_resp.json()
        except Exception:
            err_payload = {"raw": rp_resp.text}
        raise HTTPException(
            status_code=rp_resp.status_code,
            detail={"message": "Could not create Razorpay payment link", "razorpay_error": err_payload},
        )

    try:
        rp_data = rp_resp.json() or {}
    except Exception:
        raise HTTPException(status_code=502, detail="Invalid response from Razorpay")

    gateway_payment_url = rp_data.get("short_url") or rp_data.get("payment_url") or ""
    if not gateway_payment_url:
        raise HTTPException(status_code=502, detail="Razorpay did not return payment link details")

    payment_url = f"{STM_PUBLIC_BASE_URL}/stm/checkout/{token_id}"
    print(
        "🧷 STM generate | "
        f"token_id={token_id} article_id={payload.article_id.strip()} "
        f"amount={float(payload.amount):.2f} {currency_code} decimals={decimal_places}"
    )

    jwt_secret = (os.getenv("STM_JWT_SECRET") or os.getenv("APP_SECRET_KEY") or "temporary123").strip()
    jwt_payload = {
        "jti": token_id,
        "article_id": payload.article_id.strip(),
        "amount": float(payload.amount),
        "currency": currency_code,
        "iat": int(datetime.utcnow().timestamp()),
        "exp": int(expires_at.timestamp()),
    }
    jwt_header = {"alg": "HS256", "typ": "JWT"}
    header_b64 = base64.urlsafe_b64encode(json.dumps(jwt_header, separators=(",", ":")).encode("utf-8")).decode("utf-8").rstrip("=")
    payload_b64 = base64.urlsafe_b64encode(json.dumps(jwt_payload, separators=(",", ":")).encode("utf-8")).decode("utf-8").rstrip("=")
    signing_input = f"{header_b64}.{payload_b64}".encode("utf-8")
    signature = hmac.new(jwt_secret.encode("utf-8"), signing_input, hashlib.sha256).digest()
    signature_b64 = base64.urlsafe_b64encode(signature).decode("utf-8").rstrip("=")
    jwt_token = f"{header_b64}.{payload_b64}.{signature_b64}"

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor()
    try:
        cursor.execute(
            """
            INSERT INTO PAYMENT_TOKENS
                (TOKEN_ID, ARTICLE_ID, CLIENT_REFERENCE, AMOUNT, CURRENCY,
                 JWT_TOKEN, PAYMENT_URL, STATUS, CREATED_AT, EXPIRES_AT)
            VALUES
                (%s, %s, %s, %s, %s,
                 %s, %s, 'PENDING', SYSTIMESTAMP, %s)
            """,
            (
                token_id,
                payload.article_id.strip(),
                payload.client_reference,
                float(payload.amount),
                currency_code,
                jwt_token,
                gateway_payment_url,
                expires_at,
            ),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(f"❌ STM token persist error: {exc}")
        raise HTTPException(status_code=500, detail=f"Failed to persist payment link: {exc}")
    finally:
        cursor.close()
        conn.close()

    client_ref = (payload.client_reference or "").strip()
    if client_ref and "@" in client_ref and "." in client_ref:
        subject = f"ChakoraHub Payment Link - {payload.article_id.strip()}"
        text_body = (
            f"Hello,\n\n"
            f"Please complete your payment using the link below:\n"
            f"{payment_url}\n\n"
            f"Article/Order: {payload.article_id.strip()}\n"
            f"Amount: {float(payload.amount):.2f} {currency_code}\n"
            f"Expires At (UTC): {expires_at.isoformat()}\n\n"
            f"Regards,\nChakoraHub Team"
        )
        html_body = (
            f"<html><body style='font-family:Arial,sans-serif;color:#1f2937;'>"
            f"<h3>Payment Link</h3>"
            f"<p>Please complete your payment using the link below:</p>"
            f"<p><a href='{payment_url}'>{payment_url}</a></p>"
            f"<p><b>Article/Order:</b> {payload.article_id.strip()}<br>"
            f"<b>Amount:</b> {float(payload.amount):.2f} {currency_code}<br>"
            f"<b>Expires At (UTC):</b> {expires_at.isoformat()}</p>"
            f"<p>Regards,<br>ChakoraHub Team</p>"
            f"</body></html>"
        )

        try:
            destination = {
                "ToAddresses": [client_ref],
                "CcAddresses": [ADMIN_EMAIL] if client_ref.lower() != ADMIN_EMAIL.lower() else [],
            }
            ses.send_email(
                Source=ADMIN_EMAIL,
                Destination=destination,
                Message={
                    "Subject": {"Data": subject},
                    "Body": {
                        "Text": {"Data": text_body},
                        "Html": {"Data": html_body},
                    },
                },
            )
            print(f"✅ STM payment link email sent to {client_ref} | token_id={token_id}")
        except Exception as exc:
            print(f"❌ STM payment link email failed for {client_ref} | token_id={token_id} | error={exc}")

    return STMPaymentLinkCreateResponse(
        token_id=token_id,
        payment_url=payment_url,
        expires_at=expires_at.isoformat(),
    )


@app.get("/stm/checkout/{token_id}")
def stm_checkout(token_id: str):
    print(f"🔎 STM checkout requested | token_id={token_id}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT STATUS, EXPIRES_AT, PAYMENT_URL
            FROM PAYMENT_TOKENS
            WHERE TOKEN_ID = %s
            """,
            (token_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Payment link not found")

        status_val = (row.get("STATUS") or "PENDING").upper()
        expires_at = row.get("EXPIRES_AT")
        gateway_url = row.get("PAYMENT_URL")
        print(
            "🔎 STM checkout row | "
            f"token_id={token_id} status={status_val} "
            f"has_gateway_url={'Y' if bool(gateway_url) else 'N'}"
        )

        if status_val != "PENDING":
            raise HTTPException(status_code=410, detail=f"Payment link is {status_val.lower()}")

        if expires_at and isinstance(expires_at, datetime) and datetime.utcnow() > expires_at:
            cursor.execute(
                "UPDATE PAYMENT_TOKENS SET STATUS='EXPIRED' WHERE TOKEN_ID=%s AND STATUS='PENDING'",
                (token_id,),
            )
            conn.commit()
            raise HTTPException(status_code=410, detail="Payment link expired")

        if not gateway_url:
            raise HTTPException(status_code=500, detail="Gateway URL missing for this payment link")

        return RedirectResponse(gateway_url, status_code=302)
    finally:
        cursor.close()
        conn.close()


@app.get("/stm/status/{token_id}")
def stm_status(token_id: str):
    print(f"📊 STM status requested | token_id={token_id}")
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT TOKEN_ID, STATUS, AMOUNT, CURRENCY,
                GATEWAY_TXN_ID, PAID_AT, EXPIRES_AT, PAYMENT_URL
            FROM PAYMENT_TOKENS
            WHERE TOKEN_ID = %s
            """,
            (token_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Payment link not found")

        status_val = (row.get("STATUS") or "PENDING").upper()
        expires_at = row.get("EXPIRES_AT")
        print(
            "📊 STM status row | "
            f"token_id={token_id} status={status_val} "
            f"gateway_txn_id={row.get('GATEWAY_TXN_ID')} paid_at={row.get('PAID_AT')}"
        )

        if status_val == "PENDING" and expires_at and isinstance(expires_at, datetime) and datetime.utcnow() > expires_at:
            status_val = "EXPIRED"
            cursor.execute(
                "UPDATE PAYMENT_TOKENS SET STATUS='EXPIRED' WHERE TOKEN_ID=%s",
                (token_id,),
            )
            conn.commit()

        # Reload latest gateway txn after updates
        cursor.execute(
            "SELECT GATEWAY_TXN_ID, PAID_AT, EXPIRES_AT FROM PAYMENT_TOKENS WHERE TOKEN_ID=%s",
            (token_id,),
        )
        latest = cursor.fetchone() or {}

        return {
            "token_id": token_id,
            "status": status_val,
            "amount": row.get("AMOUNT"),
            "currency": row.get("CURRENCY"),
            "gateway_txn_id": latest.get("GATEWAY_TXN_ID"),
            "paid_at": latest.get("PAID_AT").isoformat() if latest.get("PAID_AT") else None,
            "expires_at": latest.get("EXPIRES_AT").isoformat() if latest.get("EXPIRES_AT") else None,
            "payment_url": row.get("PAYMENT_URL"),
        }
    finally:
        cursor.close()
        conn.close()


@app.post("/stm/webhook/razorpay")
async def stm_razorpay_webhook(request: Request):
    raw_body = await request.body()
    signature = request.headers.get("X-Razorpay-Signature", "")
    print(
        "📥 STM webhook received | "
        f"source_ip={getattr(request.client, 'host', 'unknown')} "
        f"sig_present={'Y' if bool(signature) else 'N'} "
        f"secret_configured={'Y' if bool((os.getenv('RZP_WEBHOOK_SECRET') or '').strip()) else 'N'} "
        f"body_len={len(raw_body)}"
    )
    is_signature_valid = _verify_razorpay_webhook_signature(raw_body, signature)

    if not is_signature_valid:
        print("❌ STM webhook signature invalid")
        raise HTTPException(status_code=401, detail="Invalid webhook signature")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid webhook JSON")

    event_name = str(payload.get("event") or "")
    payload_obj = payload.get("payload") or {}
    payment_link_entity = (payload_obj.get("payment_link") or {}).get("entity") or {}
    payment_entity = (payload_obj.get("payment") or {}).get("entity") or {}

    token_id = (
        payment_link_entity.get("reference_id")
        or (payment_entity.get("notes") or {}).get("reference_id")
        or (payment_entity.get("notes") or {}).get("token_id")
    )
    if not token_id:
        print(f"⚠️ STM webhook missing token reference | event={event_name} payload_keys={list((payload_obj or {}).keys())}")

    payment_id = payment_entity.get("id") or payment_link_entity.get("payment_id")
    event_seed = "|".join([
        str(event_name or ""),
        str(payload.get("created_at") or ""),
        str(token_id or ""),
        str(payment_id or ""),
    ])
    # WEBHOOK_EVENTS.EVENT_ID is VARCHAR2(64); keep deterministic id compact.
    event_id = hashlib.sha1(event_seed.encode("utf-8")).hexdigest()
    print(f"🧾 STM webhook parsed | event={event_name} token_id={token_id} payment_id={payment_id}")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")

    cursor = conn.cursor()
    try:
        if token_id:
            try:
                cursor.execute(
                    """
                    INSERT INTO WEBHOOK_EVENTS (EVENT_ID, TOKEN_ID, RAW_PAYLOAD, SIGNATURE_VALID)
                    VALUES (%s, %s, %s, 'Y')
                    """,
                    (event_id, token_id, raw_body.decode("utf-8", errors="replace")),
                )
                print(f"✅ WEBHOOK_EVENTS insert | event_id={event_id} token_id={token_id}")
            except Exception as exc:
                if "ORA-00001" in str(exc):
                    print(f"ℹ️ WEBHOOK_EVENTS duplicate ignored | event_id={event_id}")
                else:
                    print(f"⚠️ WEBHOOK_EVENTS insert skipped | event_id={event_id} error={exc}")
        else:
            print(f"ℹ️ WEBHOOK_EVENTS insert skipped | missing token_id event={event_name}")

        if token_id:
            upper_event = event_name.upper()
            if upper_event in {"PAYMENT_LINK.PAID", "PAYMENT.CAPTURED", "PAYMENT.AUTHORIZED"}:
                cursor.execute(
                    """
                    UPDATE PAYMENT_TOKENS
                    SET STATUS='PAID', GATEWAY_TXN_ID=%s, PAID_AT=SYSTIMESTAMP
                    WHERE TOKEN_ID=%s AND STATUS='PENDING'
                    """,
                    (payment_id, token_id),
                )
                print(f"✅ PAYMENT_TOKENS paid update attempted | token_id={token_id} payment_id={payment_id} rows={cursor.rowcount}")

                if payment_id:
                    try:
                        cursor.execute(
                            """
                            INSERT INTO NRM_PAYMENTS
                                (ID, USER_ID, RAZORPAY_PAYMENT_ID, RAZORPAY_ORDER_ID, COURSE, PAYMENT_AMOUNT, CREATED_AT)
                            SELECT
                                (SELECT COALESCE(MAX(ID), 0) + 1 FROM NRM_PAYMENTS),
                                u.ID,
                                %s,
                                t.ARTICLE_ID,
                                t.ARTICLE_ID,
                                t.AMOUNT,
                                CURRENT_TIMESTAMP
                            FROM PAYMENT_TOKENS t
                            JOIN NRM_USERS u
                              ON LOWER(u.EMAIL) = LOWER(t.CLIENT_REFERENCE)
                              OR LOWER(u.USERNAME) = LOWER(t.CLIENT_REFERENCE)
                            LEFT JOIN NRM_PAYMENTS existing
                              ON existing.USER_ID = u.ID AND existing.RAZORPAY_PAYMENT_ID = %s
                            WHERE t.TOKEN_ID = %s
                              AND existing.ID IS NULL
                            """,
                            (payment_id, payment_id, token_id),
                        )
                        print(f"✅ NRM_PAYMENTS insert attempted | token_id={token_id} payment_id={payment_id} rows={cursor.rowcount}")
                    except Exception as np_exc:
                        print(f"⚠️ NRM_PAYMENTS insert skipped | token_id={token_id} payment_id={payment_id} error={np_exc}")
            elif upper_event in {"PAYMENT_LINK.CANCELLED", "PAYMENT.FAILED"}:
                cursor.execute(
                    "UPDATE PAYMENT_TOKENS SET STATUS='FAILED' WHERE TOKEN_ID=%s AND STATUS='PENDING'",
                    (token_id,),
                )
            elif upper_event in {"PAYMENT_LINK.EXPIRED"}:
                cursor.execute(
                    "UPDATE PAYMENT_TOKENS SET STATUS='EXPIRED' WHERE TOKEN_ID=%s AND STATUS='PENDING'",
                    (token_id,),
                )

        conn.commit()
        print(f"✅ STM webhook committed | event={event_name} token_id={token_id}")
        return {"status": "ok", "event": event_name, "token_id": token_id}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        print(f"❌ STM webhook processing error: {exc}")
        raise HTTPException(status_code=500, detail=f"Webhook processing failed: {exc}")
    finally:
        cursor.close()
        conn.close()

# =====================================================
# SHOPPING CART — MODELS & HELPERS
# =====================================================
class CartItem(BaseModel):
    course_id: int
    quantity:  int   = 1
    price:     float


class BillingDetails(BaseModel):
    full_name: str
    email:     str
    mobile:    str
    state:     str = ""
    address:   str = ""


class Financials(BaseModel):
    subtotal:    float
    discount:    float = 0.0
    final_total: float
    # gst_amount removed — no GST charged

class CheckoutPayload(BaseModel):
    billing:        BillingDetails
    financials:     Financials
    items:          List[CartItem]
    payment_method: str



def _validate_total(subtotal: float, discount: float, client_total: float):
    """Server-side sanity check on final total (no GST)."""
    server_total = round(subtotal - discount, 2)
    if abs(client_total - server_total) > 1.0:
        raise HTTPException(
            400,
            f"Total mismatch: sent Rs.{client_total}, expected Rs.{server_total}",
        )
    return server_total

def _make_default_password(mobile: str) -> str:
    """
    Personalized default password for new shop users.
    Format:  Chakora@<last-4-digits-of-mobile>
    e.g.    Chakora@6789
    """
    suffix = (mobile or "").strip()[-4:]
    if len(suffix) < 4:
        suffix = "1234"
    return f"Chakora@{suffix}"

def _generate_order_id() -> str:
    return "CHK-" + "".join(random.choices(string.digits, k=8))


def _generate_payment_id() -> str:
    return "PAY-" + uuid.uuid4().hex[:12].upper()

# =====================================================
# KAFKA CONSUMER — shop_payment.created (background thread)
# =====================================================
def _run_payment_created_consumer():
    import time
    print("🚀 billing_service payment_created consumer thread starting…")
    consumer = None
    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                "shop_payment.created",
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                group_id="billing-payment-created-consumer",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                session_timeout_ms=30000,
            )
            print("✅ billing_service payment_created consumer connected")
            break
        except Exception as e:
            print(f"⚠️  payment_created consumer connect attempt {attempt}/5: {e}")
            time.sleep(5)
    else:
        print("❌ payment_created consumer could not connect. Thread exiting.")
        return

    while True:
        try:
            records = consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    payload = msg.value or {}
                    order_id   = payload.get("order_id")
                    payment_id = payload.get("payment_id")
                    amount     = payload.get("amount")
                    status     = payload.get("payment_status", "PENDING")
                    print(f"📩 shop_payment.created | order={order_id} payment={payment_id} amount={amount} status={status}")
                    # Update PAYMENTS table to confirm PENDING state is recorded
                    conn = get_db_connection()
                    if conn:
                        cur = conn.cursor()
                        try:
                            cur.execute(
                                "UPDATE PAYMENTS SET PAYMENT_STATUS=%s, UPDATED_AT=CURRENT_TIMESTAMP "
                                "WHERE PAYMENT_ID=%s AND PAYMENT_STATUS='PENDING'",
                                (status, payment_id)
                            )
                            conn.commit()
                            print(f"✅ PAYMENTS acknowledged PENDING for payment_id={payment_id}")
                        except Exception as e:
                            print(f"❌ payment_created consumer DB error: {e}")
                        finally:
                            cur.close()
                            conn.close()
                    consumer.commit()
        except Exception as e:
            print(f"⚠️  shop_payment.created consumer poll error: {e}")
            time.sleep(3)

# =====================================================
# WHOAMI
# =====================================================
@app.get("/whoami")
def whoami(user=Depends(get_current_user)):
    return {"username": user["username"], "role": user["role"], "authenticated": True}


# =====================================================
# RAZORPAY HELPERS + ENDPOINTS (shared for meeting flow)
# =====================================================
RAZORPAY_ORDERS_URL = "https://api.razorpay.com/v1/orders"


def _verify_razorpay_signature(order_id: str, payment_id: str, signature: str) -> bool:
    secret = (os.getenv("RZP_KEY_SECRET") or "").strip()
    if not secret:
        return False
    message = f"{order_id}|{payment_id}".encode("utf-8")
    expected = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return hmac.compare_digest(expected, signature)


@app.post("/payment/create-order")
def payment_create_order(payload: PaymentCreateOrderRequest):
    amount = int(payload.amount or 0)
    currency = (payload.currency or "INR").strip() or "INR"
    notes = (payload.notes or "Meeting Booking").strip() or "Meeting Booking"

    if amount <= 0:
        raise HTTPException(status_code=400, detail="amount must be greater than 0 (in paise)")

    key_id = (os.getenv("RZP_KEY_ID") or "").strip()
    key_secret = (os.getenv("RZP_KEY_SECRET") or "").strip()
    if not key_id or not key_secret:
        raise HTTPException(status_code=500, detail="Razorpay credentials are not configured")

    try:
        response = http_requests.post(
            RAZORPAY_ORDERS_URL,
            json={
                "amount": amount,
                "currency": currency,
                "notes": {"description": notes},
            },
            auth=(key_id, key_secret),
            headers={"Content-Type": "application/json"},
            timeout=12,
        )
    except http_requests.RequestException as exc:
        print(f"❌ Razorpay order request failed: {exc}")
        raise HTTPException(status_code=502, detail="Could not reach Razorpay")
    print("RAZORPAY STATUS:", response.status_code)
    print("RAZORPAY RESPONSE:", response.text)
    if not response.ok:
        try:
            err_payload = response.json()
        except Exception:
            err_payload = {"raw": response.text}
        raise HTTPException(
            status_code=response.status_code,
            detail={
                "success": False,
                "message": "Could not create Razorpay order",
                "razorpay_error": err_payload,
            },
        )

    try:
        order = response.json() or {}
    except Exception as exc:
        print(f"❌ Razorpay response parse error: {exc}; raw={response.text[:300]}")
        raise HTTPException(status_code=502, detail="Invalid response from Razorpay")

    # ── KAFKA: publish payment.created ─────────────────────────
    _kafka_publish("payment.created", {
        "correlation_id": str(uuid.uuid4()),
        "order_id":       order.get("id"),
        "amount":         order.get("amount", amount),
        "currency":       order.get("currency", currency),
        "notes":          notes,
        "timestamp":      datetime.utcnow().isoformat(),
    })

    return {
        "success": True,
        "order_id": order.get("id"),
        "amount": order.get("amount", amount),
        "currency": order.get("currency", currency),
        "payment_key_id": key_id,
    }


@app.post("/payment/verify")
def payment_verify(payload: PaymentVerifyRequest):
    order_id = (payload.order_id or "").strip()
    payment_id = (payload.payment_id or "").strip()
    signature = (payload.signature or "").strip()

    if not order_id or not payment_id or not signature:
        raise HTTPException(status_code=400, detail="order_id, payment_id and signature are required")

    if not _verify_razorpay_signature(order_id, payment_id, signature):
        raise HTTPException(status_code=400, detail="Payment verification failed")

    if not _verify_razorpay_signature(order_id, payment_id, signature):
        raise HTTPException(status_code=400, detail="Payment verification failed")

    # ── KAFKA: publish payment.completed ───────────────────────
    _kafka_publish("payment.completed", {
        "correlation_id": str(uuid.uuid4()),
        "order_id":       order_id,
        "payment_id":     payment_id,
        "status":         "captured",
        "timestamp":      datetime.utcnow().isoformat(),
    })

    return {
        "success":    True,
        "message":    "Payment signature verified",
        "order_id":   order_id,
        "payment_id": payment_id,
    }

# =====================================================
# MEETING BOOKING EMAIL
# =====================================================
@app.post("/meeting/send-booking-email")
def send_meeting_booking_email(req: BookingEmailRequest):
    recipient = (req.student_email or "").strip().lower()
    if not recipient:
        return {"success": False, "message": "student_email is required"}

    subject = f"Booking Confirmed – {req.date} at {req.start_time}"
    text_body = (
        f"Dear {req.student_name},\n\n"
        f"Your ChakoraHub session has been booked successfully.\n\n"
        f"Booking ID  : {req.booking_id}\n"
        f"Date        : {req.date}\n"
        f"Time        : {req.start_time}\n"
        f"Duration    : {req.duration_minutes} minutes\n"
        f"Complexity  : {req.complexity}\n"
        f"Amount      : \u20b9{req.price}\n"
        + (f"Purpose     : {req.purpose}\n" if req.purpose else "")
        + (f"Payment ID  : {req.payment_id}\n" if req.payment_id else "")
        + (f"Order ID    : {req.order_id}\n" if req.order_id else "")
        + (f"Teams Link  : {req.meeting_link}\n" if req.meeting_link else "")
        + "\nRegards,\nChakoraHub Team"
    )
    # ── HTML Email ──────────────────────────────────────────────────
    html_body = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"></head>
<body style="margin:0;padding:0;background:#f0f4f8;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:32px 16px;">
      <table width="520" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 4px 24px rgba(0,0,0,0.08);">

        <!-- Header -->
        <tr><td style="background:linear-gradient(135deg,#1a237e,#1565c0);padding:36px 40px;text-align:center;">
          <div style="font-size:32px;">🎓</div>
          <h1 style="margin:12px 0 4px;color:#ffffff;font-size:22px;letter-spacing:0.5px;">Booking Confirmed!</h1>
          <p style="margin:0;color:#90caf9;font-size:13px;">Your ChakoraHub session is all set</p>
        </td></tr>

        <!-- Greeting -->
        <tr><td style="padding:28px 40px 8px;">
          <p style="margin:0;font-size:15px;color:#37474f;">Hi <strong>{req.student_name}</strong>, your session has been booked successfully. Here are the details:</p>
        </td></tr>

        <!-- Details card -->
        <tr><td style="padding:16px 40px;">
          <table width="100%" cellpadding="0" cellspacing="0" style="background:#f5f7ff;border-radius:10px;border-left:4px solid #1565c0;">
            <tr><td style="padding:20px 24px;">
              {''.join(f'<p style="margin:0 0 10px;font-size:14px;color:#546e7a;"><span style="display:inline-block;width:130px;font-weight:600;color:#37474f;">{k}</span>{v}</p>' for k, v in [
                  ("📅  Date",          req.date),
                  ("🕐  Time",          req.start_time),
                  ("⏱  Duration",       f"{req.duration_minutes} minutes"),
                  ("📊  Complexity",     req.complexity),
                  ("💰  Amount",         f"₹{req.price}"),
              ] + ([("🎯  Purpose", req.purpose)] if req.purpose else [])
                + ([("🔗  Teams Link", f'<a href="{req.meeting_link}" style="color:#1565c0;">Join Meeting</a>')] if req.meeting_link else [])
              )}
              <hr style="border:none;border-top:1px solid #dce3f0;margin:12px 0;">
              <p style="margin:0 0 6px;font-size:12px;color:#90a4ae;"><strong>Booking ID:</strong> {req.booking_id}</p>
              {'<p style="margin:0 0 6px;font-size:12px;color:#90a4ae;"><strong>Payment ID:</strong> ' + req.payment_id + '</p>' if req.payment_id else ''}
              {'<p style="margin:0;font-size:12px;color:#90a4ae;"><strong>Order ID:</strong> ' + req.order_id + '</p>' if req.order_id else ''}
            </td></tr>
          </table>
        </td></tr>

        <!-- Footer -->
        <tr><td style="padding:24px 40px 32px;text-align:center;">
          <p style="margin:0 0 4px;font-size:13px;color:#90a4ae;">Questions? Reply to this email or write to</p>
          <a href="mailto:support@chakorahub.com" style="color:#1565c0;font-size:13px;">support@chakorahub.com</a>
          <p style="margin:20px 0 0;font-size:12px;color:#b0bec5;">© 2026 ChakoraHub · All rights reserved</p>
        </td></tr>

      </table>
    </td></tr>
  </table>
</body></html>"""

    destination = {
        "ToAddresses": [recipient],
        "CcAddresses": ["admin@chakorahub.com"] if recipient != "admin@chakorahub.com" else [],
    }

    try:
        ses.send_email(
            Source="admin@chakorahub.com",
            Destination=destination,
            Message={
                "Subject": {"Data": subject},
                "Body": {
                    "Html": {"Data": html_body},   # ← add this
                    "Text": {"Data": text_body},   # ← keep as fallback
                },
            },
        )
        print(f"✅ Booking email sent to {recipient} | booking_id={req.booking_id}")
        return {"success": True, "message": "Booking emails sent"}
    except ClientError as exc:
        err = exc.response.get("Error", {}) if hasattr(exc, "response") else {}
        code = err.get("Code", "Unknown")
        message = err.get("Message", str(exc))
        print(f"❌ SES send_email failed | booking_id={req.booking_id} recipient={recipient} code={code} message={message}")
        return {"success": False, "message": f"Email delivery failed: {code}", "details": message}
    except Exception as exc:
        print(f"❌ Email send unexpected failure | booking_id={req.booking_id} recipient={recipient} error={exc}")
        return {"success": False, "message": "Email delivery failed", "details": str(exc)}


# =====================================================
# CREATE BILLING ENTRY
# Cache evictions on write:
#   billing:history:{phone}           → evicted immediately
#   invoice:{transaction_id}          → set after insert (short TTL)
#   payment_status:{transaction_id}   → set after insert (short TTL)
# =====================================================
@app.post("/billing-create")
async def create_billing(
    billing_type: str = Form(...),
    billing_category: str = Form(...),
    payment_method: str = Form(...),
    amount: float = Form(...),
    phone: str = Form(...),
    currency: str = Form("INR"),
    upi_txn_id: Optional[str] = Form(None),
    payment_stage: str = Form(...),          # "initial" | "final"
    initial_payment: Optional[str] = Form(None),
    receipt_file: Optional[UploadFile] = File(None),
    user=Depends(get_current_user),
):
    print(f"\n{'='*60}\n💵 CREATE BILLING REQUEST\n{'='*60}")
    conn = None
    cursor = None
    receipt_s3_path = None

    try:
        initial_payment_json = None
        initial_payment_str  = None
        if initial_payment:
            initial_payment_json = json.loads(initial_payment)
            initial_payment_str  = json.dumps(initial_payment_json)

        if receipt_file and receipt_file.filename:
            print(f"📤 Uploading receipt: {receipt_file.filename}")
            receipt_s3_path = upload_receipt_to_s3(receipt_file, phone)

        # ── Resolve user (cache: user:phone:{phone}) ──────────────────
        user_cache_key = f"user:phone:{phone}"
        cached_user    = cache_get(user_cache_key)

        if cached_user:
            user_id    = cached_user["user_id"]
            user_name  = cached_user["username"]
            user_email = cached_user["email"]
            conn       = get_db_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            cursor = conn.cursor(DictCursor)
        else:
            conn = get_db_connection()
            if not conn:
                raise HTTPException(status_code=500, detail="Database connection failed")
            cursor = conn.cursor(DictCursor)

            cursor.execute(
                "SELECT ID, USERNAME, EMAIL FROM NRM_USERS WHERE PHONE = %s FETCH FIRST 1 ROWS ONLY",
                (phone,),
            )
            user_row = cursor.fetchone()
            if not user_row:
                raise HTTPException(status_code=404, detail=f"User with phone {phone} not found")

            user_id    = user_row["ID"]
            user_name  = user_row["USERNAME"]
            user_email = user_row["EMAIL"]

            cache_set(user_cache_key, {"user_id": user_id, "username": user_name, "email": user_email},
                      ttl=TTL_USER_PHONE)

        # ── Get / create customer ──────────────────────────────────────
        cursor.execute("SELECT CUSTOMER_ID FROM NRM_BILLING WHERE PHONE = %s FETCH FIRST 1 ROWS ONLY", (phone,))
        customer = cursor.fetchone()

        if customer:
            customer_id = customer["CUSTOMER_ID"]
        else:
            cursor.execute(
                "INSERT INTO NRM_BILLING (CUSTOMER_TYPE, NAME, PHONE, EMAIL, CREATED_AT) "
                "VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP)",
                (billing_type, user_name, phone, user_email),
            )
            conn.commit()
            cursor.execute("SELECT MAX(CUSTOMER_ID) as CID FROM NRM_BILLING WHERE PHONE = %s", (phone,))
            customer_id = cursor.fetchone()["CID"]

        # ── INITIAL PAYMENT ────────────────────────────────────────────
        if payment_stage == "initial":
            transaction_uuid = str(uuid.uuid4())
            cursor.execute(
                """
                INSERT INTO BILLING_TRANSACTIONS
                (TRANSACTION_UUID, CUSTOMER_ID, BILLING_CATEGORY,
                 PAYMENT_METHOD, UPI_TXN_ID, AMOUNT, CURRENCY,
                 PHONE, RECEIPT_FILE_PATH, STATUS,
                 INITIAL_PAYMENT, CREATED_AT)
                SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s,
                       'PENDING', %s, CURRENT_TIMESTAMP
                """,
                (transaction_uuid, customer_id, billing_category, payment_method,
                 upi_txn_id, amount, currency, phone, receipt_s3_path, initial_payment_str),
            )
            conn.commit()

            cursor.execute(
                "SELECT TRANSACTION_ID FROM BILLING_TRANSACTIONS WHERE TRANSACTION_UUID = %s",
                (transaction_uuid,),
            )
            transaction_id = cursor.fetchone()["TRANSACTION_ID"]

        # ── FINAL PAYMENT ──────────────────────────────────────────────
        elif payment_stage == "final":
            cursor.execute(
                "SELECT TRANSACTION_ID FROM BILLING_TRANSACTIONS "
                "WHERE PHONE = %s ORDER BY CREATED_AT DESC FETCH FIRST 1 ROWS ONLY",
                (phone,),
            )
            existing = cursor.fetchone()
            if not existing:
                raise HTTPException(status_code=404, detail="Initial payment not found")

            transaction_id = existing["TRANSACTION_ID"]
            cursor.execute(
                "UPDATE BILLING_TRANSACTIONS "
                "SET FINAL_PAYMENT = CURRENT_TIMESTAMP, STATUS = 'COMPLETED', "
                "    UPDATED_AT = CURRENT_TIMESTAMP "
                "WHERE TRANSACTION_ID = %s",
                (transaction_id,),
            )
            conn.commit()
            transaction_uuid = None
        else:
            raise HTTPException(status_code=400, detail="Invalid payment stage. Use 'initial' or 'final'.")

        # ── Insert NRM_BILLING_ENTRIES ─────────────────────────────────
        cursor.execute("SELECT ID FROM NRM_PAYMENT_STATUSES WHERE UPPER(STATUS) = 'PENDING' FETCH FIRST 1 ROWS ONLY")
        status_row = cursor.fetchone()
        status_id  = status_row["ID"] if status_row else 1

        cursor.execute(
            "INSERT INTO NRM_BILLING_ENTRIES "
            "(ID, USER_ID, UPI_ID, AMOUNT, DISCOUNT, STATUS_ID, BILLING_TIMESTAMP) "
            "VALUES (NRM_BILLING_SEQ.NEXTVAL, %s, %s, %s, 0, %s, CURRENT_TIMESTAMP)",
            (user_id, upi_txn_id, amount, status_id),
        )
        conn.commit()

        # ── Cache invoice + payment_status (short TTL) ─────────────────
        invoice_data = {
            "transaction_id":   transaction_id,
            "transaction_uuid": transaction_uuid,
            "phone":            phone,
            "amount":           amount,
            "payment_stage":    payment_stage,
            "status":           "PENDING" if payment_stage == "initial" else "COMPLETED",
            "created_at":       datetime.utcnow().isoformat(),
        }
        cache_set(f"invoice:{transaction_id}",        invoice_data, ttl=TTL_INVOICE)
        cache_set(f"payment_status:{transaction_id}", invoice_data["status"], ttl=TTL_PAYMENT_STATUS)

        # ── Evict stale billing history for this phone ─────────────────
        cache_delete(f"billing:history:{phone}")

        # ── Email (initial only) ────────────────────────────────────────
        if payment_stage == "initial":
            course_name = initial_payment_json.get("course") if initial_payment_json else None
            send_billing_email(
                user_name=user_name,
                user_email=user_email,
                phone=phone,
                course_name=course_name,
                amount=amount,
                payment_method=payment_method,
                transaction_uuid=transaction_uuid,
            )

        return {
            "status":           "success",
            "message":          f"{payment_stage.capitalize()} payment processed successfully",
            "transaction_id":   transaction_id,
            "transaction_uuid": transaction_uuid,
            "amount":           amount,
        }

    except HTTPException:
        if conn:
            conn.rollback()
        raise
    except Exception as exc:
        if conn:
            conn.rollback()
        print(f"❌ Error: {exc}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


# =====================================================
# STUDENT PAYMENT INFO
# Not cached — caller needs live status at all times.
# (Small queries; Oracle handles this fine.)
# =====================================================
@app.get("/student-payment-info")
def get_student_payment_info(phone: str, user=Depends(get_current_user)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            "SELECT INITIAL_PAYMENT, FINAL_PAYMENT, AMOUNT, STATUS "
            "FROM BILLING_TRANSACTIONS WHERE PHONE = %s ORDER BY CREATED_AT DESC FETCH FIRST 1 ROWS ONLY",
            (phone,),
        )
        row = cursor.fetchone()

        if not row:
            return {"status": "NOT_FOUND"}

        if row["FINAL_PAYMENT"] is not None or row["STATUS"] == "COMPLETED":
            return {
                "status": "FULLY_PAID",
                "course": None,
                "total_fee": row["AMOUNT"] * 2,
                "due_amount": 0,
                "next_amount": 0,
                "next_stage": "",
            }

        initial_payment = row["INITIAL_PAYMENT"]
        if initial_payment:
            try:
                parsed         = json.loads(initial_payment)
                course         = parsed.get("course")
                initial_amount = parsed.get("amount")
            except Exception:
                course         = None
                initial_amount = row["AMOUNT"]

            total_fee  = initial_amount * 2
            due_amount = total_fee - initial_amount
            return {
                "status":       "PARTIAL",
                "course":       course,
                "total_fee":    total_fee,
                "due_amount":   due_amount,
                "next_amount":  due_amount,
                "next_stage":   "final",
            }

        return {"status": "NOT_FOUND"}

    except Exception as exc:
        print("❌ student-payment-info error:", exc)
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        cursor.close()
        conn.close()


# =====================================================
# BILLING HISTORY  (cache: billing:history:{phone})
# Evicted on every create / approve mutation.
# =====================================================
@app.get("/billing-history")
async def get_billing_history(phone: str, user=Depends(get_current_user)):
    cache_key     = f"billing:history:{phone}"
    cached_result = cache_get(cache_key)
    if cached_result is not None:
        print(f"✅ Cache hit: billing history for {phone}")
        return cached_result

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute("SELECT ID FROM NRM_USERS WHERE PHONE = %s FETCH FIRST 1 ROWS ONLY", (phone,))
        user_row = cursor.fetchone()
        if not user_row:
            raise HTTPException(status_code=404, detail="User not found")

        user_id = user_row["ID"]
        cursor.execute(
            """
            SELECT
                be.ID, be.USER_ID, be.COURSE_ID, be.UPI_ID,
                be.AMOUNT, be.DISCOUNT, be.BILLING_TIMESTAMP,
                ps.STATUS as STATUS_NAME
            FROM NRM_BILLING_ENTRIES be
            LEFT JOIN NRM_PAYMENT_STATUSES ps ON be.STATUS_ID = ps.ID
            WHERE be.USER_ID = %s
            ORDER BY be.BILLING_TIMESTAMP DESC
            """,
            (user_id,),
        )
        entries = cursor.fetchall()
        result  = {"status": "success", "entries": entries}
        cache_set(cache_key, result, ttl=TTL_BILLING_HISTORY)
        return result
    finally:
        cursor.close()
        conn.close()


# =====================================================
# INVOICE DETAIL  (cache: invoice:{transaction_id})
# =====================================================
@app.get("/invoice/{transaction_id}")
def get_invoice(transaction_id: int, user=Depends(get_current_user)):
    cache_key     = f"invoice:{transaction_id}"
    cached_result = cache_get(cache_key)
    if cached_result is not None:
        print(f"✅ Cache hit: invoice:{transaction_id}")
        return {"status": "success", "source": "cache", "invoice": cached_result}

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT bt.*, nb.NAME, nb.EMAIL, nb.PHONE as CUSTOMER_PHONE
            FROM BILLING_TRANSACTIONS bt
            JOIN NRM_BILLING nb ON bt.CUSTOMER_ID = nb.CUSTOMER_ID
            WHERE bt.TRANSACTION_ID = %s
            """,
            (transaction_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Invoice not found")

        cache_set(cache_key, row, ttl=TTL_INVOICE)
        return {"status": "success", "source": "db", "invoice": row}
    finally:
        cursor.close()
        conn.close()


# =====================================================
# PAYMENT STATUS  (cache: payment_status:{transaction_id})
# =====================================================
@app.get("/payment-status/{transaction_id}")
def get_payment_status(transaction_id: int, user=Depends(get_current_user)):
    cache_key     = f"payment_status:{transaction_id}"
    cached_status = cache_get(cache_key)
    if cached_status is not None:
        print(f"✅ Cache hit: payment_status:{transaction_id}")
        return {"status": "success", "source": "cache", "payment_status": cached_status}

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            "SELECT TRANSACTION_ID, STATUS, AMOUNT, PHONE, CREATED_AT, UPDATED_AT "
            "FROM BILLING_TRANSACTIONS WHERE TRANSACTION_ID = %s",
            (transaction_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Transaction not found")

        cache_set(cache_key, row, ttl=TTL_PAYMENT_STATUS)
        return {"status": "success", "source": "db", "payment_status": row}
    finally:
        cursor.close()
        conn.close()


# =====================================================
# COURSES  (cache: courses:all)
# =====================================================
@app.get("/courses")
async def get_courses(user=Depends(get_current_user)):
    cache_key     = "courses:all"
    cached_result = cache_get(cache_key)
    if cached_result is not None:
        print("✅ Cache hit: courses:all")
        return cached_result

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute("SELECT ID, COURSE_NAME, DESCRIPTION FROM NRM_COURSES ORDER BY COURSE_NAME")
        courses = cursor.fetchall()
        result  = {"status": "success", "courses": courses}
        cache_set(cache_key, result, ttl=TTL_COURSES)
        return result
    except Exception as exc:
        print(f"❌ Error fetching courses: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        cursor.close()
        conn.close()


# =====================================================
# MY BILLINGS (STUDENT)
# =====================================================
@app.get("/billing-mybillings")
def my_billings(user=Depends(get_current_user)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """
            SELECT
                bt.TRANSACTION_ID, bt.TRANSACTION_UUID, bt.BILLING_TYPE,
                bt.BILLING_CATEGORY, bt.PAYMENT_METHOD, bt.AMOUNT,
                bt.CURRENCY, bt.STATUS, bt.CREATED_AT, bt.UPI_TXN_ID
            FROM BILLING_TRANSACTIONS bt
            JOIN NRM_BILLING nb ON bt.CUSTOMER_ID = nb.CUSTOMER_ID
            WHERE nb.PHONE IN (
                SELECT PHONE FROM NRM_USERS
                WHERE EMAIL = %(username)s OR USERNAME = %(username)s
            )
            ORDER BY bt.CREATED_AT DESC
            """,
            {"username": user["username"]},
        )
        transactions = cursor.fetchall()
        return {"status": "success", "count": len(transactions), "transactions": transactions}
    except Exception as exc:
        print(f"❌ List user billing error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        cursor.close()
        conn.close()


# =====================================================
# APPROVE / UPDATE STATUS (ADMIN)
# Cache evictions:
#   payment_status:{transaction_id}  → evicted (status changed)
#   invoice:{transaction_id}         → evicted (status changed)
#   billing:history:{phone}          → pattern-evicted (admin doesn't know phone easily)
# =====================================================
@app.put("/billing-approve")
def approve_billing(billing_id: int, status_data: StatusUpdate, user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")

    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        # Fetch phone before update so we can evict billing:history
        cursor.execute(
            "SELECT PHONE FROM BILLING_TRANSACTIONS WHERE TRANSACTION_ID = %s", (billing_id,)
        )
        tx_row = cursor.fetchone()
        phone  = tx_row["PHONE"] if tx_row else None

        cursor.execute(
            "UPDATE BILLING_TRANSACTIONS "
            "SET STATUS = %(status)s, UPDATED_AT = CURRENT_TIMESTAMP "
            "WHERE TRANSACTION_ID = %(tid)s",
            {"status": status_data.status, "tid": billing_id},
        )
        if cursor.rowcount == 0:
            raise HTTPException(status_code=404, detail="Transaction not found")

        cursor.execute(
            "SELECT ID FROM NRM_PAYMENT_STATUSES WHERE UPPER(STATUS) = UPPER(%(status)s) FETCH FIRST 1 ROWS ONLY",
            {"status": status_data.status},
        )
        status_row = cursor.fetchone()
        if status_row:
            cursor.execute(
                """
                UPDATE NRM_BILLING_ENTRIES
                SET STATUS_ID = %(sid)s
                WHERE USER_ID IN (
                    SELECT u.ID FROM NRM_USERS u
                    JOIN NRM_BILLING nb ON u.PHONE = nb.PHONE
                    JOIN BILLING_TRANSACTIONS bt ON nb.CUSTOMER_ID = bt.CUSTOMER_ID
                    WHERE bt.TRANSACTION_ID = %(tid)s
                )
                """,
                {"sid": status_row["ID"], "tid": billing_id},
            )
        conn.commit()

        # Evict stale cache entries
        cache_delete(f"payment_status:{billing_id}")
        cache_delete(f"invoice:{billing_id}")
        if phone:
            cache_delete(f"billing:history:{phone}")

        return {"status": "success", "message": f"Transaction {billing_id} updated to {status_data.status}"}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as exc:
        conn.rollback()
        print(f"❌ Update status error: {exc}")
        raise HTTPException(status_code=500, detail=str(exc))
    finally:
        cursor.close()
        conn.close()


# =====================================================
# ADMIN: CLEAR BILLING CACHE
# Only clears billing-namespace keys. Never flushdb —
# that would wipe sessions/auth/profiles for all services.
# =====================================================
@app.post("/clear-cache")
async def clear_cache(pattern: Optional[str] = None, user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Admin only")
    try:
        # Enforce billing-namespace prefix so we never accidentally
        # wipe keys owned by student_service, employee_service, etc.
        billing_prefixes = ["billing:", "invoice:", "payment_status:", "courses:", "user:phone:"]
        if pattern:
            # Validate that the supplied pattern starts with a billing prefix
            is_billing_pattern = any(pattern.startswith(p) or p.startswith(pattern.split(":")[0]) for p in billing_prefixes)
            if not is_billing_pattern:
                raise HTTPException(
                    status_code=400,
                    detail=f"Pattern must be scoped to billing namespace. Valid prefixes: {billing_prefixes}",
                )
            cache_delete_pattern(pattern)
            return {"status": "success", "message": f"Cleared billing cache matching: {pattern}"}
        else:
            # Clear all known billing-namespace patterns
            for prefix in billing_prefixes:
                cache_delete_pattern(f"{prefix}*")
            return {"status": "success", "message": "All billing-namespace cache cleared"}
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# =====================================================
# PDF INVOICE GENERATOR
# =====================================================
def _generate_invoice_pdf(
    order_id, payment_id, billing, items, financials,
    login_email="", default_password=None,
) -> Optional[bytes]:
    """
    Compact A4-width invoice PDF — same navy/blue corporate theme as the
    Chakora Hub employee payslip (see app.py: generate_payslip_pdf).
    Page height is sized to content (no login/credentials block — that
    lives in the email body only). Returns raw bytes, or None if
    reportlab is unavailable.
    """
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib import colors
        from reportlab.lib.units import mm
        from reportlab.platypus import (
            SimpleDocTemplate, Table, TableStyle,
            Paragraph, Spacer,
        )
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT
    except ImportError:
        print("⚠️  reportlab not installed — PDF invoice skipped")
        return None

    buffer = BytesIO()

    A4_W, A4_H = A4
    LM = RM = 10 * mm
    TM = BM = 8 * mm
    PAGE_W = A4_W
    UW = PAGE_W - LM - RM
    HEADER_H = 24 * mm   # extra top margin reserved for the header band
    FOOTER_H = 4 * mm    # extra bottom margin reserved for the footer line

    NAVY        = colors.HexColor("#0d2b55")
    NAVY_MID    = colors.HexColor("#1a3c6e")
    BLUE        = colors.HexColor("#2e86de")
    BLUE_LIGHT  = colors.HexColor("#e8f2fc")
    SILVER      = colors.HexColor("#f4f6fb")
    BORDER_C    = colors.HexColor("#cdd5e0")
    WHITE       = colors.white
    DARK_TEXT   = colors.HexColor("#161c2d")
    MID_TEXT    = colors.HexColor("#4a5568")
    GREEN_TEXT  = colors.HexColor("#065f46")

    COMPANY_NAME = "Chakora Hub"
    LOGO_PATH = os.getenv(
        "CHAKORA_LOGO_PATH",
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", "logo.png"),
    )

    story = []

    def S(name, size, color=DARK_TEXT, font="Helvetica", align=TA_LEFT, leading=None):
        kw = dict(fontSize=size, textColor=color, fontName=font, alignment=align)
        if leading:
            kw["leading"] = leading
        return ParagraphStyle(name, **kw)

    def T(data, widths, cmds):
        t = Table(data, colWidths=widths)
        t.setStyle(TableStyle(cmds))
        return t

    def P(text, style):
        return Paragraph(_esc(str(text)) if text is not None else "", style)

    def money(v):
        try:
            return f"Rs. {float(v):,.2f}"
        except Exception:
            return "Rs. 0.00"

    def sec_bar(title):
        return T(
            [[P(f"  {title}", S("sb", 7.5, WHITE, "Helvetica-Bold"))]], [UW],
            [("BACKGROUND", (0, 0), (-1, -1), NAVY_MID),
             ("TOPPADDING", (0, 0), (-1, -1), 5), ("BOTTOMPADDING", (0, 0), (-1, -1), 5),
             ("LEFTPADDING", (0, 0), (-1, -1), 0), ("RIGHTPADDING", (0, 0), (-1, -1), 0)],
        )

    lbl_s   = S("lbl",  8,   BLUE,      "Helvetica-Bold")
    val_s   = S("val",  8.5, NAVY,      "Helvetica-Bold")
    det_lbl = S("dl",   7,   MID_TEXT,  "Helvetica-Bold")
    det_val = S("dv",   7,   DARK_TEXT, "Helvetica")
    th_s    = S("ths",  7,   WHITE,     "Helvetica-Bold", TA_CENTER)
    th_r    = S("thr",  7,   WHITE,     "Helvetica-Bold", TA_RIGHT)
    dc_l    = S("dcl",  7.5, DARK_TEXT, "Helvetica")
    dc_r    = S("dcr",  7.5, DARK_TEXT, "Helvetica",      TA_RIGHT)
    net_l   = S("nl",   10,  WHITE,     "Helvetica-Bold")
    net_r   = S("nr",   10,  WHITE,     "Helvetica-Bold", TA_RIGHT)
    note_s  = S("note", 7,   MID_TEXT,  "Helvetica-Oblique", TA_CENTER)

    period_style = [
        ("BACKGROUND",    (0, 0), (-1, -1), BLUE_LIGHT),
        ("BOX",           (0, 0), (-1, -1), 0.5, BORDER_C),
        ("TOPPADDING",    (0, 0), (-1, -1), 6), ("BOTTOMPADDING", (0, 0), (-1, -1), 6),
        ("LEFTPADDING",   (0, 0), (-1, -1), 10), ("RIGHTPADDING", (0, 0), (-1, -1), 10),
        ("VALIGN",        (0, 0), (-1, -1), "MIDDLE"),
    ]
    order_row = [[
        P("ORDER DATE", lbl_s), P(datetime.now().strftime("%d %b %Y"), val_s),
        P("PAYMENT ID", lbl_s), P(payment_id, val_s),
        P("STATUS", lbl_s),
        Paragraph('<font color="#065f46"><b>PAID</b></font>', S("ps", 8, GREEN_TEXT, "Helvetica-Bold")),
    ]]
    story.append(T(order_row, [UW * 0.14, UW * 0.18, UW * 0.15, UW * 0.27, UW * 0.11, UW * 0.15], period_style))
    story.append(Spacer(1, 1.5 * mm))

    full_name = billing.get("full_name") or "Customer"
    b_email   = billing.get("email")    or ""
    mobile    = billing.get("mobile")   or ""
    address   = billing.get("address")  or ""
    state     = billing.get("state")    or ""
    addr_full = address + (f", {state}" if state else "")

    story.append(sec_bar("BILL TO"))
    story.append(Spacer(1, 0.3 * mm))
    bill_rows = [
        [P("Name", det_lbl),   P(full_name, det_val),     P("Email", det_lbl),   P(b_email or "—", det_val)],
        [P("Mobile", det_lbl), P(mobile or "—", det_val), P("Address", det_lbl), P(addr_full or "—", det_val)],
    ]
    story.append(T(
        bill_rows, [UW * 0.16, UW * 0.34, UW * 0.16, UW * 0.34],
        [("BOX", (0, 0), (-1, -1), 0.5, BORDER_C), ("INNERGRID", (0, 0), (-1, -1), 0.3, BORDER_C),
         ("ROWBACKGROUNDS", (0, 0), (-1, -1), [SILVER, WHITE]),
         ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
         ("LEFTPADDING", (0, 0), (-1, -1), 8), ("RIGHTPADDING", (0, 0), (-1, -1), 8),
         ("VALIGN", (0, 0), (-1, -1), "MIDDLE")],
    ))
    story.append(Spacer(1, 1.5 * mm))

    story.append(sec_bar("ITEMS PURCHASED"))
    story.append(Spacer(1, 0.3 * mm))

    item_cw = [UW * 0.08, UW * 0.44, UW * 0.12, UW * 0.18, UW * 0.18]
    item_rows = [[P("#", th_s), P("COURSE / ITEM", th_s), P("QTY", th_s), P("UNIT PRICE", th_r), P("AMOUNT", th_r)]]
    for idx, it in enumerate(items, 1):
        name  = it.get("name") or it.get("course_name") or "Course"
        qty   = int(it.get("quantity") or it.get("qty") or 1)
        price = float(it.get("price") or it.get("unit_price") or 0)
        amt   = float(it.get("amount") or (price * qty))
        item_rows.append([P(str(idx), dc_l), P(name, dc_l), P(str(qty), dc_r),
                           P(money(price), dc_r), P(money(amt), dc_r)])
    if len(item_rows) == 1:
        item_rows.append([P("", dc_l), P("No items recorded", dc_l), P("", dc_r), P("", dc_r), P("", dc_r)])

    item_style = [
        ("BOX", (0, 0), (-1, -1), 0.7, NAVY_MID), ("INNERGRID", (0, 0), (-1, -1), 0.3, BORDER_C),
        ("BACKGROUND", (0, 0), (-1, 0), NAVY_MID),
        ("ROWBACKGROUNDS", (0, 1), (-1, -1), [SILVER, WHITE]),
        ("TOPPADDING", (0, 0), (-1, -1), 4), ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 7), ("RIGHTPADDING", (0, 0), (-1, -1), 7),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("ALIGN", (0, 1), (0, -1), "CENTER"), ("ALIGN", (2, 1), (2, -1), "CENTER"),
    ]
    story.append(T(item_rows, item_cw, item_style))
    story.append(Spacer(1, 1 * mm))

    subtotal = float(financials.get("subtotal") or 0)
    discount = float(financials.get("discount") or 0)
    total    = float(financials.get("final_total") or 0)
    tot_rows = [[P("Subtotal", dc_l), P(money(subtotal), dc_r)]]
    if discount > 0:
        tot_rows.append([P("Discount", dc_l), P(f"- {money(discount)}", dc_r)])
    story.append(T(
        tot_rows, [UW - 55 * mm, 55 * mm],
        [("ALIGN", (0, 0), (-1, -1), "RIGHT"),
         ("TOPPADDING", (0, 0), (-1, -1), 3), ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
         ("LEFTPADDING", (0, 0), (-1, -1), 6), ("RIGHTPADDING", (0, 0), (-1, -1), 6)],
    ))
    story.append(Spacer(1, 1 * mm))

    story.append(T(
        [[P("TOTAL PAID", net_l), P(money(total), net_r)]],
        [UW * 0.6, UW * 0.4],
        [("BACKGROUND", (0, 0), (-1, -1), NAVY), ("BOX", (0, 0), (-1, -1), 0, WHITE),
         ("TOPPADDING", (0, 0), (-1, -1), 9), ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
         ("LEFTPADDING", (0, 0), (-1, -1), 12), ("RIGHTPADDING", (0, 0), (-1, -1), 12),
         ("VALIGN", (0, 0), (-1, -1), "MIDDLE")],
    ))
    story.append(Spacer(1, 2 * mm))

    story.append(Paragraph(
        "Queries: support@chakorahub.com &nbsp;|&nbsp; www.chakorahub.com",
        note_s,
    ))

    # ── Size the page to the content we actually built ────────────────
    # (more reliable than estimating — Tables/Paragraphs report their
    # real wrapped height, so the page never overflows or wastes space)
    measured_h = 0.0
    for flowable in story:
        try:
            _, h = flowable.wrap(UW, 5000 * mm)
        except Exception:
            h = 0
        measured_h += h
    PAGE_H = max(150 * mm, min(A4_H, measured_h + HEADER_H + TM + FOOTER_H + BM + 8 * mm))

    def draw_frame(canvas_obj, doc_obj):
        canvas_obj.saveState()
        band_h = 28 * mm
        band_y = PAGE_H - band_h
        canvas_obj.setFillColor(NAVY)
        canvas_obj.rect(0, band_y, PAGE_W, band_h, fill=1, stroke=0)
        canvas_obj.setFillColorRGB(0.18, 0.53, 0.87, alpha=0.35)
        canvas_obj.rect(PAGE_W * 0.45, band_y, PAGE_W * 0.55, band_h, fill=1, stroke=0)

        text_x = LM
        if os.path.exists(LOGO_PATH):
            lh = 18 * mm
            lx = LM
            ly = band_y + (band_h - lh) / 2
            canvas_obj.setFillColor(NAVY)
            canvas_obj.rect(lx - 1, ly - 1, lh + 2, lh + 2, fill=1, stroke=0)
            try:
                canvas_obj.drawImage(LOGO_PATH, lx, ly, width=lh, height=lh,
                                      preserveAspectRatio=True, mask='auto')
                text_x = LM + 22 * mm
            except Exception:
                pass

        canvas_obj.setFillColor(WHITE)
        canvas_obj.setFont("Helvetica-Bold", 14)
        canvas_obj.drawString(text_x, band_y + band_h / 2 + 1 * mm, COMPANY_NAME)
        canvas_obj.setFont("Helvetica", 8)
        canvas_obj.setFillColorRGB(1, 1, 1, alpha=0.7)
        canvas_obj.drawString(text_x, band_y + band_h / 2 - 4 * mm,
                               "www.chakorahub.com  |  support@chakorahub.com")

        canvas_obj.setFillColor(WHITE)
        canvas_obj.setFont("Helvetica-Bold", 18)
        canvas_obj.drawRightString(PAGE_W - RM, band_y + band_h / 2 + 2 * mm, "INVOICE")
        canvas_obj.setFont("Helvetica", 8)
        canvas_obj.setFillColorRGB(1, 1, 1, alpha=0.7)
        canvas_obj.drawRightString(PAGE_W - RM, band_y + band_h / 2 - 5 * mm, f"#{order_id}")

        canvas_obj.setStrokeColor(BLUE)
        canvas_obj.setLineWidth(1.5)
        canvas_obj.line(0, band_y - 0.5, PAGE_W, band_y - 0.5)

        canvas_obj.setFillColor(MID_TEXT)
        canvas_obj.setFont("Helvetica-Oblique", 7)
        canvas_obj.drawCentredString(
            PAGE_W / 2, BM - 4 * mm,
            "This is a computer-generated invoice. No signature is required."
        )
        canvas_obj.setStrokeColor(BORDER_C)
        canvas_obj.setLineWidth(0.5)
        canvas_obj.line(LM, BM + 2 * mm, PAGE_W - RM, BM + 2 * mm)
        canvas_obj.restoreState()

    doc = SimpleDocTemplate(
        buffer, pagesize=(PAGE_W, PAGE_H),
        rightMargin=RM, leftMargin=LM,
        topMargin=TM + HEADER_H,
        bottomMargin=BM + FOOTER_H,
    )

    doc.build(story, onFirstPage=draw_frame, onLaterPages=draw_frame)
    buffer.seek(0)
    return buffer.getvalue()

# =====================================================
# ORDER CONFIRMATION EMAIL
# =====================================================
def _send_order_confirmation(
    user_email:       str,
    full_name:        str,
    order_id:         str,
    payment_id:       str,
    items=None,
    financials=None,
    billing=None,
    login_email:      str = "",
    login_password: Optional[str] = None,
) -> bool:
    """
    Sends order-confirmation email + PDF invoice.
    Includes username & password in both email body and PDF.
    Always fully wrapped in try/except — called from a background Thread.
    """
    try:
        if _ses is None:
            print("⚠️  SES skipped — client not initialized")
            return False

        items      = items      or []
        financials = financials or {}
        billing    = billing    or {}
        display    = (full_name or user_email or "Customer").strip()
        login_id   = login_email or user_email

        def _money(val):
            try:    return f"{float(val):,.0f}"
            except: return "0"

        # ── 1. Generate PDF ─────────────────────────────────────────
        pdf_bytes = None
        try:
            pdf_bytes = _generate_invoice_pdf(
                order_id, payment_id, billing, items, financials,
                login_email=login_id,
                default_password=login_password,
            )
            if pdf_bytes:
                print(f"✅ Invoice PDF generated ({len(pdf_bytes):,} bytes)")
            else:
                print("⚠️  PDF returned None — reportlab not installed on server")
        except Exception as pdf_err:
            print(f"⚠️  PDF generation error: {pdf_err}")
            traceback.print_exc()

        # ── 2. Item rows (HTML) ──────────────────────────────────────
        item_rows_html = ""
        for it in items:
            name = it.get("name") or it.get("course_name") or "Course"
            qty  = it.get("quantity") or it.get("qty") or 1
            amt  = it.get("amount")
            if amt is None:
                amt = float(it.get("price") or 0) * int(qty)
            item_rows_html += (
                f"<tr>"
                f"<td style='padding:8px 10px;font-size:13px;color:#2d3748;"
                f"border-bottom:1px solid #e7edf5;'>{name}</td>"
                f"<td style='padding:8px 6px;font-size:13px;color:#2d3748;"
                f"border-bottom:1px solid #e7edf5;text-align:center;'>{qty}</td>"
                f"<td style='padding:8px 10px;font-size:13px;color:#2d3748;"
                f"border-bottom:1px solid #e7edf5;text-align:right;'>&#8377;{_money(amt)}</td>"
                f"</tr>"
            )

        discount_row_html = ""
        if float(financials.get("discount") or 0) > 0:
            discount_row_html = (
                f"<tr>"
                f"<td style='padding:3px 10px;font-size:13px;color:#718096;'>Discount</td>"
                f"<td align='right' style='padding:3px 10px;font-size:13px;color:#2e7d32;'>"
                f"&#8722;&nbsp;&#8377;{_money(financials.get('discount'))}</td>"
                f"</tr>"
            )

        # ── 3. Credentials block (HTML) ──────────────────────────────
        if login_password:
            pwd_html = (
                f"<tr>"
                f"<td width='90' style='font-size:12px;color:#6d4c41;padding:3px 0;'>"
                f"&#128274;&nbsp;<b>Password</b></td>"
                f"<td style='font-size:12px;color:#3e2723;font-weight:700;"
                f"letter-spacing:.04em;padding:3px 0;'>{login_password}</td>"
                f"</tr>"
                f"<tr><td colspan='2' style='font-size:11px;color:#8d6e63;padding:4px 0 0;'>"
                f"Please change your password after first login via Profile settings.</td></tr>"
            )
        else:
            pwd_html = (
                f"<tr>"
                f"<td width='90' style='font-size:12px;color:#6d4c41;padding:3px 0;'>"
                f"&#128274;&nbsp;<b>Password</b></td>"
                f"<td style='font-size:12px;color:#5d4037;padding:3px 0;'>"
                f"Use your existing ChakoraHub password</td>"
                f"</tr>"
            )

        creds_html = f"""
          <table width="100%" cellpadding="0" cellspacing="0" border="0" role="presentation"
                 style="margin-top:14px;background:#fff8e1;border-radius:7px;
                        border-left:4px solid #f9a825;overflow:hidden;">
            <tr><td style="padding:12px 16px;">
              <p style="margin:0 0 8px;font-size:13px;font-weight:700;color:#5d4037;">
                &#128272; Your ChakoraHub Login Details
              </p>
              <table cellpadding="0" cellspacing="0" border="0">
                <tr>
                  <td width="90" style="font-size:12px;color:#6d4c41;padding:3px 0;">
                    &#128100;&nbsp;<b>Username</b>
                  </td>
                  <td style="font-size:12px;color:#3e2723;font-weight:600;padding:3px 0;">
                    {login_id}
                  </td>
                </tr>
                {pwd_html}
                <tr>
                  <td width="90" style="font-size:12px;color:#6d4c41;padding:3px 0;">
                    &#127968;&nbsp;<b>Login URL</b>
                  </td>
                  <td style="font-size:12px;padding:3px 0;">
                    <a href="{LOGIN_URL}"
                       style="color:#1565c0;text-decoration:none;font-weight:600;">
                      www.chakorahub.com
                    </a>
                  </td>
                </tr>
              </table>
            </td></tr>
          </table>"""

        # ── 4. Full HTML email ───────────────────────────────────────
        html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>Order Confirmed – ChakoraHub</title>
</head>
<body style="margin:0;padding:0;background:#f0f4f8;font-family:Arial,Helvetica,sans-serif;">
<table width="100%" cellpadding="0" cellspacing="0" border="0" role="presentation"
       style="background:#f0f4f8;padding:16px 0;">
  <tr><td align="center">
    <table width="580" cellpadding="0" cellspacing="0" border="0" role="presentation"
           style="max-width:580px;width:100%;background:#ffffff;border-radius:10px;
                  overflow:hidden;box-shadow:0 2px 14px rgba(0,0,0,0.10);">

      <!-- HEADER -->
      <tr>
        <td style="background:#1a2340;padding:14px 24px;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0" role="presentation">
            <tr>
              <td style="font-size:15px;font-weight:700;color:#ffffff;letter-spacing:.04em;">
                &#128717; ChakoraHub
              </td>
              <td align="right" style="font-size:11px;color:#aaccee;white-space:nowrap;">
                Course Store
              </td>
            </tr>
          </table>
        </td>
      </tr>

      <!-- SUCCESS BANNER -->
      <tr>
        <td style="background:#e8f5e9;padding:10px 24px;border-bottom:2px solid #a5d6a7;">
          <table width="100%" cellpadding="0" cellspacing="0" border="0" role="presentation">
            <tr>
              <td style="font-size:14px;font-weight:700;color:#2e7d32;">
                &#10003; Order Confirmed &#8212; {order_id}
              </td>
              <td align="right" style="font-size:11px;color:#4a5568;white-space:nowrap;">
                {datetime.now().strftime('%d %b %Y')}
              </td>
            </tr>
          </table>
        </td>
      </tr>

      <!-- BODY -->
      <tr>
        <td style="padding:18px 24px 8px;">
          <p style="margin:0 0 14px;font-size:14px;color:#2d3748;line-height:1.5;">
            Hi <strong>{display}</strong>, your payment was successful and your
            enrollment is confirmed. Here&#8217;s your receipt:
          </p>

          <!-- ITEMS -->
          <table width="100%" cellpadding="0" cellspacing="0" border="0" role="presentation"
                 style="border:1px solid #e2e8f0;border-radius:7px;overflow:hidden;">
            <tr style="background:#edf2f7;">
              <td style="padding:7px 10px;font-size:10px;font-weight:700;color:#4a5568;
                         text-transform:uppercase;letter-spacing:.05em;">Course</td>
              <td style="padding:7px 6px;font-size:10px;font-weight:700;color:#4a5568;
                         text-transform:uppercase;letter-spacing:.05em;text-align:center;">Qty</td>
              <td style="padding:7px 10px;font-size:10px;font-weight:700;color:#4a5568;
                         text-transform:uppercase;letter-spacing:.05em;text-align:right;">Amount</td>
            </tr>
            {item_rows_html}
          </table>

          <!-- TOTALS (no GST) -->
          <table width="100%" cellpadding="0" cellspacing="0" border="0" role="presentation"
                 style="margin-top:8px;">
            <tr>
              <td style="padding:3px 10px;font-size:13px;color:#718096;">Subtotal</td>
              <td align="right" style="padding:3px 10px;font-size:13px;color:#718096;">
                &#8377;{_money(financials.get("subtotal"))}
              </td>
            </tr>
            {discount_row_html}
            <tr>
              <td colspan="2" style="padding:2px 10px 0;">
                <hr style="border:none;border-top:2px solid #4a90d9;margin:0;">
              </td>
            </tr>
            <tr>
              <td style="padding:8px 10px 0;font-size:15px;font-weight:700;color:#1a2340;">
                Total Paid
              </td>
              <td align="right"
                  style="padding:8px 10px 0;font-size:16px;font-weight:700;color:#c8932f;">
                &#8377;{_money(financials.get("final_total"))}
              </td>
            </tr>
          </table>

          <!-- ORDER / PAYMENT IDS -->
          <table width="100%" cellpadding="0" cellspacing="0" border="0" role="presentation"
                 style="margin-top:14px;background:#f7fafc;border-radius:6px;">
            <tr>
              <td style="padding:9px 12px;font-size:11px;color:#718096;line-height:1.7;">
                <span style="color:#4a5568;font-weight:600;">Order ID:</span>&nbsp;{order_id}
                &emsp;
                <span style="color:#4a5568;font-weight:600;">Payment ID:</span>&nbsp;{payment_id}
                <br>
                <span style="color:#4a5568;font-weight:600;">Email:</span>&nbsp;{user_email}
              </td>
            </tr>
          </table>

          {creds_html}

          <p style="margin:14px 0 0;font-size:12px;color:#718096;line-height:1.5;">
            Access your courses from
            <strong style="color:#4a5568;">My Courses</strong> in your account.
            Your invoice PDF is attached to this email.
          </p>
        </td>
      </tr>

      <!-- FOOTER -->
      <tr>
        <td style="padding:12px 24px;border-top:1px solid #edf2f7;">
          <p style="margin:0;font-size:12px;color:#a0aec0;text-align:center;line-height:1.6;">
            Questions? Write to&nbsp;
            <a href="mailto:support@chakorahub.com"
               style="color:#5b9bd5;text-decoration:none;font-weight:600;">
              support@chakorahub.com
            </a>
          </p>
        </td>
      </tr>

    </table>
  </td></tr>
</table>
</body>
</html>"""

        # ── 5. Plain-text fallback ───────────────────────────────────
        pwd_text = (
            f"Password   : {login_password}\n"
            "(Please change your password after first login.)"
            if login_password
            else "Password   : Use your existing ChakoraHub password"
        )
        items_text = "\n".join(
            f"  - {(it.get('name') or it.get('course_name') or 'Course')} "
            f"x{it.get('quantity') or 1} — "
            f"Rs. {_money(it.get('amount') or float(it.get('price') or 0) * int(it.get('quantity') or 1))}"
            for it in items
        )
        text_content = (
            f"ChakoraHub — Order Confirmed\n{'='*44}\n"
            f"Order ID   : {order_id}\n"
            f"Payment ID : {payment_id}\n"
            f"Date       : {datetime.now().strftime('%d %b %Y')}\n\n"
            f"Items:\n{items_text}\n\n"
            f"Subtotal   : Rs. {_money(financials.get('subtotal'))}\n"
            + (f"Discount   : Rs. {_money(financials.get('discount'))}\n" if float(financials.get('discount') or 0) > 0 else "")
            + f"Total Paid : Rs. {_money(financials.get('final_total'))}\n\n"
            f"{'='*44}\n"
            f"YOUR LOGIN DETAILS\n"
            f"Login URL  : {LOGIN_URL}\n"
            f"Username   : {login_id}\n"
            f"{pwd_text}\n"
            f"{'='*44}\n\n"
            f"Access your courses via My Courses in your ChakoraHub account.\n\n"
            f"Regards,\nChakoraHub Team\nsupport@chakorahub.com\n"
        )

        # ── 6. Build MIME (mixed = body + PDF attachment) ────────────
        def _build_msg(to_addr, subject):
            m = MIMEMultipart("mixed")
            m["Subject"] = subject
            m["From"]    = ADMIN_EMAIL
            m["To"]      = to_addr

            bp = MIMEMultipart("alternative")
            bp.attach(MIMEText(text_content, "plain", "utf-8"))
            bp.attach(MIMEText(html_content, "html",  "utf-8"))
            m.attach(bp)

            if pdf_bytes:
                pp = MIMEApplication(pdf_bytes, _subtype="pdf")
                pp.add_header(
                    "Content-Disposition", "attachment",
                    filename=f"ChakoraHub-Invoice-{order_id}.pdf",
                )
                m.attach(pp)
            return m

        if not pdf_bytes:
            print("⚠️  Sending email WITHOUT PDF attachment")

        # ── 7. Send to customer ───────────────────────────────────────
        customer_subject = f"Order Confirmed — {order_id} | ChakoraHub"
        customer_msg = _build_msg(user_email, customer_subject)
        _ses.send_raw_email(
            Source=ADMIN_EMAIL,
            Destinations=[user_email],
            RawMessage={"Data": customer_msg.as_bytes()},
        )
        attach_note = "with PDF" if pdf_bytes else "WITHOUT PDF"
        print(f"✅ Order confirmation {attach_note} sent → {user_email}")
        _kafka_publish("shop_email.sent", {
            "event_id": str(uuid.uuid4()),
            "event_type": "shop_email.sent",
            "timestamp": datetime.utcnow().isoformat(),
            "order_id": order_id,
            "payment_id": payment_id,
            "recipient": user_email,
            "status": "SENT",
        })

        # ── 8. Separate, explicit copy to admin — always its own send,
        #      so it never silently depends on a hidden Destinations
        #      entry that the customer-addressed message happens to share.
        try:
            admin_subject = (
                f"[New Order] {order_id} — {display} ({user_email}) — "
                f"Rs. {_money(financials.get('final_total'))}"
            )
            admin_msg = _build_msg(ADMIN_EMAIL, admin_subject)
            _ses.send_raw_email(
                Source=ADMIN_EMAIL,
                Destinations=[ADMIN_EMAIL],
                RawMessage={"Data": admin_msg.as_bytes()},
            )
            print(f"✅ Admin copy ({attach_note}) sent → {ADMIN_EMAIL}")
        except Exception as admin_err:
            print(f"⚠️  Admin copy failed: {admin_err}")
            traceback.print_exc()

        return True

    except Exception as email_err:
        print(f"❌ _send_order_confirmation failed: {email_err}")
        traceback.print_exc()
        return False


# =====================================================
# CREATE BILLING ENTRY
# =====================================================
@app.post("/billing/create")
def create_billing(data: BillingRequest, username: str = Depends(verify_credentials)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(status_code=500, detail="Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            "SELECT ID, USERNAME, EMAIL FROM NRM_USERS WHERE PHONE = %s FETCH FIRST 1 ROWS ONLY",
            (data.phone,),
        )
        user = cursor.fetchone()
        if not user:
            raise HTTPException(404, f"User with phone {data.phone} not found in NRM_USERS")
        user_id    = user["ID"]
        user_name  = user["USERNAME"]
        user_email = user["EMAIL"]

        cursor.execute(
            "SELECT CUSTOMER_ID FROM NRM_BILLING WHERE PHONE = %s FETCH FIRST 1 ROWS ONLY",
            (data.phone,),
        )
        customer = cursor.fetchone()
        if customer:
            customer_id = customer["CUSTOMER_ID"]
        else:
            cursor.execute(
                "INSERT INTO NRM_BILLING (CUSTOMER_TYPE,NAME,PHONE,EMAIL,CREATED_AT) "
                "VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                (data.billing_type, user_name or "Customer", data.phone, user_email),
            )
            conn.commit()
            cursor.execute("SELECT MAX(CUSTOMER_ID) as CID FROM NRM_BILLING WHERE PHONE=%s", (data.phone,))
            customer_id = cursor.fetchone()["CID"]

        cursor.execute(
            "SELECT PAYMENT_METHOD_ID FROM PAYMENT_METHODS WHERE LOWER(CODE)=LOWER(%s) FETCH FIRST 1 ROWS ONLY",
            (data.payment_method,),
        )
        pm = cursor.fetchone()
        if not pm:
            raise HTTPException(400, f"Invalid payment method: {data.payment_method}")

        transaction_uuid = str(uuid.uuid4())
        cursor.execute(
            """INSERT INTO BILLING_TRANSACTIONS
               (TRANSACTION_UUID,CUSTOMER_ID,BILLING_TYPE,BILLING_CATEGORY,PAYMENT_METHOD,
                UPI_TXN_ID,AMOUNT,CURRENCY,PHONE,RECEIPT_FILE_PATH,STATUS,CREATED_AT)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'PENDING',CURRENT_TIMESTAMP)""",
            (transaction_uuid, customer_id, data.billing_type, data.billing_category,
             data.payment_method, data.upi_txn_id, data.amount, data.currency,
             data.phone, data.receipt_file_path),
        )
        conn.commit()
        cursor.execute(
            "SELECT TRANSACTION_ID FROM BILLING_TRANSACTIONS WHERE TRANSACTION_UUID=%s",
            (transaction_uuid,),
        )
        transaction_id = cursor.fetchone()["TRANSACTION_ID"]

        cursor.execute(
            "SELECT ID FROM NRM_PAYMENT_STATUSES WHERE UPPER(STATUS)='PENDING' FETCH FIRST 1 ROWS ONLY"
        )
        status_row = cursor.fetchone()
        if not status_row:
            raise HTTPException(500, "Payment status 'PENDING' not found")
        status_id = status_row["ID"]

        cursor.execute(
            """INSERT INTO NRM_BILLING_ENTRIES
               (ID,USER_ID,COURSE_ID,UPI_ID,AMOUNT,DISCOUNT,STATUS_ID,BILLING_TIMESTAMP)
               SELECT NVL(MAX(ID),0)+1,%s,NULL,%s,%s,0,%s,CURRENT_TIMESTAMP
               FROM NRM_BILLING_ENTRIES""",
            (user_id, data.upi_txn_id, data.amount, status_id),
        )
        conn.commit()

        if data.receipt_file_path:
            cursor.execute(
                "INSERT INTO RECEIPTS (TRANSACTION_ID,FILE_NAME,FILE_PATH,UPLOADED_AT) "
                "VALUES (%s,%s,%s,CURRENT_TIMESTAMP)",
                (transaction_id, data.receipt_file_path.split("/")[-1], data.receipt_file_path),
            )
            conn.commit()

        return {
            "status": "success", "message": "Billing entry created successfully",
            "customer_id": customer_id, "transaction_id": transaction_id,
            "transaction_uuid": transaction_uuid, "amount": data.amount,
            "payment_method": data.payment_method, "billing_status": "PENDING",
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        raise HTTPException(500, str(e))
    finally:
        cursor.close()


# =====================================================
# ADMIN — LIST BILLING
# =====================================================
@app.get("/billing/admin/list")
def admin_list_billing(category: Optional[str] = None, username: str = Depends(verify_credentials)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        q = (
            "SELECT bt.TRANSACTION_ID,bt.TRANSACTION_UUID,nb.CUSTOMER_TYPE,"
            "nb.NAME AS CUSTOMER_NAME,nb.PHONE,bt.BILLING_TYPE,bt.BILLING_CATEGORY,"
            "bt.PAYMENT_METHOD,bt.AMOUNT,bt.CURRENCY,bt.STATUS,bt.CREATED_AT,bt.UPI_TXN_ID "
            "FROM BILLING_TRANSACTIONS bt JOIN NRM_BILLING nb ON bt.CUSTOMER_ID=nb.CUSTOMER_ID"
        )
        if category:
            cursor.execute(q + " WHERE bt.BILLING_CATEGORY=%s ORDER BY bt.CREATED_AT DESC", (category,))
        else:
            cursor.execute(q + " ORDER BY bt.CREATED_AT DESC")
        transactions = cursor.fetchall()
        return {"status": "success", "count": len(transactions), "transactions": transactions}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        cursor.close()


# =====================================================
# ADMIN — UPDATE BILLING STATUS
# =====================================================
@app.put("/billing/{billing_id}/status")
def update_billing_status(billing_id: int, status_data: StatusUpdate,
                           username: str = Depends(verify_credentials)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            "UPDATE BILLING_TRANSACTIONS SET STATUS=%s,UPDATED_AT=CURRENT_TIMESTAMP WHERE TRANSACTION_ID=%s",
            (status_data.status, billing_id),
        )
        if cursor.rowcount == 0:
            raise HTTPException(404, "Transaction not found")
        cursor.execute(
            "SELECT ID FROM NRM_PAYMENT_STATUSES WHERE UPPER(STATUS)=UPPER(%s) FETCH FIRST 1 ROWS ONLY",
            (status_data.status,),
        )
        status_row = cursor.fetchone()
        if status_row:
            cursor.execute(
                """UPDATE NRM_BILLING_ENTRIES SET STATUS_ID=%s
                   WHERE USER_ID IN (
                       SELECT u.ID FROM NRM_USERS u
                       JOIN NRM_BILLING nb ON u.PHONE=nb.PHONE
                       JOIN BILLING_TRANSACTIONS bt ON nb.CUSTOMER_ID=bt.CUSTOMER_ID
                       WHERE bt.TRANSACTION_ID=%s
                   )""",
                (status_row["ID"], billing_id),
            )
        conn.commit()
        return {"status": "success", "message": f"Transaction {billing_id} → {status_data.status}"}
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        raise HTTPException(500, str(e))
    finally:
        cursor.close()


# =====================================================
# USER — LIST OWN BILLING
# =====================================================
@app.get("/billing/list")
def list_user_billing(username: str = Depends(verify_credentials)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """SELECT bt.TRANSACTION_ID,bt.TRANSACTION_UUID,bt.BILLING_TYPE,
                      bt.BILLING_CATEGORY,bt.PAYMENT_METHOD,bt.AMOUNT,bt.CURRENCY,
                      bt.STATUS,bt.CREATED_AT,bt.UPI_TXN_ID
               FROM BILLING_TRANSACTIONS bt
               JOIN NRM_BILLING nb ON bt.CUSTOMER_ID=nb.CUSTOMER_ID
               WHERE nb.PHONE IN (SELECT PHONE FROM NRM_USERS WHERE EMAIL=%s OR USERNAME=%s)
               ORDER BY bt.CREATED_AT DESC""",
            (username, username),
        )
        transactions = cursor.fetchall()
        return {"status": "success", "count": len(transactions), "transactions": transactions}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        cursor.close()


# =====================================================
# MOBILE — LIST BILLING BY PHONE
# =====================================================
@app.get("/billing/user")
def list_billing_by_phone(phone: str, username: str = Depends(verify_credentials)):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """SELECT bt.TRANSACTION_ID,bt.TRANSACTION_UUID,bt.BILLING_TYPE,
                      bt.BILLING_CATEGORY,bt.PAYMENT_METHOD,bt.AMOUNT,bt.CURRENCY,
                      bt.STATUS,bt.CREATED_AT,bt.UPI_TXN_ID
               FROM BILLING_TRANSACTIONS bt
               JOIN NRM_BILLING nb ON bt.CUSTOMER_ID=nb.CUSTOMER_ID
               WHERE nb.PHONE=%s ORDER BY bt.CREATED_AT DESC""",
            (phone,),
        )
        transactions = cursor.fetchall()
        return {"status": "success", "count": len(transactions), "transactions": transactions}
    except Exception as e:
        raise HTTPException(500, str(e))
    finally:
        cursor.close()

# =====================================================
# HEALTH CHECK
# =====================================================
@app.get("/health")
def health_check():
    conn      = get_db_connection()
    db_status = "connected" if conn else "disconnected"
    if conn:
        conn.close()
    return {
        "status":    "healthy",
        "database":  db_status,
        "cache":     "disabled",
        "timestamp": datetime.now().isoformat(),
    }


# =====================================================
# TEST ENDPOINT
# =====================================================
@app.get("/billing-test")
async def test_billing():
    return {
        "status":    "ok",
        "message":   "Billing service is running",
        "endpoints": {
            "create":         "/billing-create (POST)",
            "history":        "/billing-history (GET)",
            "invoice":        "/invoice/{id} (GET)",
            "payment_status": "/payment-status/{id} (GET)",
            "courses":        "/courses (GET)",
            "approve":        "/billing-approve (PUT)",
            "my_billings":    "/billing-mybillings (GET)",
            "clear_cache":    "/clear-cache (POST — admin)",
            "health":         "/health (GET)",
            "test":           "/billing-test (GET)",
        },
    }


# =====================================================
# SHOP — /courses  (fallback catalogue when student_service is unavailable)
# =====================================================

# ── Display-only price override ───────────────────────────────────────────
# Does NOT touch NRM_COURSES.COURSE_FEE in Oracle. Any course priced > 0
# in the DB is shown as DISPLAY_PRICE here, matching student_service's shop
# catalogue. Courses priced at 0 stay 0 so the frontend keeps hiding them.
DISPLAY_PRICE = 6000

def _apply_display_price(price: float) -> float:
    return DISPLAY_PRICE if price and price > 0 else 0.0


# =====================================================
# SHOP — /cart/action  (analytics passthrough from app.py)
# =====================================================
class CartActionPayload(BaseModel):
    session_id:  str
    user_id:     Optional[str] = "guest"
    action_type: str
    course_id:   Optional[int]   = None
    qty:         Optional[int]   = None
    price:       Optional[float] = None
    metadata:    Optional[dict]  = {}


# @app.post("/cart/action")
# def cart_action(data: CartActionPayload):
#     """
#     Receives analytics events forwarded by app.py /api/cart/action and publishes
#     them to the shop.cart Kafka topic.  DynamoDB logging is handled downstream by
#     the FastAPI pricing service consumer; this endpoint just enqueues the event.

#     Returns 200 regardless of Kafka availability — analytics are non-fatal.
#     """
#     try:
#         _kafka_publish("shop.cart", {
#             "event_id":    str(uuid.uuid4()),
#             "event_type":  data.action_type,
#             "timestamp":   datetime.now().isoformat() + "Z",
#             "session_id":  data.session_id,
#             "user_id":     data.user_id,
#             "course_id":   data.course_id,
#             "qty":         data.qty,
#             "price":       data.price,
#             "metadata":    data.metadata or {},
#         })
#         return {"status": "ok", "action": data.action_type}
#     except Exception as e:
#         # Fire-and-forget — never let analytics break the shop flow
#         print(f"⚠️  cart_action Kafka publish failed (non-fatal): {e}")
#         return {"status": "ok", "note": "event queued locally"}


# SHOP — /checkout
# =====================================================
@app.post("/checkout")
def checkout(data: CheckoutPayload):
    """
    Upserts user (student_ht), writes ORDERS / ORDER_ITEMS / PAYMENTS,
    publishes Kafka events. No GST — final_total = subtotal - discount.
    """
    server_total = _validate_total(
        data.financials.subtotal,
        data.financials.discount,
        data.financials.final_total,
    )

    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor   = conn.cursor(DictCursor)
    order_id = _generate_order_id()
    pay_id   = _generate_payment_id()
    is_new   = False
    user_id  = None

    try:
        # ── 1. Find or create user (student_ht) ──────────────────────
        cursor.execute(
            "SELECT ID FROM NRM_USERS WHERE UPPER(EMAIL)=UPPER(%s) OR PHONE=%s ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY",
            (data.billing.email, data.billing.mobile),
        )
        existing = cursor.fetchone()
        if existing:
            user_id = existing["ID"]
        else:
            uname = data.billing.email.split("@")[0]
            cursor.execute(
                """INSERT INTO NRM_USERS
                   (USERNAME,EMAIL,PHONE,PROFILE_PIC,USERTYPE,REGISTRATION_SOURCE,
                    CREATED_AT,UPDATED_AT)
                   VALUES (%s,%s,%s,'default.jpg','student_ht','shop_checkout',
                           CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                (uname, data.billing.email, data.billing.mobile),
            )
            conn.commit()
            cursor.execute(
                "SELECT ID FROM NRM_USERS WHERE UPPER(EMAIL)=UPPER(%s) ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY",
                (data.billing.email,),
            )
            user_id = cursor.fetchone()["ID"]
            is_new  = True
            print(f"✅ New student_ht: user_id={user_id}")

        # ── 2. INSERT ORDERS (GST cols set to 0) ──────────────────────
        cursor.execute(
            """INSERT INTO ORDERS
               (ORDER_ID,USER_ID,BILLING_FULL_NAME,BILLING_EMAIL,BILLING_MOBILE,
                BILLING_STATE,BILLING_ADDRESS,SUBTOTAL,DISCOUNT,GST_RATE,
                GST_AMOUNT,FINAL_TOTAL,PAYMENT_METHOD,ORDER_STATUS,CREATED_AT,UPDATED_AT)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,0.0,0.0,%s,%s,'PENDING',
                       CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
            (order_id, user_id, data.billing.full_name, data.billing.email,
             data.billing.mobile, data.billing.state, data.billing.address,
             data.financials.subtotal, data.financials.discount,
             server_total, data.payment_method.upper()),
        )

        # ── 3. INSERT ORDER_ITEMS ─────────────────────────────────────
        for item in data.items:
            cursor.execute(
                "INSERT INTO ORDER_ITEMS (ORDER_ID,COURSE_ID,QUANTITY,UNIT_PRICE,CREATED_AT) "
                "VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                (order_id, item.course_id, item.quantity, item.price),
            )

        # ── 4. INSERT PAYMENTS ────────────────────────────────────────
        cursor.execute(
            """INSERT INTO PAYMENTS
               (PAYMENT_ID,ORDER_ID,PAYMENT_METHOD,PAYMENT_STATUS,AMOUNT,CREATED_AT,UPDATED_AT)
               VALUES (%s,%s,%s,'PENDING',%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
            (pay_id, order_id, data.payment_method.upper(), server_total),
        )
        conn.commit()
        print(f"✅ Order {order_id} | payment {pay_id}")

        # ── 5. Kafka ──────────────────────────────────────────────────
        now = datetime.now().isoformat() + "Z"
        _kafka_publish("order.placed", {
            "event_id":    str(uuid.uuid4()), "event_type": "order.placed",
            "timestamp":   now, "order_id": order_id, "payment_id": pay_id,
            "user_id":     user_id, "is_new_user": is_new,
            "billing": {
                "full_name": data.billing.full_name, "email": data.billing.email,
                "mobile":    data.billing.mobile,    "state": data.billing.state,
                "address":   data.billing.address,
            },
            "financials": {
                "subtotal":    data.financials.subtotal,
                "discount":    data.financials.discount,
                "final_total": server_total,
            },
            "items": [{"course_id": i.course_id, "quantity": i.quantity, "price": i.price} for i in data.items],
            "payment_method": data.payment_method.upper(),
        })
        _kafka_publish("shop_payment.created", {
            "event_id":       str(uuid.uuid4()), "event_type": "shop_payment.created",
            "timestamp":      now, "order_id": order_id, "payment_id": pay_id,
            "user_id":        user_id, "payment_method": data.payment_method.upper(),
            "amount":         server_total, "currency": "INR",
        })

        return {
            "status": "success", "order_id": order_id, "payment_id": pay_id,
            "user_id": user_id, "is_new_user": is_new,
            "final_total": server_total, "currency": "INR",
            "message": "Order placed. Complete payment to confirm.",
        }

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        raise HTTPException(500, str(e))
    finally:
        cursor.close()


# =====================================================
# KAFKA CONSUMER — order.placed (background thread)
# =====================================================
def _run_order_placed_consumer():
    import time
    print("🚀 billing_service order consumer thread starting…")
    consumer = None
    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                "order.placed",
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                group_id="billing-shop-order-consumer",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                session_timeout_ms=30000,
            )
            print("✅ billing_service order consumer connected")
            break
        except Exception as e:
            print(f"⚠️  Order consumer connect attempt {attempt}/5: {e}")
            time.sleep(5)
    else:
        print("❌ order consumer could not connect. Thread exiting.")
        return

    while True:
        try:
            records = consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    payload = msg.value or {}
                    order_id = str(payload.get("order_id") or "").strip()
                    payment_id = str(payload.get("payment_id") or "").strip()
                    billing = payload.get("billing") or {}
                    financials = payload.get("financials") or {}
                    items = payload.get("items") or []
                    payment_method = str(payload.get("payment_method") or "RAZORPAY").upper()
                    user_id = payload.get("user_id")

                    if not order_id or not payment_id or not billing:
                        print(f"⚠️  Invalid order.placed payload: {payload}")
                        continue

                    conn = get_db_connection()
                    if not conn:
                        continue

                    cursor = conn.cursor(DictCursor)
                    try:
                        cursor.execute("SELECT ORDER_ID FROM ORDERS WHERE ORDER_ID=%s FETCH FIRST 1 ROWS ONLY", (order_id,))
                        if cursor.fetchone():
                            consumer.commit()
                            continue

                        if not user_id:
                            cursor.execute(
                                "SELECT ID FROM NRM_USERS WHERE UPPER(EMAIL)=UPPER(%s) OR PHONE=%s ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY",
                                (billing.get("email"), billing.get("mobile")),
                            )
                            user_row = cursor.fetchone()
                            if user_row:
                                user_id = user_row["ID"]
                            else:
                                uname = str(billing.get("email") or "shop_user").split("@")[0]
                                cursor.execute(
                                    """INSERT INTO NRM_USERS
                                       (USERNAME,EMAIL,PHONE,PROFILE_PIC,USERTYPE,REGISTRATION_SOURCE,
                                        CREATED_AT,UPDATED_AT)
                                       VALUES (%s,%s,%s,'default.jpg','student_ht','shop_checkout',
                                               CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                                    (uname, billing.get("email"), billing.get("mobile")),
                                )
                                conn.commit()
                                cursor.execute(
                                    "SELECT ID FROM NRM_USERS WHERE UPPER(EMAIL)=UPPER(%s) ORDER BY ID DESC FETCH FIRST 1 ROWS ONLY",
                                    (billing.get("email"),),
                                )
                                created_user_row = cursor.fetchone()
                                if not created_user_row:
                                    raise Exception(f"Unable to resolve user_id for email={billing.get('email')}")
                                user_id = created_user_row["ID"]

                        cursor.execute(
                            """INSERT INTO ORDERS
                               (ORDER_ID,USER_ID,BILLING_FULL_NAME,BILLING_EMAIL,BILLING_MOBILE,
                                BILLING_STATE,BILLING_ADDRESS,SUBTOTAL,DISCOUNT,GST_RATE,
                                GST_AMOUNT,FINAL_TOTAL,PAYMENT_METHOD,ORDER_STATUS,CREATED_AT,UPDATED_AT)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,0.0,0.0,%s,%s,'PENDING',
                                       CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                            (
                                order_id, user_id, billing.get("full_name"), billing.get("email"),
                                billing.get("mobile"), billing.get("state"), billing.get("address"),
                                financials.get("subtotal", 0), financials.get("discount", 0),
                                financials.get("final_total", 0), payment_method,
                            ),
                        )

                        for item in items:
                            cursor.execute(
                                "INSERT INTO ORDER_ITEMS (ORDER_ID,COURSE_ID,QUANTITY,UNIT_PRICE,CREATED_AT) "
                                "VALUES (%s,%s,%s,%s,CURRENT_TIMESTAMP)",
                                (order_id, item.get("course_id"), item.get("quantity", 1), item.get("price", 0)),
                            )

                        cursor.execute(
                            """INSERT INTO PAYMENTS
                               (PAYMENT_ID,ORDER_ID,PAYMENT_METHOD,PAYMENT_STATUS,AMOUNT,CREATED_AT,UPDATED_AT)
                               VALUES (%s,%s,%s,'PENDING',%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)""",
                            (payment_id, order_id, payment_method, financials.get("final_total", 0)),
                        )
                        conn.commit()
                        consumer.commit()
                        print(f"✅ Kafka order stored | order_id={order_id} payment_id={payment_id}")
                    except Exception as e:
                        conn.rollback()
                        print(f"❌ order.placed consumer DB error: {e}")
                    finally:
                        cursor.close()
                        conn.close()
        except Exception as e:
            print(f"⚠️  order.placed consumer poll error: {e}")
            time.sleep(3)


# =====================================================
# SHOP — /payment/webhook
# Updates DB → Kafka → NRM_LOGINS → email + PDF
# =====================================================
@app.post("/payment/webhook")
def payment_webhook(data: PaymentWebhookPayload):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")

    cursor = conn.cursor(DictCursor)
    try:
        # ── Fetch order ───────────────────────────────────────────────
        cursor.execute(
            """SELECT o.ORDER_ID, o.USER_ID, o.FINAL_TOTAL,
                      o.BILLING_FULL_NAME, o.BILLING_EMAIL, o.BILLING_MOBILE,
                      o.BILLING_STATE,    o.BILLING_ADDRESS,
                      o.SUBTOTAL, o.DISCOUNT
               FROM ORDERS o WHERE o.ORDER_ID = %s""",
            (data.order_id,),
        )
        order = cursor.fetchone()
        if not order:
            raise HTTPException(404, f"Order {data.order_id} not found")

        user_id          = order["USER_ID"]
        final_total      = order["FINAL_TOTAL"]
        new_order_status = "CONFIRMED" if data.payment_status.upper() == "SUCCESS" else "FAILED"

        # ── DB updates ────────────────────────────────────────────────
        cursor.execute(
            """UPDATE PAYMENTS
               SET PAYMENT_STATUS=%s, UPI_TXN_ID=%s, GATEWAY_REF=%s,
                   FAILURE_REASON=%s, UPDATED_AT=CURRENT_TIMESTAMP
               WHERE PAYMENT_ID=%s""",
            (data.payment_status.upper(), data.upi_txn_id,
             data.gateway_ref, data.failure_reason, data.payment_id),
        )
        cursor.execute(
            "UPDATE ORDERS SET ORDER_STATUS=%s, UPDATED_AT=CURRENT_TIMESTAMP WHERE ORDER_ID=%s",
            (new_order_status, data.order_id),
        )
        conn.commit()
        print(f"✅ Order {data.order_id} → {new_order_status}")

        # ── Kafka ─────────────────────────────────────────────────────
        now = datetime.now().isoformat() + "Z"
        _kafka_publish("shop_payment.completed", {
            "event_id":       str(uuid.uuid4()), "event_type": "shop_payment.completed",
            "timestamp":      now, "order_id": data.order_id, "payment_id": data.payment_id,
            "user_id":        user_id, "payment_status": data.payment_status.upper(),
            "upi_txn_id":     data.upi_txn_id, "gateway_ref": data.gateway_ref,
            "amount":         float(final_total), "currency": "INR",
            "failure_reason": data.failure_reason,
        })

        if data.payment_status.upper() == "SUCCESS":
            _kafka_publish("order.confirmed", {
                "event_id":    str(uuid.uuid4()), "event_type": "order.confirmed",
                "timestamp":   now, "order_id": data.order_id, "payment_id": data.payment_id,
                "user_id":     user_id, "amount_paid": float(final_total), "currency": "INR",
            })

            # ── NRM_LOGINS: create login for new shop users ───────────
            login_password = None
            try:
                if not user_id:
                    raise Exception(f"Missing user_id for order={data.order_id}")
                cursor.execute(
                    "SELECT USER_ID FROM NRM_LOGINS WHERE USER_ID=%s FETCH FIRST 1 ROWS ONLY",
                    (user_id,),
                )
                has_login = cursor.fetchone()
                if not has_login:
                    login_password = DEFAULT_SHOP_PASSWORD
                    hashed = generate_password_hash(
                        DEFAULT_SHOP_PASSWORD,
                        method="pbkdf2:sha256"
                    )

                    cursor.execute(
                        """INSERT INTO NRM_LOGINS (USER_ID, PASSWORD, IS_ACTIVE)
                           VALUES (%s,%s,'N')""",
                        (user_id, hashed),
                    )
                    conn.commit()
                    print(f"✅ NRM_LOGINS created for user_id={user_id} | pwd=changeme123")
                else:
                    print(f"ℹ️  NRM_LOGINS already exists for user_id={user_id} — keeping existing password")
            except Exception as login_err:
                print(f"⚠️  NRM_LOGINS check/create error: {login_err}")
                # Non-fatal — email still goes out

            # ── Fetch items with course names ─────────────────────────
            _email_items = []
            try:
                cursor.execute(
                    """SELECT oi.QUANTITY, oi.UNIT_PRICE,
                              COALESCE(c.COURSE_NAME, 'Course') AS COURSE_NAME
                       FROM ORDER_ITEMS oi
                       LEFT JOIN NRM_COURSES c ON oi.COURSE_ID = c.ID
                       WHERE oi.ORDER_ID = %s""",
                    (data.order_id,),
                )
                for row in cursor.fetchall():
                    qty   = int(row["QUANTITY"]    or 1)
                    price = float(row["UNIT_PRICE"] or 0)
                    _email_items.append({
                        "name":     row["COURSE_NAME"],
                        "quantity": qty,
                        "price":    price,
                        "amount":   price * qty,
                    })
            except Exception as item_err:
                print(f"⚠️  Could not fetch items for email: {item_err}")

            _email_financials = {
                "subtotal":    float(order["SUBTOTAL"]  or 0),
                "discount":    float(order["DISCOUNT"]  or 0),
                "final_total": float(final_total        or 0),
            }
            _email_billing = {
                "full_name": order["BILLING_FULL_NAME"],
                "email":     order["BILLING_EMAIL"],
                "mobile":    order["BILLING_MOBILE"],
                "state":     order["BILLING_STATE"],
                "address":   order["BILLING_ADDRESS"],
            }

            # ── Spawn email thread (non-blocking) ─────────────────────
            Thread(
                target=_send_order_confirmation,
                kwargs=dict(
                    user_email=order["BILLING_EMAIL"] or "",
                    full_name=order["BILLING_FULL_NAME"] or "",
                    order_id=data.order_id,
                    payment_id=data.payment_id,
                    items=_email_items,
                    financials=_email_financials,
                    billing=_email_billing,
                    login_email=order["BILLING_EMAIL"] or "",
                    login_password=login_password,
                ),
                daemon=True,
            ).start()

        return {"status": "success", "order_id": data.order_id, "order_status": new_order_status}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        traceback.print_exc()
        raise HTTPException(500, str(e))
    finally:
        cursor.close()


# =====================================================
# SHOP — /order/{order_id}
# =====================================================
@app.get("/order/{order_id}")
def get_order_status(order_id: str):
    conn = get_db_connection()
    if not conn:
        raise HTTPException(500, "Database connection failed")
    cursor = conn.cursor(DictCursor)
    try:
        cursor.execute(
            """SELECT o.ORDER_ID, o.ORDER_STATUS, o.FINAL_TOTAL, o.PAYMENT_METHOD,
                      o.CREATED_AT, p.PAYMENT_ID, p.PAYMENT_STATUS,
                      p.GATEWAY_REF, p.FAILURE_REASON
               FROM ORDERS o
               LEFT JOIN PAYMENTS p ON o.ORDER_ID = p.ORDER_ID
               WHERE o.ORDER_ID = %s FETCH FIRST 1 ROWS ONLY""",
            (order_id,),
        )
        row = cursor.fetchone()
        if not row:
            raise HTTPException(404, "Order not found")
        return {"status": "success", "order": dict(row)}
    finally:
        cursor.close()


# =====================================================
# KAFKA CONSUMER — payment.completed (background thread)
# =====================================================
def _run_payment_consumer():
    import time
    print("🚀 billing_service payment consumer thread starting…")
    consumer = None
    for attempt in range(1, 6):
        try:
            consumer = KafkaConsumer(
                "shop_payment.completed",
                "payment.completed",
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                group_id="billing-shop-payment-consumer",
                auto_offset_reset="earliest",
                enable_auto_commit=False,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                session_timeout_ms=30000,
            )
            print("✅ billing_service payment consumer connected")
            break
        except Exception as e:
            print(f"⚠️  Consumer connect attempt {attempt}/5: {e}")
            time.sleep(5)
    else:
        print("❌ payment consumer could not connect. Thread exiting.")
        return

    while True:
        try:
            records = consumer.poll(timeout_ms=1000)
            for tp, messages in records.items():
                for msg in messages:
                    payload  = msg.value
                    order_id = payload.get("order_id")
                    pay_id   = payload.get("payment_id")
                    status   = (payload.get("payment_status") or "").upper()
                    print(f"📩 {msg.topic} | order={order_id} status={status}")
                    conn = get_db_connection()
                    if not conn:
                        continue
                    cur = conn.cursor(DictCursor)
                    try:
                        cur.execute(
                            """SELECT o.USER_ID, o.BILLING_FULL_NAME, o.BILLING_EMAIL, o.BILLING_MOBILE,
                                      o.BILLING_STATE, o.BILLING_ADDRESS, o.SUBTOTAL, o.DISCOUNT, o.FINAL_TOTAL
                               FROM ORDERS o WHERE o.ORDER_ID=%s FETCH FIRST 1 ROWS ONLY""",
                            (order_id,),
                        )
                        order = cur.fetchone()
                        if not order:
                            continue

                        cur.execute(
                            """UPDATE PAYMENTS
                               SET PAYMENT_STATUS=%s, UPI_TXN_ID=%s, GATEWAY_REF=%s,
                                   FAILURE_REASON=%s, UPDATED_AT=CURRENT_TIMESTAMP
                               WHERE PAYMENT_ID=%s""",
                            (status, payload.get("upi_txn_id"), payload.get("gateway_ref"),
                             payload.get("failure_reason"), pay_id),
                        )
                        new_os = "CONFIRMED" if status == "SUCCESS" else "FAILED"
                        cur.execute(
                            "UPDATE ORDERS SET ORDER_STATUS=%s,UPDATED_AT=CURRENT_TIMESTAMP WHERE ORDER_ID=%s",
                            (new_os, order_id),
                        )

                        if msg.topic == "shop_payment.completed" and status == "SUCCESS":
                            user_id = order.get("USER_ID")
                            login_password = None
                            # if user_id:
                            #     cur.execute(
                            #         "UPDATE NRM_USERS SET USERTYPE='student_hs', UPDATED_AT=CURRENT_TIMESTAMP WHERE ID=%s",
                            #         (user_id,),
                            #     )
                            if not user_id:
                                raise Exception(f"Missing user_id for order={order_id}")

                            cur.execute(
                                "SELECT USER_ID FROM NRM_LOGINS WHERE USER_ID=%s FETCH FIRST 1 ROWS ONLY",
                                (user_id,),
                            )
                            has_login = cur.fetchone()
                            if not has_login:
                                login_password = DEFAULT_SHOP_PASSWORD
                                hashed = generate_password_hash(
                                    DEFAULT_SHOP_PASSWORD,
                                    method="pbkdf2:sha256",
                                )
                                cur.execute(
                                    """INSERT INTO NRM_LOGINS (USER_ID, PASSWORD, IS_ACTIVE)
                                       VALUES (%s,%s,'N')""",
                                    (user_id, hashed),
                                )
                                print(f"✅ NRM_LOGINS created for user_id={user_id} | pwd=changeme123")
                            else:
                                print(f"ℹ️  NRM_LOGINS already exists for user_id={user_id} — keeping existing password")

                            cur.execute(
                                """SELECT oi.COURSE_ID, oi.QUANTITY, oi.UNIT_PRICE,
                                          COALESCE(c.COURSE_NAME, 'Course') AS COURSE_NAME
                                   FROM ORDER_ITEMS oi
                                   LEFT JOIN NRM_COURSES c ON c.ID = oi.COURSE_ID
                                   WHERE oi.ORDER_ID=%s""",
                                (order_id,),
                            )
                            order_items = cur.fetchall() or []

                            for item in order_items:
                                try:
                                    _pricing_history_table.put_item(Item={
                                        "event_id": str(uuid.uuid4()),
                                        "event_type": "purchase.completed",
                                        "event_timestamp": datetime.utcnow().isoformat() + "Z",
                                        "session_id": str(payload.get("session_id") or order_id or pay_id),
                                        "order_id": str(order_id),
                                        "payment_id": str(pay_id),
                                        "user_id": int(user_id) if user_id is not None else None,
                                        "course_id": int(item.get("COURSE_ID")) if item.get("COURSE_ID") is not None else None,
                                        "price": Decimal(str(item.get("UNIT_PRICE") or 0)),
                                        "quantity": int(item.get("QUANTITY") or 1),
                                        "metadata": {
                                            "course_name": item.get("COURSE_NAME"),
                                            "billing_email": order.get("BILLING_EMAIL"),
                                        },
                                    })
                                except Exception as ddb_err:
                                    print(f"⚠️  pricing history write failed for order={order_id}: {ddb_err}")

                            _kafka_publish("order.confirmed", {
                                "event_id": str(uuid.uuid4()),
                                "event_type": "order.confirmed",
                                "timestamp": datetime.utcnow().isoformat() + "Z",
                                "order_id": order_id,
                                "payment_id": pay_id,
                                "user_id": user_id,
                                "amount_paid": float(order.get("FINAL_TOTAL") or 0),
                                "currency": "INR",
                            })

                            _email_items = [
                                {
                                    "name": item.get("COURSE_NAME") or "Course",
                                    "qty": int(item.get("QUANTITY") or 1),
                                    "amount": float(item.get("UNIT_PRICE") or 0) * int(item.get("QUANTITY") or 1),
                                }
                                for item in order_items
                            ]
                            _email_financials = {
                                "subtotal": float(order.get("SUBTOTAL") or 0),
                                "discount": float(order.get("DISCOUNT") or 0),
                                "final_total": float(order.get("FINAL_TOTAL") or 0),
                            }
                            _email_billing = {
                                "full_name": order.get("BILLING_FULL_NAME") or "",
                                "email": order.get("BILLING_EMAIL") or "",
                                "mobile": order.get("BILLING_MOBILE") or "",
                                "state": order.get("BILLING_STATE") or "",
                                "address": order.get("BILLING_ADDRESS") or "",
                            }

                            Thread(
                                target=_send_order_confirmation,
                                kwargs=dict(
                                    user_email=order.get("BILLING_EMAIL") or "",
                                    full_name=order.get("BILLING_FULL_NAME") or "",
                                    order_id=order_id,
                                    payment_id=pay_id,
                                    items=_email_items,
                                    financials=_email_financials,
                                    billing=_email_billing,
                                    login_email=order.get("BILLING_EMAIL") or "",
                                    login_password=login_password,
                                ),
                                daemon=True,
                            ).start()

                        conn.commit()
                        consumer.commit()
                        print(f"✅ Consumer: order {order_id} → {new_os}")
                    except Exception as e:
                        conn.rollback()
                        print(f"❌ Consumer DB error: {e}")
                    finally:
                        cur.close()
                        conn.close()
        except Exception as e:
            print(f"⚠️  Consumer poll error: {e}")
            time.sleep(3)


def _warmup_oracle():
    import time
    time.sleep(3)
    conn = get_db_connection()
    print("✅ Oracle warm-up ready" if conn else "⚠️  Oracle warm-up failed")


Thread(target=_warmup_oracle, daemon=True).start()
Thread(target=_run_order_placed_consumer, daemon=True).start()
Thread(target=_run_payment_consumer, daemon=True).start()
Thread(target=_run_payment_created_consumer, daemon=True).start()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("BILLING_SERVICE_PORT", "8010")))
