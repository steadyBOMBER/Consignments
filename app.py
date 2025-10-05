import os
import threading
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

import sentry_sdk
import requests
from flask import (
    Flask, render_template, request, redirect, url_for,
    jsonify, send_from_directory, session, Blueprint
)
from flask_jwt_extended import JWTManager, create_access_token, jwt_required, get_jwt_identity
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit
from flask_wtf.csrf import CSRFProtect
from flask_caching import Cache
from flask_restx import Api, Resource, fields
from celery import Celery
from pydantic import BaseModel, Field, ValidationError
from werkzeug.security import generate_password_hash, check_password_hash
from retry import retry
from sqlalchemy import func

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# --- Sentry Integration ---
sentry_sdk.init(
    dsn=os.environ.get("SENTRY_DSN"),
    integrations=[sentry_sdk.integrations.flask.FlaskIntegration()],
    traces_sample_rate=1.0
)

# --- Configuration and Initialization ---
def validate_config():
    """Validate required environment variables."""
    required_envs = ["SECRET_KEY", "JWT_SECRET_KEY", "ADMIN_PASSWORD", "SENTRY_DSN", "TELEGRAM_TOKEN"]
    for env in required_envs:
        if not os.environ.get(env):
            logger.error(f"Missing required environment variable: {env}")
            raise ValueError(f"Missing {env}")
    logger.info("Environment variables validated successfully")

def get_database_url():
    """Patch for Render and other platforms with 'postgres://' scheme."""
    url = os.environ.get("DATABASE_URL", "sqlite:///consignment.db")
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return url

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config.from_mapping(
    SECRET_KEY=os.environ.get("SECRET_KEY"),
    SQLALCHEMY_DATABASE_URI=get_database_url(),
    SQLALCHEMY_TRACK_MODIFICATIONS=False,
    SQLALCHEMY_ENGINE_OPTIONS={
        "pool_size": 10,
        "max_overflow": 20,
        "pool_timeout": 30
    },
    JWT_SECRET_KEY=os.environ.get("JWT_SECRET_KEY"),
    CELERY_BROKER_URL=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    CACHE_TYPE="redis",
    CACHE_REDIS_URL=os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
    ADMIN_PASSWORD_HASH=generate_password_hash(os.environ.get("ADMIN_PASSWORD")),
    SMTP_HOST=os.environ.get("SMTP_HOST", ""),
    SMTP_PORT=int(os.environ.get("SMTP_PORT", "587")),
    SMTP_USER=os.environ.get("SMTP_USER", ""),
    SMTP_PASS=os.environ.get("SMTP_PASS", ""),
    SMTP_FROM=os.environ.get("SMTP_FROM", "no-reply@example.com"),
    APP_BASE_URL=os.environ.get("APP_BASE_URL", "http://localhost:5000"),
    SESSION_COOKIE_SECURE=True,
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(minutes=30),
    TELEGRAM_TOKEN=os.environ.get("TELEGRAM_TOKEN")
)

# Validate configuration
validate_config()

db = SQLAlchemy(app)
jwt = JWTManager(app)
csrf = CSRFProtect(app)
cache = Cache(app)
socketio = SocketIO(app, cors_allowed_origins=os.environ.get("ALLOWED_ORIGINS", "http://localhost:5000"))
celery = Celery(app.name, broker=app.config["CELERY_BROKER_URL"])
celery.conf.update(app.config)
limiter = Limiter(key_func=get_remote_address)
limiter.init_app(app)

# --- Swagger API Documentation ---
api = Api(app, version="1.0", title="Courier Tracking API", description="API for tracking courier shipments")
shipment_ns = api.namespace("shipments", description="Shipment operations")
admin_ns = api.namespace("admin", description="Admin operations")

# --- Pydantic Models for Validation ---
class CheckpointCreate(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lng: float = Field(..., ge=-180, le=180)
    label: str = Field(..., min_length=1)
    note: Optional[str] = None
    status: Optional[str] = None

class ShipmentCreate(BaseModel):
    tracking_number: str = Field(..., min_length=1)
    title: str = "Consignment"
    origin: Dict[str, float]
    destination: Dict[str, float]
    status: str = "Created"

# --- SQLAlchemy Models ---
class Shipment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracking = db.Column(db.String(50), unique=True, nullable=False, index=True)
    title = db.Column(db.String(100))
    origin_lat = db.Column(db.Float, nullable=False)
    origin_lng = db.Column(db.Float, nullable=False)
    dest_lat = db.Column(db.Float, nullable=False)
    dest_lng = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(20), default="Created")
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "tracking": self.tracking,
            "title": self.title,
            "origin_lat": self.origin_lat,
            "origin_lng": self.origin_lng,
            "dest_lat": self.dest_lat,
            "dest_lng": self.dest_lng,
            "status": self.status,
            "updated_at": self.updated_at
        }

class Checkpoint(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey("shipment.id"), nullable=False, index=True)
    position = db.Column(db.Integer, nullable=False)
    lat = db.Column(db.Float, nullable=False)
    lng = db.Column(db.Float, nullable=False)
    label = db.Column(db.String(50), nullable=False)
    note = db.Column(db.Text)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "shipment_id": self.shipment_id,
            "position": self.position,
            "lat": self.lat,
            "lng": self.lng,
            "label": self.label,
            "note": self.note,
            "timestamp": self.timestamp
        }

class Subscriber(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey("shipment.id"), nullable=False, index=True)
    email = db.Column(db.String(100), nullable=False)
    is_active = db.Column(db.Boolean, default=True)

class ShipmentStatusHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey("shipment.id"), nullable=False, index=True)
    status = db.Column(db.String(20), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "shipment_id": self.shipment_id,
            "status": self.status,
            "timestamp": self.timestamp
        }

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(50), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    role_id = db.Column(db.Integer, db.ForeignKey("role.id"), nullable=False)

class Role(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(50), unique=True, nullable=False)
    permissions = db.Column(db.JSON, default={
        "can_view_shipments": True,
        "can_edit_shipments": False,
        "can_view_analytics": False
    })

class ApiKey(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(64), unique=True, nullable=False)
    is_active = db.Column(db.Boolean, default=True)

# --- Ensure tables exist with retry ---
@retry(tries=3, delay=2, backoff=2)
def ensure_tables():
    with app.app_context():
        try:
            db.create_all()
            db.engine.execute("CREATE INDEX IF NOT EXISTS idx_shipment_tracking ON shipment (tracking)")
            db.engine.execute("CREATE INDEX IF NOT EXISTS idx_checkpoint_shipment_id ON checkpoint (shipment_id)")
            db.engine.execute("CREATE INDEX IF NOT EXISTS idx_subscriber_shipment_id ON subscriber (shipment_id)")
            db.engine.execute("CREATE INDEX IF NOT EXISTS idx_status_history_shipment_id ON shipment_status_history (shipment_id)")
            logger.info("Database tables and indexes ensured.")
        except Exception as e:
            logger.error(f"Could not create tables: {e}")
            raise

try:
    ensure_tables()
except Exception as e:
    logger.error(f"Failed to initialize database after retries: {e}")
    raise

# --- Security Headers and HTTPS ---
@app.after_request
def set_security_headers(response):
    headers = {
        "X-Frame-Options": "DENY",
        "X-Content-Type-Options": "nosniff",
        "Referrer-Policy": "no-referrer-when-downgrade",
        "Content-Security-Policy": "default-src 'self' 'unsafe-inline' data:; script-src 'self' https://unpkg.com https://cdnjs.cloudflare.com; style-src 'self' https://unpkg.com;"
    }
    for k, v in headers.items():
        response.headers.setdefault(k, v)
    return response

@app.before_request
def enforce_https():
    if not request.is_secure and not app.debug:
        url = request.url.replace("http://", "https://", 1)
        return redirect(url, code=301)

# --- Telegram Webhook Blueprint ---
telegram_bp = Blueprint("telegram_webhook", __name__, url_prefix="/telegram")

def send_message(chat_id, text):
    token = app.config["TELEGRAM_TOKEN"]
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": text})
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

@telegram_bp.route("/webhook/<token>", methods=["POST"])
def webhook(token):
    if not app.config["TELEGRAM_TOKEN"] or token != app.config["TELEGRAM_TOKEN"]:
        return jsonify({"ok": False, "error": "invalid token"}), 403
    data = request.get_json(force=True)
    threading.Thread(target=handle_update, args=(data,), daemon=True).start()
    return jsonify({"ok": True})

def handle_update(update_json):
    try:
        message = update_json.get("message") or update_json.get("edited_message") or {}
        text = message.get("text", "")
        chat = message.get("chat", {})
        chat_id = chat.get("id")
        if not text or not chat_id:
            return
        parts = text.strip().split(" ", 1)
        cmd = parts[0].lstrip("/").split("@")[0].lower()
        payload = parts[1].strip() if len(parts) > 1 else ""

        if cmd == "status":
            t = payload.split()[0] if payload else ""
            if not t:
                send_message(chat_id, "Usage: /status <TRACKING>")
                return
            shipment = Shipment.query.filter_by(tracking=t).first()
            if not shipment:
                send_message(chat_id, f"Tracking {t} not found.")
                return
            latest = Checkpoint.query.filter_by(shipment_id=shipment.id).order_by(Checkpoint.id.desc()).first()
            text = f"{shipment.title} ({shipment.tracking})\nStatus: {shipment.status}\nUpdated: {shipment.updated_at}\n"
            if latest:
                text += f"Latest: {latest.label} at {latest.timestamp} ({latest.lat:.4f},{latest.lng:.4f})\n"
            text += f"Map: {app.config['APP_BASE_URL']}/track/{shipment.tracking}"
            send_message(chat_id, text)

        elif cmd == "create":
            if "|" not in payload:
                send_message(chat_id, "Usage: /create TRACKING|Title|orig_lat,orig_lng|dest_lat,dest_lng")
                return
            p = payload.split("|")
            tracking = p[0].strip()
            title = p[1].strip() if len(p) > 1 else "Consignment"
            o = p[2].split(",") if len(p) > 2 else None
            d = p[3].split(",") if len(p) > 3 else None
            if not o or not d:
                send_message(chat_id, "Invalid coordinates")
                return
            try:
                shipment = Shipment(
                    tracking=tracking,
                    title=title,
                    origin_lat=float(o[0]),
                    origin_lng=float(o[1]),
                    dest_lat=float(d[0]),
                    dest_lng=float(d[1]),
                    status="Created"
                )
                db.session.add(shipment)
                db.session.flush()  # Get shipment ID before committing
                history = ShipmentStatusHistory(shipment_id=shipment.id, status="Created")
                db.session.add(history)
                db.session.commit()
                send_message(chat_id, f"Created {tracking}")
                socketio.emit("update", {
                    "tracking": shipment.tracking,
                    "status": shipment.status,
                    "checkpoints": []
                }, broadcast=True)
            except Exception as e:
                db.session.rollback()
                logger.error(f"Failed to create shipment via Telegram: {e}")
                send_message(chat_id, "Error creating shipment")

        elif cmd == "addcp":
            if "|" not in payload:
                send_message(chat_id, "Usage: /addcp TRACKING|lat,lng|Label|note")
                return
            p = payload.split("|")
            tracking = p[0].strip()
            coords = p[1].split(",") if len(p) > 1 else None
            label = p[2].strip() if len(p) > 2 else "Scanned"
            note = p[3].strip() if len(p) > 3 else None
            if not coords:
                send_message(chat_id, "Invalid coordinates")
                return
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                send_message(chat_id, "Shipment not found")
                return
            try:
                position = Checkpoint.query.filter_by(shipment_id=shipment.id).count()
                checkpoint = Checkpoint(
                    shipment_id=shipment.id,
                    position=position,
                    lat=float(coords[0]),
                    lng=float(coords[1]),
                    label=label,
                    note=note
                )
                shipment.updated_at = datetime.utcnow()
                db.session.add(checkpoint)
                db.session.commit()
                send_checkpoint_email_task.delay(shipment.id, checkpoint.id)
                socketio.emit("update", {
                    "tracking": shipment.tracking,
                    "status": shipment.status,
                    "checkpoints": [{
                        "lat": cp.lat,
                        "lng": cp.lng,
                        "label": cp.label,
                        "note": cp.note,
                        "timestamp": cp.timestamp
                    } for cp in Checkpoint.query.filter_by(shipment_id=shipment.id).order_by(Checkpoint.position).all()]
                }, broadcast=True)
                send_message(chat_id, f"Added checkpoint to {tracking}")
            except Exception as e:
                db.session.rollback()
                logger.error(f"Failed to add checkpoint via Telegram: {e}")
                send_message(chat_id, "Error adding checkpoint")

        elif cmd == "list":
            ships = Shipment.query.order_by(Shipment.updated_at.desc()).limit(20).all()
            if not ships:
                send_message(chat_id, "No shipments found.")
                return
            msg = "Recent shipments:\n" + "\n".join([f"{s.tracking}: {s.title} ({s.status})" for s in ships])
            send_message(chat_id, msg)

        elif cmd == "remove_sub":
            if "|" not in payload:
                send_message(chat_id, "Usage: /remove_sub TRACKING|email")
                return
            tracking, email = payload.split("|", 1)
            shipment = Shipment.query.filter_by(tracking=tracking.strip()).first()
            if not shipment:
                send_message(chat_id, "Shipment not found")
                return
            subscriber = Subscriber.query.filter_by(shipment_id=shipment.id, email=email.strip().lower()).first()
            if not subscriber:
                send_message(chat_id, "Subscriber not found")
                return
            try:
                subscriber.is_active = False
                db.session.commit()
                send_message(chat_id, f"Removed {email}")
            except Exception as e:
                db.session.rollback()
                logger.error(f"Failed to remove subscriber via Telegram: {e}")
                send_message(chat_id, "Error removing subscriber")

        elif cmd == "simulate":
            if "|" not in payload:
                send_message(chat_id, "Usage: /simulate TRACKING|steps|interval_seconds")
                return
            parts = payload.split("|")
            tracking = parts[0].strip()
            steps = int(parts[1].strip()) if len(parts) > 1 and parts[1].strip() else 6
            interval = float(parts[2].strip()) if len(parts) > 2 and parts[2].strip() else 3.0
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                send_message(chat_id, "Shipment not found")
                return
            def worker(shipment_id, steps, interval):
                try:
                    for i in range(steps):
                        shipment = Shipment.query.get(shipment_id)
                        if not shipment:
                            break
                        frac = (i + 1) / float(steps)
                        lat = shipment.origin_lat + (shipment.dest_lat - shipment.origin_lat) * frac
                        lng = shipment.origin_lng + (shipment.dest_lng - shipment.origin_lng) * frac
                        position = Checkpoint.query.filter_by(shipment_id=shipment_id).count()
                        checkpoint = Checkpoint(
                            shipment_id=shipment_id,
                            position=position,
                            lat=lat,
                            lng=lng,
                            label=f"Simulated {i + 1}/{steps}"
                        )
                        shipment.updated_at = datetime.utcnow()
                        db.session.add(checkpoint)
                        db.session.commit()
                        send_checkpoint_email_task.delay(shipment_id, checkpoint.id)
                        socketio.emit("update", {
                            "tracking": shipment.tracking,
                            "status": shipment.status,
                            "checkpoints": [{
                                "lat": cp.lat,
                                "lng": cp.lng,
                                "label": cp.label,
                                "note": cp.note,
                                "timestamp": cp.timestamp
                            } for cp in Checkpoint.query.filter_by(shipment_id=shipment_id).order_by(Checkpoint.position).all()]
                        }, broadcast=True)
                        time.sleep(interval)
                except Exception as e:
                    db.session.rollback()
                    logger.error(f"Simulation error for shipment {shipment_id}: {e}")
            threading.Thread(target=worker, args=(shipment.id, steps, interval), daemon=True).start()
            send_message(chat_id, f"Started simulation for {tracking}: {steps} steps, {interval}s interval.")
    except Exception as e:
        logger.error(f"Webhook handler error: {e}")

# Register Telegram blueprint
app.register_blueprint(telegram_bp)

# --- Routes ---
@app.route("/")
def index():
    return render_template("index.html")

@app.route("/track/<tracking>")
def track_page(tracking):
    shipment = Shipment.query.filter_by(tracking=tracking).first_or_404()
    checkpoints = Check
