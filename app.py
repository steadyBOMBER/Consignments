import os
import logging
import smtplib
import random
import json
import re
import time
from datetime import datetime, timedelta
from typing import Dict, Union, Optional as TypingOptional, List, Tuple
from packaging import version
import validators
import requests
from flask import Flask, request, jsonify, render_template, redirect, url_for, session, abort
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_socketio import SocketIO, emit, join_room
from flask_caching import Cache
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from werkzeug.security import generate_password_hash, check_password_hash
from werkzeug.utils import secure_filename
from math import radians, sin, cos, sqrt, atan2
from pydantic import BaseModel, validator, ValidationError
from logging.handlers import RotatingFileHandler
from celery import Celery, shared_task
import redis
import bleach
from retry import retry
from enum import Enum
from PIL import Image
from io import BytesIO
import boto3
from botocore.exceptions import ClientError
from geographiclib.geodesic import Geodesic
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# Initialize Flask app
app = Flask(__name__, template_folder='templates', static_folder='static')

# Configuration class with validation
class Settings(BaseModel):
    SECRET_KEY: str
    SQLALCHEMY_DATABASE_URI: str
    CELERY_BROKER_URL: str
    CELERY_RESULT_BACKEND: str
    REDIS_URL: str
    SMTP_HOST: str
    SMTP_PORT: int
    SMTP_USER: str
    SMTP_PASS: str
    SMTP_FROM: str
    APP_BASE_URL: str
    ADMIN_EMAIL: str
    ADMIN_PHONE: str
    TELEGRAM_TOKEN: str
    ADMIN_PASSWORD: str = "Biggerboy"  # Default hardcoded password
    AWS_ACCESS_KEY_ID: TypingOptional[str] = None
    AWS_SECRET_ACCESS_KEY: TypingOptional[str] = None
    S3_BUCKET: TypingOptional[str] = None
    UPLOAD_FOLDER: str = "static/uploads"
    MAX_FILE_SIZE: int = 10 * 1024 * 1024  # 10MB
    IMAGE_QUALITY: int = 85
    DEFAULT_SPEED_KMH: float = 50.0
    GEOCODING_TIMEOUT: int = 5
    RATE_LIMIT_DEFAULT: str = "200 per day, 50 per hour"
    OSRM_ENABLED: bool = True
    OSRM_URL: str = "http://router.project-osrm.org"
    MAX_WAYPOINTS: int = 50
    TRAFFIC_VARIABILITY: float = 0.5

    @validator('SMTP_PORT')
    def validate_smtp_port(cls, v):
        if not 1 <= v <= 65535:
            raise ValueError('SMTP_PORT must be between 1 and 65535')
        return v

    @validator('TELEGRAM_TOKEN')
    def validate_telegram_token(cls, v):
        if not re.match(r'^\d+:[A-Za-z0-9_-]+$', v):
            raise ValueError('Invalid Telegram token format')
        return v

    @validator('APP_BASE_URL')
    def validate_base_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('APP_BASE_URL must start with http:// or https://')
        return v.rstrip('/')

    class Config:
        env_file = '.env'
        env_file_encoding = 'utf-8'

# Dependency validator
class DependencyValidator:
    REQUIRED_PACKAGES = {
        'flask': '2.0.0',
        'flask-sqlalchemy': '2.5.0',
        'flask-limiter': '2.0.0',
        'celery': '5.0.0',
        'redis': '4.0.0',
        'pydantic': '1.8.0',
        'pillow': '8.0.0',
        'boto3': '1.18.0',
        'retry': '0.9.2',
        'geographiclib': '2.0'
    }

    @classmethod
    def validate_dependencies(cls):
        missing = []
        outdated = []
        for package, min_version in cls.REQUIRED_PACKAGES.items():
            try:
                imported = __import__(package.replace('-', '_'))
                installed_version = getattr(imported, '__version__', 'unknown')
                if version.parse(str(installed_version)) < version.parse(min_version):
                    outdated.append(f"{package} (need >= {min_version}, have {installed_version})")
            except ImportError:
                missing.append(package)
        if missing:
            raise ImportError(f"Missing packages: {', '.join(missing)}\nInstall with: pip install {' '.join(missing)}")
        if outdated:
            app.logger.warning(f"Outdated packages: {', '.join(outdated)}")

# Initialize logging
handler = RotatingFileHandler('app.log', maxBytes=1000000, backupCount=5)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)
logger = app.logger

# Validate dependencies and load settings
try:
    DependencyValidator.validate_dependencies()
    settings = Settings()
    for key, value in settings.dict().items():
        if value is not None:
            app.config[key] = value
    # Hash the admin password for secure comparison
    app.config['ADMIN_PASSWORD_HASH'] = generate_password_hash(app.config['ADMIN_PASSWORD'])
except Exception as e:
    logger.error(f"Startup validation failed: {e}")
    raise

# Initialize extensions
db = SQLAlchemy(app)
migrate = Migrate(app, db)
socketio = SocketIO(app, cors_allowed_origins="*")
limiter = Limiter(app, key_func=get_remote_address, default_limits=[settings.RATE_LIMIT_DEFAULT])
cache = Cache(app, config={'CACHE_TYPE': 'redis', 'CACHE_REDIS_URL': app.config['REDIS_URL']})
redis_client = redis.Redis.from_url(app.config['REDIS_URL'])

# Initialize Celery
def make_celery(app):
    celery = Celery(
        app.import_name,
        broker=app.config['CELERY_BROKER_URL'],
        backend=app.config['CELERY_RESULT_BACKEND']
    )
    celery.conf.update(app.config)
    return celery

celery = make_celery(app)

# Shipment status enum
class ShipmentStatus(Enum):
    CREATED = 'Created'
    IN_TRANSIT = 'In Transit'
    OUT_FOR_DELIVERY = 'Out for Delivery'
    DELIVERED = 'Delivered'

# Authentication decorator
def admin_required(f):
    def wrap(*args, **kwargs):
        if not session.get('is_admin'):
            abort(401)  # Unauthorized
        return f(*args, **kwargs)
    wrap.__name__ = f.__name__
    return wrap

# Database Models
class StatusHistory(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipment.id'), nullable=False)
    status = db.Column(db.String(50), nullable=False)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'status': self.status,
            'timestamp': self.timestamp.isoformat() + 'Z'
        }

class Shipment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracking = db.Column(db.String(50), unique=True, nullable=False, index=True)
    title = db.Column(db.String(100), nullable=False)
    origin_lat = db.Column(db.Float, nullable=False)
    origin_lng = db.Column(db.Float, nullable=False)
    dest_lat = db.Column(db.Float, nullable=False)
    dest_lng = db.Column(db.Float, nullable=False)
    origin_address = db.Column(db.String(200))
    dest_address = db.Column(db.String(200))
    distance_km = db.Column(db.Float)
    eta = db.Column(db.DateTime)
    status = db.Column(db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    checkpoints = db.relationship('Checkpoint', backref='shipment', lazy=True)
    subscribers = db.relationship('Subscriber', backref='shipment', lazy=True)
    history = db.relationship('StatusHistory', backref='shipment', lazy=True)
    simulation_state = db.relationship('SimulationState', backref='shipment', uselist=False, lazy=True)

    def calculate_distance_and_eta(self):
        self.distance_km = haversine(self.origin_lat, self.origin_lng, self.dest_lat, self.dest_lng)
        self.eta = datetime.utcnow() + calculate_eta(self.distance_km)

    def to_dict(self):
        return {
            'id': self.id,
            'tracking': self.tracking,
            'title': self.title,
            'origin': {'lat': self.origin_lat, 'lng': self.origin_lng, 'address': self.origin_address},
            'destination': {'lat': self.dest_lat, 'lng': self.dest_lng, 'address': self.dest_address},
            'distance_km': self.distance_km,
            'eta': self.eta.isoformat() + 'Z' if self.eta else None,
            'status': self.status,
            'created_at': self.created_at.isoformat() + 'Z',
            'checkpoints': [cp.to_dict() for cp in self.checkpoints],
            'history': [h.to_dict() for h in self.history]
        }

class Checkpoint(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipment.id'), nullable=False)
    position = db.Column(db.Integer, nullable=False)
    lat = db.Column(db.Float, nullable=False)
    lng = db.Column(db.Float, nullable=False)
    label = db.Column(db.String(100), nullable=False)
    note = db.Column(db.Text, nullable=True)
    status = db.Column(db.String(50), nullable=True)
    proof_photo = db.Column(db.String(500), nullable=True)
    timestamp = db.Column(db.DateTime, default=datetime.utcnow)

    def to_dict(self):
        return {
            'id': self.id,
            'shipment_id': self.shipment_id,
            'position': self.position,
            'lat': self.lat,
            'lng': self.lng,
            'label': self.label,
            'note': self.note,
            'status': self.status,
            'proof_photo': self.proof_photo,
            'timestamp': self.timestamp.isoformat() + 'Z' if self.timestamp else None
        }

class Subscriber(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipment.id'), nullable=False)
    email = db.Column(db.String(120), nullable=False)
    is_active = db.Column(db.Boolean, default=True)
    __table_args__ = (db.UniqueConstraint('shipment_id', 'email', name='uix_shipment_email'),)

class SimulationState(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipment.id'), nullable=False)
    tracking = db.Column(db.String(50), nullable=False)
    status = db.Column(db.String(20), nullable=False, default='running')
    current_position = db.Column(db.Integer, nullable=False, default=0)
    waypoints = db.Column(db.Text, nullable=False)
    current_time = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    num_points = db.Column(db.Integer, nullable=False)
    step_hours = db.Column(db.Integer, nullable=False)

    def to_dict(self):
        return {
            'tracking': self.tracking,
            'status': self.status,
            'current_position': self.current_position,
            'waypoints': json.loads(self.waypoints),
            'current_time': self.current_time.isoformat() + 'Z',
            'num_points': self.num_points,
            'step_hours': self.step_hours
        }

# Pydantic models
class Coordinate(BaseModel):
    lat: float
    lng: float

    @validator('lat')
    def check_lat(cls, v):
        if not -90 <= v <= 90:
            raise ValueError('Latitude must be between -90 and 90')
        return v

    @validator('lng')
    def check_lng(cls, v):
        if not -180 <= v <= 180:
            raise ValueError('Longitude must be between -180 and 180')
        return v

class CheckpointCreate(BaseModel):
    lat: TypingOptional[float] = None
    lng: TypingOptional[float] = None
    address: TypingOptional[str] = None
    label: str
    note: TypingOptional[str] = None
    status: TypingOptional[str] = None
    proof_photo: TypingOptional[str] = None

    @validator('label')
    def check_label(cls, v):
        v = bleach.clean(v.strip())
        if not v:
            raise ValueError('Label cannot be empty')
        return v

    @validator('address', always=True)
    def check_coordinates_or_address(cls, v, values):
        if not v and (values.get('lat') is None or values.get('lng') is None):
            raise ValueError('Either address or lat/lng must be provided')
        if v and (values.get('lat') is not None or values.get('lng') is not None):
            raise ValueError('Provide either address or lat/lng, not both')
        return bleach.clean(v.strip()) if v else v

class ShipmentCreate(BaseModel):
    tracking_number: str
    title: str = "Consignment"
    origin: Union[Dict[str, float], str]
    destination: Union[Dict[str, float], str]
    status: str = ShipmentStatus.CREATED.value

    @validator('tracking_number')
    def check_tracking_number(cls, v):
        v = bleach.clean(v.strip())
        if not v or not re.match(r'^\d{10,12}$', v):
            raise ValueError('Tracking number must be 10-12 digits')
        return v

    @validator('title')
    def check_title(cls, v):
        v = bleach.clean(v.strip())
        if not v:
            raise ValueError('Title cannot be empty')
        return v

    @validator('origin', 'destination')
    def check_coordinates_or_address(cls, v):
        if isinstance(v, dict):
            if 'lat' not in v or 'lng' not in v:
                raise ValueError('Coordinates must be a dict with lat and lng')
            Coordinate(**v)
        elif not isinstance(v, str) or not v.strip():
            raise ValueError('Address must be a non-empty string')
        return bleach.clean(v.strip()) if isinstance(v, str) else v

    @validator('status')
    def check_status(cls, v):
        if v not in [status.value for status in ShipmentStatus]:
            raise ValueError(f"Status must be one of: {', '.join(status.value for status in ShipmentStatus)}")
        return v

# Helper functions
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

def calculate_eta(distance_km, speed_kmh=None):
    speed_kmh = speed_kmh or app.config['DEFAULT_SPEED_KMH']
    return timedelta(hours=distance_km / speed_kmh)

@cache.memoize(timeout=3600)
def geocode_address(address: str) -> Dict[str, float]:
    for attempt in range(3):
        try:
            response = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": address, "format": "json", "limit": 1},
                headers={"User-Agent": "CourierTrackingApp/1.0 (contact@yourdomain.com)"},
                timeout=app.config['GEOCODING_TIMEOUT']
            )
            response.raise_for_status()
            data = response.json()
            if not data:
                raise ValueError(f"Geocoding failed for address: {address}")
            location = data[0]
            return {"lat": float(location['lat']), "lng": float(location['lon'])}
        except requests.RequestException as e:
            logger.warning(f"Geocoding attempt {attempt + 1} failed for {address}: {e}")
            if attempt == 2:
                logger.error(f"Nominatim geocoding error: {e}")
                raise ValueError(f"Failed to geocode address: {address}")
            time.sleep(2 ** attempt)

def compress_image(file_content, max_size=(800, 800), quality=None):
    quality = quality or app.config['IMAGE_QUALITY']
    try:
        img = Image.open(BytesIO(file_content))
        if img.format not in ['JPEG', 'PNG']:
            raise ValueError("Only JPEG or PNG images are supported")
        if img.mode in ('RGBA', 'P'):
            img = img.convert('RGB')
        img.thumbnail(max_size, Image.Resampling.LANCZOS)
        output = BytesIO()
        img.save(output, format='JPEG', quality=quality, optimize=True)
        compressed_content = output.getvalue()
        if len(compressed_content) > app.config['MAX_FILE_SIZE']:
            raise ValueError(f"Compressed image size ({len(compressed_content)} bytes) exceeds {app.config['MAX_FILE_SIZE']} bytes")
        logger.debug(f"Compressed image from {len(file_content)} to {len(compressed_content)} bytes")
        return compressed_content
    except Exception as e:
        logger.error(f"Image compression failed: {e}")
        raise ValueError(f"Failed to compress image: {str(e)}")

def upload_to_s3(file_content, filename):
    if not all([app.config.get('AWS_ACCESS_KEY_ID'), app.config.get('AWS_SECRET_ACCESS_KEY'), app.config.get('S3_BUCKET')]):
        logger.warning("S3 credentials not configured, skipping S3 upload")
        return None
    try:
        s3_client = boto3.client(
            's3',
            aws_access_key_id=app.config['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=app.config['AWS_SECRET_ACCESS_KEY']
        )
        bucket = app.config['S3_BUCKET']
        key = f"proof_photos/{secure_filename(filename)}"
        s3_client.put_object(Bucket=bucket, Key=key, Body=file_content, ContentType='image/jpeg')
        url = f"https://{bucket}.s3.amazonaws.com/{key}"
        logger.info(f"Uploaded photo to S3: {url}")
        return url
    except ClientError as e:
        logger.error(f"S3 upload failed: {e}")
        return None

def save_locally(file_content, filename):
    try:
        upload_dir = os.path.join(app.config['UPLOAD_FOLDER'], 'proof_photos')
        os.makedirs(upload_dir, exist_ok=True)
        file_path = os.path.join(upload_dir, secure_filename(filename))
        with open(file_path, 'wb') as f:
            f.write(file_content)
        url = f"{app.config['APP_BASE_URL']}/{file_path}"
        logger.info(f"Saved photo locally: {url}")
        return url
    except Exception as e:
        logger.error(f"Local file save failed: {e}")
        return None

def get_smtp_connection():
    server = smtplib.SMTP(app.config['SMTP_HOST'], app.config['SMTP_PORT'], timeout=5)
    server.starttls()
    server.login(app.config['SMTP_USER'], app.config['SMTP_PASS'])
    return server

# Realistic simulation logic
class RealisticSimulator:
    @staticmethod
    def geocode_with_fallback(address_or_coords: Union[str, Tuple[float, float]]) -> Tuple[float, float]:
        if isinstance(address_or_coords, tuple):
            return address_or_coords
        cached = cache.get(f"geocode:{address_or_coords}")
        if cached:
            return cached['lat'], cached['lng']
        try:
            coords = geocode_address(address_or_coords)
            cache.set(f"geocode:{address_or_coords}", coords, timeout=3600)
            return coords['lat'], coords['lng']
        except:
            raise ValueError(f"Failed to geocode: {address_or_coords}")

    @staticmethod
    def get_realistic_route(origin: Tuple[float, float], destination: Tuple[float, float], profile: str = 'driving') -> List[Tuple[float, float]]:
        if not app.config['OSRM_ENABLED']:
            return RealisticSimulator.generate_linear_route(origin, destination)
        try:
            origin_ll = f"{origin[1]},{origin[0]}"  # OSRM expects lng,lat
            dest_ll = f"{destination[1]},{destination[0]}"
            url = f"{app.config['OSRM_URL']}/route/v1/{profile}/{origin_ll};{dest_ll}?overview=full&alternatives=false"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            data = response.json()
            if 'routes' not in data or not data['routes']:
                raise ValueError("No route found")
            route = data['routes'][0]
            coordinates = route['geometry']['coordinates']
            max_points = app.config['MAX_WAYPOINTS']
            step = max(1, len(coordinates) // max_points)
            waypoints = [(lat, lng) for lng, lat in coordinates[::step]]
            logger.info(f"Generated {len(waypoints)} realistic waypoints from OSRM")
            return waypoints
        except Exception as e:
            logger.warning(f"OSRM route failed: {e}, falling back to linear")
            return RealisticSimulator.generate_linear_route(origin, destination)

    @staticmethod
    def generate_linear_route(origin: Tuple[float, float], destination: Tuple[float, float], num_points: int = 15) -> List[Tuple[float, float]]:
        waypoints = []
        geod = Geodesic.WGS84
        num_points = min(num_points, app.config['MAX_WAYPOINTS'])
        for i in range(num_points + 1):
            t = i / num_points
            lat = origin[0] + t * (destination[0] - origin[0])
            lng = origin[1] + t * (destination[1] - origin[1])
            deviation = (1 - (t * (1 - t)) * 4) * 0.005 * random.uniform(0.5, 2.0)
            bearing = geod.Inverse(origin[0], origin[1], destination[0], destination[1])['azi1']
            perp_bearing = bearing + 90
            perp_dist = deviation * random.uniform(0.7, 1.3)
            new_pos = geod.Direct(lat, lng, perp_bearing, perp_dist)
            waypoints.append((new_pos['lat2'], new_pos['lon2']))
        return waypoints

# Celery Tasks
@shared_task(bind=True, max_retries=3, retry_backoff=2, retry_jitter=True)
def send_checkpoint_email_async(self, shipment_dict: dict, checkpoint_dict: dict, contact: str):
    try:
        utc_time = datetime.fromisoformat(checkpoint_dict['timestamp'].replace('Z', '+00:00'))
        wat_time = utc_time + timedelta(hours=1)
        wat_time_str = wat_time.strftime("%Y-%m-%d %I:%M:%S %p WAT")

        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Update: Shipment {shipment_dict['tracking']} - {checkpoint_dict['label']}"
        msg['From'] = app.config['SMTP_FROM']
        msg['To'] = contact

        text = f"""DHL Tracking Update

Shipment: {shipment_dict['title']} ({shipment_dict['tracking']})
Status: {shipment_dict['status']}
Updated: {wat_time_str}

Checkpoint:
- Label: {checkpoint_dict['label']}
- Location: ({checkpoint_dict['lat']:.4f}, {checkpoint_dict['lng']:.4f})
- Note: {checkpoint_dict['note'] or 'None'}
{f"- Photo: {checkpoint_dict['proof_photo']}" if checkpoint_dict['proof_photo'] else ""}

Track: {app.config['APP_BASE_URL']}/track/{shipment_dict['tracking']}
Unsubscribe: {app.config['APP_BASE_URL']}/unsubscribe/{shipment_dict['tracking']}?email={contact}
"""
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shipment Update</title>
    <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
</head>
<body class="bg-gray-100 font-sans">
    <div class="max-w-2xl mx-auto bg-white shadow-md rounded-lg overflow-hidden">
        <div class="bg-blue-600 text-white text-center py-4">
            <img src="{app.config['APP_BASE_URL']}/static/dhl-logo.png" alt="DHL Logo" class="h-12 mx-auto" style="max-width: 150px;">
            <h1 class="text-2xl font-bold mt-2">Shipment Update</h1>
        </div>
        <div class="p-6">
            <h2 class="text-xl font-semibold text-gray-800">Shipment: {shipment_dict['title']} ({shipment_dict['tracking']})</h2>
            <div class="mt-4 space-y-2">
                <p><span class="font-medium text-gray-700">Status:</span> {shipment_dict['status']}</p>
                <p><span class="font-medium text-gray-700">Updated:</span> {wat_time_str}</p>
            </div>
            <h3 class="text-lg font-semibold text-gray-800 mt-6">Latest Checkpoint</h3>
            <div class="mt-2 space-y-2">
                <p><span class="font-medium text-gray-700">Label:</span> {checkpoint_dict['label']}</p>
                <p><span class="font-medium text-gray-700">Location:</span> ({checkpoint_dict['lat']:.4f}, {checkpoint_dict['lng']:.4f})</p>
                <p><span class="font-medium text-gray-700">Note:</span> {checkpoint_dict['note'] or 'None'}</p>
                {f'<p><span class="font-medium text-gray-700">Photo:</span> <a href="{checkpoint_dict["proof_photo"]}" class="text-blue-600 hover:underline">View</a></p>' if checkpoint_dict['proof_photo'] else ''}
            </div>
            <div class="mt-6 text-center">
                <a href="{app.config['APP_BASE_URL']}/track/{shipment_dict['tracking']}" class="inline-block bg-blue-600 text-white font-semibold py-2 px-4 rounded hover:bg-blue-700">
                    Track Shipment
                </a>
            </div>
        </div>
        <div class="bg-gray-200 text-gray-600 text-center py-4 text-sm">
            <p>You're receiving this email because you're subscribed to updates for this shipment.</p>
            <p><a href="{app.config['APP_BASE_URL']}/unsubscribe/{shipment_dict['tracking']}?email={contact}" class="text-blue-600 hover:underline">Unsubscribe</a></p>
            <p>&copy; {datetime.now().year} DHL Tracking Service. All rights reserved.</p>
        </div>
    </div>
</body>
</html>
"""
        text_part = MIMEText(text, 'plain')
        html_part = MIMEText(html, 'html')
        msg.attach(text_part)
        msg.attach(html_part)

        @retry(tries=3, delay=2, backoff=2, exceptions=(smtplib.SMTPException, smtplib.SMTPServerDisconnected))
        def send_email():
            server = get_smtp_connection()
            try:
                server.send_message(msg)
                logger.info(f"Sent email to {contact} for checkpoint {checkpoint_dict['id']}")
            finally:
                server.quit()

        send_email()
    except Exception as e:
        logger.error(f"Email error for {contact}: {e}")
        raise self.retry(exc=e)

@shared_task(bind=True, max_retries=3)
def run_enhanced_simulation(self, shipment_id, num_points=15, step_hours=2, use_realistic=True):
    try:
        shipment = Shipment.query.get(shipment_id)
        if not shipment:
            logger.error(f"Shipment {shipment_id} not found")
            return

        simulation_state = SimulationState.query.filter_by(shipment_id=shipment_id).first()
        if simulation_state and simulation_state.status == 'running':
            logger.info(f"Simulation already running for {shipment.tracking}")
            return

        origin = (shipment.origin_lat, shipment.origin_lng)
        destination = (shipment.dest_lat, shipment.dest_lng)
        waypoints = (RealisticSimulator.get_realistic_route(origin, destination)
                     if use_realistic else RealisticSimulator.generate_linear_route(origin, destination, num_points))

        if not simulation_state:
            simulation_state = SimulationState(
                shipment_id=shipment.id,
                tracking=shipment.tracking,
                status='running',
                current_position=0,
                waypoints=json.dumps(waypoints),
                current_time=datetime.utcnow(),
                num_points=len(waypoints) - 1,
                step_hours=step_hours
            )
            db.session.add(simulation_state)
        else:
            simulation_state.waypoints = json.dumps(waypoints)
            simulation_state.status = 'running'
            simulation_state.current_position = 0
        db.session.commit()

        traffic_factor = random.uniform(1.0 - app.config['TRAFFIC_VARIABILITY'], 1.0 + app.config['TRAFFIC_VARIABILITY'])
        statuses = [s.value for s in ShipmentStatus]
        status_points = [0, len(waypoints)//3, len(waypoints)*2//3, len(waypoints)-1]
        current_time = simulation_state.current_time

        for i, (lat, lng) in enumerate(waypoints):
            if simulation_state.status != 'running':
                break
            status = next((s for j, s in enumerate(statuses) if i >= status_points[j]), ShipmentStatus.IN_TRANSIT.value)
            time_step = timedelta(hours=step_hours * traffic_factor)
            checkpoint_time = current_time + time_step * (i / len(waypoints))

            checkpoint = Checkpoint(
                shipment_id=shipment.id,
                position=i,
                lat=lat,
                lng=lng,
                label=f"Route Point {i+1}/{len(waypoints)}",
                note=f"Progress: {i/len(waypoints)*100:.1f}% - Traffic: {'Heavy' if traffic_factor > 1.2 else 'Normal'}",
                status=status if i in status_points else None,
                timestamp=checkpoint_time
            )

            with db.session.begin():
                db.session.add(checkpoint)
                if status and status != shipment.status:
                    shipment.status = status
                    db.session.add(StatusHistory(shipment=shipment, status=status))
                    if status == ShipmentStatus.DELIVERED.value:
                        shipment.eta = checkpoint_time
                simulation_state.current_position = i
                simulation_state.current_time = checkpoint_time

            socketio.emit('update', shipment.to_dict(), room=shipment.tracking)
            for subscriber in shipment.subscribers:
                if subscriber.is_active:
                    send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)

            current_time = checkpoint_time
            socketio.sleep(random.uniform(1, 3))

        if simulation_state.status == 'running':
            simulation_state.status = 'completed'
            db.session.commit()
        logger.info(f"Simulation completed for {shipment.tracking}")
    except Exception as e:
        logger.error(f"Simulation error for {shipment_id}: {e}")
        db.session.rollback()
        raise self.retry(exc=e)

# Routes
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/login', methods=['GET', 'POST'])
def login():
    try:
        if request.method == 'POST':
            password = request.form.get('password', '')
            if check_password_hash(app.config['ADMIN_PASSWORD_HASH'], password):
                session['is_admin'] = True
                return redirect(url_for('admin'))
            return render_template('login.html', error='Invalid password'), 401
        return render_template('login.html')
    except Exception as e:
        logger.error(f"Login error: {e}")
        return render_template('error.html', error='Login failed'), 500

@app.route('/logout', methods=['GET', 'POST'])
def logout():
    session.pop('is_admin', None)
    return redirect(url_for('login'))

@app.route('/track', methods=['GET'])
def track_redirect():
    try:
        tracking = bleach.clean(request.args.get('tracking', '').strip())
        if tracking:
            return redirect(url_for('track', tracking=tracking))
        return render_template('error.html', error='Tracking number required'), 400
    except Exception as e:
        logger.error(f"Track redirect error: {e}")
        return render_template('error.html', error='Failed to redirect'), 500

@app.route('/track/<tracking>')
@cache.cached(timeout=300, query_string=True)
def track(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('tracking.html', shipment=None), 404
        pagination = Checkpoint.query.filter_by(shipment_id=shipment.id).order_by(Checkpoint.position).paginate(page=page, per_page=per_page, error_out=False)
        history = [h.to_dict() for h in shipment.history]
        return render_template('tracking.html',
                              shipment=shipment,
                              checkpoints=pagination.items,
                              history=history,
                              origin_lat=shipment.origin_lat,
                              origin_lng=shipment.origin_lng,
                              dest_lat=shipment.dest_lat,
                              dest_lng=shipment.dest_lng,
                              pagination=pagination)
    except Exception as e:
        logger.error(f"Track error for {tracking}: {e}")
        return render_template('tracking.html', shipment=None, error='Failed to retrieve tracking data'), 500

@app.route('/admin', methods=['GET', 'POST'])
@admin_required
def admin():
    try:
        shipments = Shipment.query.all()
        simulation_states = SimulationState.query.all()
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states)
    except Exception as e:
        logger.error(f"Admin dashboard error: {e}")
        return render_template('error.html', error='Failed to load admin dashboard'), 500

@app.route('/pause_simulation/<tracking>', methods=['POST'])
@admin_required
def pause_simulation(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
        simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
        if not simulation_state or simulation_state.status != 'running':
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='No active simulation'), 400
        with db.session.begin():
            simulation_state.status = 'paused'
        return redirect(url_for('admin'))
    except Exception as e:
        logger.error(f"Pause simulation error for {tracking}: {e}")
        return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to pause simulation'), 500

@app.route('/continue_simulation/<tracking>', methods=['POST'])
@admin_required
def continue_simulation(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
        simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
        if not simulation_state or simulation_state.status != 'paused':
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='No paused simulation'), 400
        with db.session.begin():
            simulation_state.status = 'running'
        run_enhanced_simulation.delay(shipment.id, simulation_state.num_points, simulation_state.step_hours, app.config['OSRM_ENABLED'])
        return redirect(url_for('admin'))
    except Exception as e:
        logger.error(f"Continue simulation error for {tracking}: {e}")
        return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to continue simulation'), 500

@app.route('/shipments/<tracking>/simulate', methods=['POST'])
@admin_required
def run_simulation(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
        simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
        if simulation_state and simulation_state.status == 'running':
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Simulation already running'), 400
        run_enhanced_simulation.delay(shipment.id, num_points=15, step_hours=2, use_realistic=app.config['OSRM_ENABLED'])
        return redirect(url_for('admin'))
    except Exception as e:
        logger.error(f"Simulation error for {tracking}: {e}")
        return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to run simulation'), 500

@app.route('/shipments', methods=['GET'])
@admin_required
def list_shipments():
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 20, type=int)
        pagination = Shipment.query.paginate(page=page, per_page=per_page, error_out=False)
        return jsonify({
            'shipments': [s.to_dict() for s in pagination.items],
            'page': pagination.page,
            'pages': pagination.pages,
            'total': pagination.total
        })
    except Exception as e:
        logger.error(f"Shipment list error: {e}")
        return jsonify({'error': 'Failed to retrieve shipments'}), 500

@app.route('/shipments', methods=['POST'])
@admin_required
def create_shipment():
    try:
        data = request.get_json() or request.form
        data = {k: bleach.clean(v.strip()) if isinstance(v, str) else v for k, v in data.items()}
        shipment_data = ShipmentCreate(**data).dict()
        origin = shipment_data['origin']
        destination = shipment_data['destination']
        if isinstance(origin, str):
            coords = geocode_address(origin)
            origin_lat, origin_lng = coords['lat'], coords['lng']
            origin_address = origin
        else:
            origin_lat, origin_lng = origin['lat'], origin['lng']
            origin_address = None
        if isinstance(destination, str):
            coords = geocode_address(destination)
            dest_lat, dest_lng = coords['lat'], coords['lng']
            dest_address = destination
        else:
            dest_lat, dest_lng = destination['lat'], destination['lng']
            dest_address = None
        shipment = Shipment(
            tracking=shipment_data['tracking_number'],
            title=shipment_data['title'],
            origin_lat=origin_lat,
            origin_lng=origin_lng,
            dest_lat=dest_lat,
            dest_lng=dest_lng,
            origin_address=origin_address,
            dest_address=dest_address,
            status=shipment_data['status']
        )
        shipment.calculate_distance_and_eta()
        with db.session.begin():
            db.session.add(shipment)
            db.session.add(StatusHistory(shipment=shipment, status=shipment_data['status']))
        socketio.emit('update', shipment.to_dict(), room=shipment.tracking)
        if request.form:
            return redirect(url_for('admin'))
        return jsonify(shipment.to_dict()), 201
    except ValidationError as e:
        error = str(e)
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=error), 400
        return jsonify({'error': error}), 400
    except Exception as e:
        logger.error(f"Shipment creation error: {e}")
        error = 'Failed to create shipment'
        if isinstance(e, IntegrityError):
            error = 'Tracking number already exists'
            db.session.rollback()
            status = 409
        else:
            status = 500
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=error), status
        return jsonify({'error': error}), status

@app.route('/shipments/<tracking>/checkpoints', methods=['POST'])
@admin_required
@limiter.limit("50 per minute")
def add_checkpoint(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            if request.form:
                return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
            return jsonify({'error': 'Shipment not found'}), 404
        data = request.get_json() or request.form
        data = {k: bleach.clean(v.strip()) if isinstance(v, str) else v for k, v in data.items()}
        proof_photo = None
        if 'proof_photo' in request.files:
            file = request.files['proof_photo']
            if file and file.filename:
                file_content = file.read()
                if len(file_content) > app.config['MAX_FILE_SIZE']:
                    error = 'Photo too large (max 10MB)'
                    if request.form:
                        return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=error), 400
                    return jsonify({'error': error}), 400
                try:
                    compressed_content = compress_image(file_content)
                    filename = f"{tracking}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jpg"
                    proof_photo = upload_to_s3(compressed_content, filename) or save_locally(compressed_content, filename)
                    if not proof_photo:
                        error = 'Failed to store photo'
                        if request.form:
                            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=error), 500
                        return jsonify({'error': error}), 500
                except ValueError as e:
                    if request.form:
                        return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=str(e)), 400
                    return jsonify({'error': str(e)}), 400
        else:
            proof_photo = data.get('proof_photo')
        checkpoint_data = CheckpointCreate(
            **{k: v for k, v in data.items() if k != 'proof_photo'},
            proof_photo=proof_photo
        ).dict()
        if checkpoint_data['address']:
            coords = geocode_address(checkpoint_data['address'])
            lat, lng = coords['lat'], coords['lng']
        else:
            lat, lng = checkpoint_data['lat'], checkpoint_data['lng']
        position = db.session.query(db.func.max(Checkpoint.position)).filter_by(shipment_id=shipment.id).scalar() or 0
        checkpoint = Checkpoint(
            shipment_id=shipment.id,
            position=position + 1,
            lat=lat,
            lng=lng,
            label=checkpoint_data['label'],
            note=checkpoint_data['note'],
            status=checkpoint_data['status'],
            proof_photo=checkpoint_data.get('proof_photo'),
            timestamp=datetime.utcnow()
        )
        with db.session.begin():
            db.session.add(checkpoint)
            if checkpoint_data['status']:
                shipment.status = checkpoint_data['status']
                db.session.add(StatusHistory(shipment=shipment, status=checkpoint_data['status']))
                if shipment.status == ShipmentStatus.DELIVERED.value:
                    shipment.eta = checkpoint.timestamp
        socketio.emit('update', shipment.to_dict(), room=shipment.tracking)
        for subscriber in shipment.subscribers:
            if subscriber.is_active and subscriber.email:
                send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
        if request.form:
            return redirect(url_for('admin'))
        return jsonify(checkpoint.to_dict()), 201
    except Exception as e:
        logger.error(f"Checkpoint creation error for {tracking}: {e}")
        error = 'Failed to add checkpoint'
        status = 500
        if isinstance(e, ValidationError):
            error = str(e)
            status = 400
        elif isinstance(e, ValueError):
            error = str(e)
            status = 400
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=error), status
        return jsonify({'error': error}), status

@app.route('/shipments/<tracking>/subscribe', methods=['POST'])
def subscribe(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        data = request.get_json()
        email = bleach.clean(data.get('email', '').strip())
        if not email:
            return jsonify({'error': 'Email is required'}), 400
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return jsonify({'error': 'Shipment not found'}), 404
        subscriber = Subscriber(shipment_id=shipment.id, email=email)
        with db.session.begin():
            db.session.add(subscriber)
        return jsonify({'message': 'Subscribed successfully'}), 201
    except IntegrityError:
        db.session.rollback()
        return jsonify({'error': 'Already subscribed'}), 409
    except Exception as e:
        logger.error(f"Subscription error for {tracking}: {e}")
        return jsonify({'error': 'Failed to subscribe'}), 500

@app.route('/track_multiple', methods=['POST'])
def track_multiple():
    try:
        tracking_numbers = [bleach.clean(tn.strip()) for tn in request.get_json() if tn.strip()]
        if not tracking_numbers:
            return jsonify({'error': 'Tracking numbers required'}), 400
        shipments = [s.to_dict() for s in Shipment.query.filter(Shipment.tracking.in_(tracking_numbers)).all()]
        return jsonify({'shipments': shipments})
    except Exception as e:
        logger.error(f"Track multiple error: {e}")
        return jsonify({'error': 'Failed to track shipments'}), 500

@app.route('/unsubscribe/<tracking>', methods=['GET'])
def unsubscribe(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        email = bleach.clean(request.args.get('email', '').strip())
        if not email:
            return render_template('error.html', error='Email is required'), 400
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('error.html', error='Shipment not found'), 404
        subscriber = Subscriber.query.filter_by(shipment_id=shipment.id, email=email).first()
        if not subscriber:
            return render_template('error.html', error='Subscription not found'), 404
        with db.session.begin():
            subscriber.is_active = False
        return render_template('error.html', error='Unsubscribed successfully'), 200
    except Exception as e:
        logger.error(f"Unsubscribe error for {tracking}: {e}")
        return render_template('error.html', error='Failed to unsubscribe'), 500

# Socket.IO Events
@socketio.on('subscribe')
def handle_subscribe(tracking):
    join_room(tracking)
    logger.info(f"Client subscribed to updates for {tracking}")

# Initialize Database
with app.app_context():
    db.create_all()

if __name__ == '__main__':
    socketio.run(app, debug=True, host='0.0.0.0', port=5000)
