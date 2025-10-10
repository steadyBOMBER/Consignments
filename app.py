import os
import logging
import smtplib
import random
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, redirect, url_for
from flask_jwt_extended import jwt_required, create_access_token
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_socketio import SocketIO, emit, join_room
from flask_caching import Cache
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from werkzeug.security import check_password_hash
from twilio.rest import Client
from math import radians, sin, cos, sqrt, atan2
from typing import Dict, Union, Optional as TypingOptional
from pydantic import BaseModel, validator, ValidationError
from logging.handlers import RotatingFileHandler
from celery import Celery, shared_task
import redis
import bleach
try:
    from retry import retry
except ImportError:
    retry = None
try:
    from twilio.base.exceptions import TwilioRestException
except ImportError:
    TwilioRestException = Exception
import requests
import base64
from enum import Enum

# Initialize Flask app
app = Flask(__name__, template_folder='templates')

# Load required environment variables and fail early if missing
required_vars = [
    'SECRET_KEY', 'SQLALCHEMY_DATABASE_URI', 'JWT_SECRET_KEY', 'CELERY_BROKER_URL',
    'CELERY_RESULT_BACKEND', 'REDIS_URL', 'SMTP_HOST', 'SMTP_PORT', 'SMTP_USER', 'SMTP_PASS',
    'SMTP_FROM', 'APP_BASE_URL', 'ADMIN_EMAIL', 'ADMIN_PHONE', 'TELEGRAM_TOKEN', 'TWILIO_ACCOUNT_SID',
    'TWILIO_AUTH_TOKEN', 'TWILIO_PHONE_NUMBER', 'ADMIN_PASSWORD_HASH', 'FEDEX_CLIENT_ID',
    'FEDEX_CLIENT_SECRET', 'FEDEX_ACCOUNT_NUMBER', 'FEDEX_METER_NUMBER'
]
for var in required_vars:
    value = os.environ.get(var)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {var}")
    app.config[var] = value

# Configure caching
cache = Cache(app, config={'CACHE_TYPE': 'redis', 'CACHE_REDIS_URL': app.config['REDIS_URL']})

# Shipment status enum
class ShipmentStatus(Enum):
    CREATED = 'Created'
    IN_TRANSIT = 'In Transit'
    OUT_FOR_DELIVERY = 'Out for Delivery'
    DELIVERED = 'Delivered'

# Configure logging
handler = RotatingFileHandler('app.log', maxBytes=1000000, backupCount=5)
handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]'))
app.logger.addHandler(handler)
app.logger.setLevel(logging.INFO)
logger = app.logger

# Initialize extensions
db = SQLAlchemy(app)
migrate = Migrate(app, db)
socketio = SocketIO(app, cors_allowed_origins="*")
limiter = Limiter(app, key_func=get_remote_address, default_limits=["200 per day", "50 per hour"])

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

# Initialize Redis
redis_client = redis.Redis.from_url(app.config['REDIS_URL'])

# Initialize Twilio
TWILIO_ACCOUNT_SID = os.getenv('TWILIO_ACCOUNT_SID')
TWILIO_AUTH_TOKEN = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_PHONE_NUMBER = os.getenv('TWILIO_PHONE_NUMBER')

# Admin sessions (stored in Redis)
admin_sessions = {}

# Helper function for SMTP connection
def get_smtp_connection():
    server = smtplib.SMTP(app.config['SMTP_HOST'], app.config['SMTP_PORT'])
    server.starttls()
    server.login(app.config['SMTP_USER'], app.config['SMTP_PASS'])
    return server

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
    tracking = db.Column(db.String(50), unique=True, nullable=False)
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
            'checkpoints': [cp.to_dict() for cp in self.checkpoints]
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
            'timestamp': self.timestamp.isoformat() + 'Z'
        }

class Subscriber(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipment.id'), nullable=False)
    email = db.Column(db.String(120), nullable=True)
    phone = db.Column(db.String(20), nullable=True)
    is_active = db.Column(db.Boolean, default=True)

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
        if not v:
            raise ValueError('Tracking number cannot be empty')
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

# Haversine formula for distance calculation
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance

def calculate_eta(distance_km, speed_kmh=50):
    return timedelta(hours=distance_km / speed_kmh)

# Geocoding function using Nominatim
@cache.memoize(timeout=86400)
def geocode_address(address: str) -> Dict[str, float]:
    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": address, "format": "json", "limit": 1},
            headers={"User-Agent": "CourierTrackingApp/1.0 (contact@yourdomain.com)"},
            timeout=5
        )
        response.raise_for_status()
        data = response.json()
        if not data:
            raise ValueError(f"Geocoding failed for address: {address}")
        location = data[0]
        return {"lat": float(location['lat']), "lng": float(location['lon'])}
    except requests.RequestException as e:
        logger.error(f"Nominatim geocoding error: {e}")
        raise ValueError(f"Failed to geocode address: {address}")

# FedEx API Configuration
FEDEX_API_URL = 'https://apis-sandbox.fedex.com/track/v1/trackingnumbers'  # Use 'https://apis.fedex.com' for production
fedex_token_cache = {}

def get_fedex_token():
    """Get OAuth token for FedEx API."""
    now = datetime.utcnow()
    if 'token' in fedex_token_cache and (now - fedex_token_cache['expires']) < timedelta(minutes=50):
        return fedex_token_cache['token']
    
    url = 'https://apis.fedex.com/oauth/token'
    payload = 'grant_type=client_credentials&scope=track'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f'Basic {base64.b64encode(f"{app.config["FEDEX_CLIENT_ID"]}:{app.config["FEDEX_CLIENT_SECRET"]}".encode()).decode()}'
    }
    try:
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()
        token_data = response.json()
        fedex_token_cache['token'] = token_data['access_token']
        fedex_token_cache['expires'] = now
        return token_data['access_token']
    except requests.RequestException as e:
        logger.error(f"FedEx token fetch failed: {e}")
        raise

def track_fedex_shipment(tracking_number):
    """Call FedEx Track API and return normalized data."""
    if not app.config['FEDEX_CLIENT_ID'] or not app.config['FEDEX_CLIENT_SECRET']:
        raise ValueError("FedEx credentials not configured.")
    
    token = get_fedex_token()
    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }
    payload = {
        "trackingInfo": [
            {
                "trackingNumberId": tracking_number
            }
        ],
        "includeDetailedScans": 1,
        "transactionContext": {
            "transactionId": f"track-{tracking_number}-{int(datetime.utcnow().timestamp())}"
        }
    }
    
    try:
        response = requests.post(FEDEX_API_URL, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if not data.get('output', {}).get('completeTrackResults'):
            raise ValueError("No tracking data found.")
        
        result = data['output']['completeTrackResults'][0]
        track_info = result['trackResults'][0]
        
        shipment = {
            'tracking': tracking_number,
            'title': track_info.get('packageIdentifier', {}).get('value', 'FedEx Shipment'),
            'status': track_info.get('latestStatus', {}).get('status', 'Unknown'),
            'eta': track_info.get('estimatedDeliveryTimestamp', ''),
            'distance_km': None,
            'origin': {'lat': None, 'lng': None, 'address': track_info.get('shipperAddress', {}).get('addressLines', [''])[0] if track_info.get('shipperAddress') else ''},
            'destination': {'lat': None, 'lng': None, 'address': track_info.get('deliveryAddress', {}).get('addressLines', [''])[0] if track_info.get('deliveryAddress') else ''},
            'checkpoints': [],
            'history': []
        }
        
        for event in track_info.get('events', []):
            checkpoint = {
                'id': event.get('occurrenceTimestamp', ''),
                'position': len(shipment['checkpoints']) + 1,
                'lat': event.get('location', {}).get('latitude', None),
                'lng': event.get('location', {}).get('longitude', None),
                'label': event.get('eventDescription', ''),
                'note': event.get('statusDescription', ''),
                'status': event.get('eventCode', ''),
                'timestamp': event.get('occurrenceTimestamp', '')
            }
            shipment['checkpoints'].append(checkpoint)
        
        for status in track_info.get('statusHistory', []):
            history_entry = {
                'status': status.get('status', ''),
                'timestamp': status.get('timestamp', '')
            }
            shipment['history'].append(history_entry)
        
        return shipment
    except requests.RequestException as e:
        logger.error(f"FedEx API request failed: {e}")
        raise
    except (KeyError, ValueError) as e:
        logger.error(f"FedEx API parsing failed: {e}")
        raise

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

        text = f"""Courier Tracking Update

Shipment: {shipment_dict['title']} ({shipment_dict['tracking']})
Status: {shipment_dict['status']}
Updated: {wat_time_str}

Checkpoint:
- Label: {checkpoint_dict['label']}
- Location: ({checkpoint_dict['lat']:.4f}, {checkpoint_dict['lng']:.4f})
- Note: {checkpoint_dict['note'] or 'None'}

Track: {app.config['APP_BASE_URL']}/track/{shipment_dict['tracking']}
Unsubscribe: {app.config['APP_BASE_URL']}/unsubscribe/{shipment_dict['tracking']}?email={contact}
"""
        text_part = MIMEText(text, 'plain')

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
            <img src="{app.config['APP_BASE_URL']}/static/logo.png" alt="Courier Logo" class="h-12 mx-auto" style="max-width: 150px;">
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
            <p>&copy; {datetime.now().year} Courier Tracking Service. All rights reserved.</p>
        </div>
    </div>
</body>
</html>
"""
        html_part = MIMEText(html, 'html')

        msg.attach(text_part)
        msg.attach(html_part)

        @retry(tries=3, delay=2, backoff=2, exceptions=(smtplib.SMTPException, smtplib.SMTPServerDisconnected))
        def send_email():
            try:
                server = get_smtp_connection()
                server.send_message(msg)
                logger.info(f"Sent email to {contact} for checkpoint {checkpoint_dict['id']} of shipment {shipment_dict['tracking']}")
            except smtplib.SMTPAuthenticationError:
                logger.error(f"SMTP authentication failed for {contact}")
                raise
            except smtplib.SMTPConnectError:
                logger.error(f"SMTP connection failed for {contact}")
                raise
            except smtplib.SMTPException as e:
                logger.error(f"SMTP error sending email to {contact}: {e}")
                raise

        send_email()
    except smtplib.SMTPAuthenticationError:
        logger.error(f"Authentication error sending email to {contact}")
        raise self.retry(exc=Exception("SMTP authentication failed"))
    except smtplib.SMTPConnectError:
        logger.error(f"Connection error sending email to {contact}")
        raise self.retry(exc=Exception("SMTP server unreachable"))
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending email to {contact}: {e}")
        raise self.retry(exc=e)
    except Exception as e:
        logger.error(f"Unexpected error sending email to {contact}: {e}")
        raise

@shared_task(bind=True, max_retries=3, retry_backoff=2, retry_jitter=True)
def send_checkpoint_sms_async(self, shipment_dict: dict, checkpoint_dict: dict, phone: str):
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_PHONE_NUMBER):
        return
    try:
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        message = client.messages.create(
            body=f"Update for {shipment_dict['tracking']}: {checkpoint_dict['label']} at {checkpoint_dict['timestamp']}",
            from_=TWILIO_PHONE_NUMBER,
            to=phone
        )
        logger.info(f"Sent SMS to {phone} for checkpoint {checkpoint_dict['id']} of shipment {shipment_dict['tracking']}")
    except TwilioRestException as e:
        logger.error(f"Twilio error sending SMS to {phone}: {e}")
        raise self.retry(exc=e)
    except Exception as e:
        logger.error(f"Unexpected error sending SMS to {phone}: {e}")
        raise

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
                access_token = create_access_token(identity='admin')
                response = redirect(url_for('admin'))
                response.set_cookie('access_token', access_token, httponly=True, secure=True)
                return response
            return render_template('login.html', error='Invalid password'), 401
        return render_template('login.html')
    except Exception as e:
        logger.error(f"Login error: {e}")
        return render_template('error.html', error='Login failed'), 400

@app.route('/track', methods=['GET'])
def track_redirect():
    try:
        tracking = bleach.clean(request.args.get('tracking', '').strip())
        carrier = request.args.get('carrier', 'dhl')  # Add carrier param
        if tracking:
            if carrier.lower() == 'fedex':
                return redirect(url_for('fedex_track', tracking=tracking))
            return redirect(url_for('track', tracking=tracking))
        return render_template('error.html', error='Tracking number required'), 400
    except Exception as e:
        logger.error(f"Track redirect error: {e}")
        return render_template('error.html', error='Failed to redirect to tracking'), 500

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
        checkpoints = pagination.items
        history = [h.to_dict() for h in shipment.history]
        return render_template('tracking.html', 
                              shipment=shipment, 
                              checkpoints=checkpoints, 
                              history=history,
                              origin_lat=shipment.origin_lat,
                              origin_lng=shipment.origin_lng,
                              dest_lat=shipment.dest_lat,
                              dest_lng=shipment.dest_lng,
                              pagination=pagination)
    except Exception as e:
        logger.error(f"Track error for {tracking}: {e}")
        return render_template('tracking.html', shipment=None, error='Failed to retrieve tracking data'), 500

@app.route('/fedex/track/<tracking>')
def fedex_track(tracking):
    """Route for FedEx tracking, rendering tracking.html with API data."""
    try:
        tracking = bleach.clean(tracking.strip())
        fedex_data = track_fedex_shipment(tracking)
        return render_template('tracking.html',
                              shipment=fedex_data,
                              checkpoints=fedex_data['checkpoints'],
                              history=fedex_data['history'],
                              origin_lat=fedex_data['origin']['lat'],
                              origin_lng=fedex_data['origin']['lng'],
                              dest_lat=fedex_data['destination']['lat'],
                              dest_lng=fedex_data['destination']['lng'])
    except Exception as e:
        logger.error(f"FedEx track error for {tracking}: {e}")
        return render_template('tracking.html', shipment=None, error='Failed to retrieve FedEx tracking data'), 500

@app.route('/admin', methods=['GET', 'POST'])
@jwt_required()
def admin():
    try:
        shipments = Shipment.query.all()
        simulation_states = SimulationState.query.all()

        if request.method == 'POST':
            form_type = request.form.get('form_type')
            try:
                if form_type == 'shipment':
                    return handle_shipment_form(request, shipments, simulation_states)
                elif form_type == 'checkpoint':
                    return handle_checkpoint_form(request, shipments, simulation_states)
                elif form_type == 'simulation':
                    return handle_simulation_form(request, shipments, simulation_states)
                elif form_type == 'subscribe':
                    return handle_subscribe_form(request, shipments, simulation_states)
                elif form_type == 'track_multiple':
                    return handle_track_multiple_form(request, shipments, simulation_states)
                else:
                    return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Invalid form type'), 400
            except Exception as e:
                logger.error(f"Admin form error: {e}")
                return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error=str(e)), 500

        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states)
    except Exception as e:
        logger.error(f"Admin dashboard error: {e}")
        return render_template('error.html', error='Failed to load admin dashboard'), 500

@app.route('/pause_simulation/<tracking>', methods=['POST'])
@jwt_required()
def pause_simulation(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
        simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
        if not simulation_state or simulation_state.status != 'running':
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='No active simulation to pause'), 400
        with db.session.begin():
            simulation_state.status = 'paused'
        return redirect(url_for('admin'))
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in pause simulation: {e}")
        return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Database error'), 500
    except Exception as e:
        logger.error(f"Pause simulation error for {tracking}: {e}")
        return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to pause simulation'), 500

@app.route('/continue_simulation/<tracking>', methods=['POST'])
@jwt_required()
def continue_simulation(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
        simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
        if not simulation_state or simulation_state.status != 'paused':
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='No paused simulation to continue'), 400
        with db.session.begin():
            simulation_state.status = 'running'
        run_simulation_async.delay(shipment.id, simulation_state.num_points, simulation_state.step_hours)
        return redirect(url_for('admin'))
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in continue simulation: {e}")
        return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Database error'), 500
    except Exception as e:
        logger.error(f"Continue simulation error for {tracking}: {e}")
        return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to continue simulation'), 500

@app.route('/ping', methods=['GET'])
@limiter.limit("1000 per day")
def ping():
    try:
        db.session.execute('SELECT 1')
        return jsonify({'status': 'alive', 'timestamp': datetime.utcnow().isoformat() + 'Z'}), 200
    except Exception as e:
        logger.error(f"Ping failed: {e}")
        return jsonify({'status': 'unhealthy'}), 500

@app.route('/health')
@limiter.limit("1000 per day")
def health():
    try:
        db.session.execute('SELECT 1')
        celery_status = celery.control.ping(timeout=1)
        return jsonify({
            'status': 'healthy',
            'database': 'connected',
            'celery': 'responsive' if celery_status else 'unresponsive',
            'timestamp': datetime.utcnow().isoformat() + 'Z'
        })
    except SQLAlchemyError as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 500
    except Exception as e:
        logger.error(f"Health check exception: {e}")
        return jsonify({'status': 'unhealthy'}), 500

@app.route('/shipments', methods=['GET'])
@jwt_required()
def list_shipments():
    try:
        shipments = Shipment.query.all()
        return [s.to_dict() for s in shipments]
    except Exception as e:
        logger.error(f"Shipment list error: {e}")
        return {'error': 'Failed to retrieve shipments'}, 500

@app.route('/shipments', methods=['POST'])
@jwt_required()
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
        socketio.emit('update', shipment.to_dict(), namespace='/', room=shipment.tracking)
        if request.form:
            return redirect(url_for('admin'))
        return shipment.to_dict(), 201
    except ValidationError as e:
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=str(e)), 400
        return {'error': str(e)}, 400
    except ValueError as e:
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=str(e)), 400
        return {'error': str(e)}, 400
    except IntegrityError:
        db.session.rollback()
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Tracking number already exists'), 409
        return {'error': 'Tracking number already exists'}, 409
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in shipment creation: {e}")
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Database error'), 500
        return {'error': 'Database error'}, 500
    except Exception as e:
        logger.error(f"Shipment creation error: {e}")
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to create shipment'), 500
        return {'error': 'Failed to create shipment'}, 500

@app.route('/shipments/<tracking>/checkpoints', methods=['POST'])
@jwt_required()
def add_checkpoint(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            if request.form:
                return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
            return {'error': 'Shipment not found'}, 404
        data = request.get_json() or request.form
        data = {k: bleach.clean(v.strip()) if isinstance(v, str) else v for k, v in data.items()}
        checkpoint_data = CheckpointCreate(**data).dict()
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
            proof_photo=checkpoint_data.get('proof_photo')
        )
        with db.session.begin():
            db.session.add(checkpoint)
            if checkpoint_data['status']:
                shipment.status = checkpoint_data['status']
                db.session.add(StatusHistory(shipment=shipment, status=checkpoint_data['status']))
                if shipment.status == ShipmentStatus.DELIVERED.value:
                    shipment.eta = checkpoint.timestamp
        socketio.emit('update', shipment.to_dict(), namespace='/', room=tracking)
        for subscriber in shipment.subscribers:
            if subscriber.is_active:
                if subscriber.email:
                    send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
                if subscriber.phone:
                    send_checkpoint_sms_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.phone)
        if request.form:
            return redirect(url_for('admin'))
        return checkpoint.to_dict(), 201
    except ValidationError as e:
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=str(e)), 400
        return {'error': str(e)}, 400
    except ValueError as e:
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=str(e)), 400
        return {'error': str(e)}, 400
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in checkpoint creation: {e}")
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Database error'), 500
        return {'error': 'Database error'}, 500
    except Exception as e:
        logger.error(f"Checkpoint creation error for {tracking}: {e}")
        if request.form:
            return render_template('admin.html', shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to add checkpoint'), 500
        return {'error': 'Failed to add checkpoint'}, 500

@app.route('/shipments/<tracking>/subscribe', methods=['POST'])
def subscribe(tracking):
    try:
        tracking = bleach.clean(tracking.strip())
        data = request.get_json()
        email = bleach.clean(data.get('email', '').strip()) if data.get('email') else None
        phone = bleach.clean(data.get('phone', '').strip()) if data.get('phone') else None
        if not email and not phone:
            return {'error': 'Email or phone is required'}, 400
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return {'error': 'Shipment not found'}, 404
        subscriber = Subscriber(shipment_id=shipment.id, email=email or '', phone=phone or '')
        with db.session.begin():
            db.session.add(subscriber)
        return {'message': 'Subscribed successfully'}, 201
    except IntegrityError:
        db.session.rollback()
        return {'error': 'Already subscribed'}, 409
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in subscription: {e}")
        return {'error': 'Database error'}, 500
    except Exception as e:
        logger.error(f"Subscription error for {tracking}: {e}")
        return {'error': 'Failed to subscribe'}, 500

@app.route('/track_multiple', methods=['POST'])
def track_multiple():
    try:
        tracking_numbers = [bleach.clean(tn.strip()) for tn in request.get_json() if tn.strip()]
        if not tracking_numbers:
            return {'error': 'Tracking numbers required'}, 400
        shipments = []
        for tn in tracking_numbers:
            shipment = Shipment.query.filter_by(tracking=tn).first()
            if shipment:
                shipments.append(shipment.to_dict())
        return {'shipments': shipments}
    except Exception as e:
        logger.error(f"Track multiple error: {e}")
        return {'error': 'Failed to track shipments'}, 500

# Helper functions for admin route
def handle_shipment_form(request, shipments, simulation_states):
    try:
        tracking = bleach.clean(request.form.get('tracking', '').strip())
        title = bleach.clean(request.form.get('title', 'Consignment').strip())
        origin = bleach.clean(request.form.get('origin', '').strip())
        destination = bleach.clean(request.form.get('destination', '').strip())
        status = request.form.get('status')

        if not tracking or not origin or not destination or not status:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Missing required fields'), 400

        if len(tracking) > 50 or len(title) > 100:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Tracking or title too long'), 400

        if status not in [status.value for status in ShipmentStatus]:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Invalid status'), 400

        data = {
            'tracking_number': tracking,
            'title': title,
            'origin': origin,
            'destination': destination,
            'status': status
        }
        ShipmentCreate(**data)
        if ',' in origin and all(x.replace('.', '').isdigit() for x in origin.split(',')):
            origin_lat, origin_lng = map(float, origin.split(','))
            origin_address = None
        else:
            coords = geocode_address(origin)
            origin_lat, origin_lng = coords['lat'], coords['lng']
            origin_address = origin
        if ',' in destination and all(x.replace('.', '').isdigit() for x in destination.split(',')):
            dest_lat, dest_lng = map(float, destination.split(','))
            dest_address = None
        else:
            coords = geocode_address(destination)
            dest_lat, dest_lng = coords['lat'], coords['lng']
            dest_address = destination
        shipment = Shipment(
            tracking=tracking,
            title=title,
            origin_lat=origin_lat,
            origin_lng=origin_lng,
            dest_lat=dest_lat,
            dest_lng=dest_lng,
            origin_address=origin_address,
            dest_address=dest_address,
            status=status
        )
        shipment.calculate_distance_and_eta()
        with db.session.begin():
            db.session.add(shipment)
            db.session.add(StatusHistory(shipment=shipment, status=status))
        socketio.emit('update', shipment.to_dict(), namespace='/', room=shipment.tracking)
        return redirect(url_for('admin'))
    except ValidationError as e:
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
    except ValueError as e:
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
    except IntegrityError:
        db.session.rollback()
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Tracking number already exists'), 409
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in shipment creation: {e}")
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Database error'), 500

def handle_checkpoint_form(request, shipments, simulation_states):
    try:
        tracking = bleach.clean(request.form.get('tracking', '').strip())
        location = bleach.clean(request.form.get('location', '').strip())
        label = bleach.clean(request.form.get('label', '').strip())
        note = bleach.clean(request.form.get('note', '').strip()) or None
        status = request.form.get('status') or None
        proof_photo = bleach.clean(request.form.get('proof_photo', '').strip()) or None

        if not tracking or not location or not label:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Missing required fields'), 400

        if len(tracking) > 50 or len(label) > 100 or (note and len(note) > 500) or (proof_photo and len(proof_photo) > 500):
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Input too long'), 400

        valid_statuses = ['', *list(status.value for status in ShipmentStatus)]
        if status and status not in valid_statuses:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Invalid status'), 400

        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Shipment not found'), 404

        data = {
            'address': location if ',' not in location else None,
            'lat': float(location.split(',')[0]) if ',' in location and all(x.replace('.', '').isdigit() for x in location.split(',')) else None,
            'lng': float(location.split(',')[1]) if ',' in location and all(x.replace('.', '').isdigit() for x in location.split(',')) else None,
            'label': label,
            'note': note,
            'status': status,
            'proof_photo': proof_photo
        }
        CheckpointCreate(**data)
        if data['address']:
            coords = geocode_address(data['address'])
            lat, lng = coords['lat'], coords['lng']
        else:
            lat, lng = data['lat'], data['lng']
        position = db.session.query(db.func.max(Checkpoint.position)).filter_by(shipment_id=shipment.id).scalar() or 0
        checkpoint = Checkpoint(
            shipment_id=shipment.id,
            position=position + 1,
            lat=lat,
            lng=lng,
            label=data['label'],
            note=data['note'],
            status=data['status'],
            proof_photo=data['proof_photo']
        )
        with db.session.begin():
            db.session.add(checkpoint)
            if data['status']:
                shipment.status = data['status']
                db.session.add(StatusHistory(shipment=shipment, status=data['status']))
                if shipment.status == ShipmentStatus.DELIVERED.value:
                    shipment.eta = checkpoint.timestamp
        socketio.emit('update', shipment.to_dict(), namespace='/', room=tracking)
        for subscriber in shipment.subscribers:
            if subscriber.is_active:
                if subscriber.email:
                    send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
                if subscriber.phone:
                    send_checkpoint_sms_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.phone)
        return redirect(url_for('admin'))
    except ValidationError as e:
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
    except ValueError as e:
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in checkpoint creation: {e}")
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Database error'), 500

def handle_simulation_form(request, shipments, simulation_states):
    try:
        tracking = bleach.clean(request.form.get('tracking', '').strip())
        num_points = request.form.get('num_points')
        step_hours = request.form.get('step_hours')

        if not tracking or not num_points or not step_hours:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Missing required fields'), 400

        num_points = int(num_points)
        step_hours = int(step_hours)
        if num_points < 2 or num_points > 10 or step_hours < 1 or step_hours > 24:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Number of points must be 2-10, step hours must be 1-24'), 400

        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Shipment not found'), 404
        if shipment.status == ShipmentStatus.DELIVERED.value:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Shipment already delivered'), 400
        simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
        if simulation_state and simulation_state.status == 'running':
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Simulation already running'), 400
        run_simulation_async.delay(shipment.id, num_points, step_hours)
        return redirect(url_for('admin'))
    except ValueError as e:
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in simulation creation: {e}")
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Database error'), 500

def handle_subscribe_form(request, shipments, simulation_states):
    try:
        tracking = bleach.clean(request.form.get('tracking', '').strip())
        email = bleach.clean(request.form.get('email', '').strip())
        phone = bleach.clean(request.form.get('phone', '').strip()) or None

        if not tracking or (not email and not phone):
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Tracking number and either email or phone required'), 400

        if len(tracking) > 50 or (email and len(email) > 120) or (phone and len(phone) > 20):
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Input too long'), 400

        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Shipment not found'), 404

        subscriber = Subscriber(shipment_id=shipment.id, email=email, phone=phone)
        with db.session.begin():
            db.session.add(subscriber)
        return redirect(url_for('admin'))
    except IntegrityError:
        db.session.rollback()
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Already subscribed'), 409
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in subscription: {e}")
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Database error'), 500

def handle_track_multiple_form(request, shipments, simulation_states):
    try:
        tracking_numbers = bleach.clean(request.form.get('tracking_numbers', '').strip())
        if not tracking_numbers:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='At least one tracking number required'), 400

        tracking_list = [tn.strip() for tn in tracking_numbers.split(',') if tn.strip()]
        if not tracking_list:
            return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='No valid tracking numbers provided'), 400

        tracked_shipments = []
        for tn in tracking_list:
            shipment = Shipment.query.filter_by(tracking=tn).first()
            if shipment:
                tracked_shipments.append(shipment)
        return render_template('track_multiple.html', tracked_shipments=tracked_shipments)
    except SQLAlchemyError as e:
        logger.error(f"Database error in track multiple: {e}")
        return render_template('admin.html', shipments=shipments, simulation_states=simulation_states, error='Database error'), 500

# Simulation logic
def generate_waypoints(start_lat, start_lng, end_lat, end_lng, num_points):
    waypoints = []
    for i in range(num_points + 1):
        t = i / num_points
        lat = start_lat + t * (end_lat - start_lat)
        lng = start_lng + t * (end_lng - start_lng)
        deviation = random.uniform(-0.01, 0.01)
        lat += deviation
        lng += deviation
        waypoints.append((lat, lng))
    return waypoints

@shared_task
def run_simulation_async(shipment_id, num_points, step_hours):
    try:
        shipment = Shipment.query.get(shipment_id)
        if not shipment:
            logger.error(f"Shipment {shipment_id} not found for simulation")
            return
        simulation_state = SimulationState.query.filter_by(shipment_id=shipment_id).first()
        start_position = simulation_state.current_position if simulation_state else 0
        waypoints = json.loads(simulation_state.waypoints) if simulation_state else generate_waypoints(
            shipment.origin_lat, shipment.origin_lng, shipment.dest_lat, shipment.dest_lng, num_points
        )
        current_time = simulation_state.current_time if simulation_state else datetime.utcnow()

        if not simulation_state:
            simulation_state = SimulationState(
                shipment_id=shipment.id,
                tracking=shipment.tracking,
                status='running',
                current_position=0,
                waypoints=json.dumps(waypoints),
                current_time=current_time,
                num_points=num_points,
                step_hours=step_hours
            )
            with db.session.begin():
                db.session.add(simulation_state)

        updates = []
        statuses = [ShipmentStatus.IN_TRANSIT.value, ShipmentStatus.OUT_FOR_DELIVERY.value, ShipmentStatus.DELIVERED.value]
        for i, (lat, lng) in enumerate(waypoints[start_position:], start_position + 1):
            if simulation_state.status != 'running':
                logger.info(f"Simulation paused or stopped for {shipment.tracking}")
                break
            status = statuses[min(i // (num_points // len(statuses)), len(statuses) - 1)] if i == num_points else None
            checkpoint = Checkpoint(
                shipment_id=shipment.id,
                position=i,
                lat=lat,
                lng=lng,
                label=f"Checkpoint {i}",
                note=f"Simulated checkpoint at {current_time.isoformat()}",
                status=status,
                timestamp=current_time
            )
            with db.session.begin():
                db.session.add(checkpoint)
                if status and status != shipment.status:
                    shipment.status = status
                    db.session.add(StatusHistory(shipment=shipment, status=status))
                    if shipment.status == ShipmentStatus.DELIVERED.value:
                        shipment.eta = checkpoint.timestamp
                simulation_state.current_position = i
                simulation_state.current_time = current_time
            updates.append(shipment.to_dict())
            if len(updates) >= 5:
                socketio.emit('update', updates[-1], namespace='/', room=shipment.tracking)
                updates = []
            for subscriber in shipment.subscribers:
                if subscriber.is_active:
                    if subscriber.email:
                        send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
                    if subscriber.phone:
                        send_checkpoint_sms_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.phone)
            current_time += timedelta(hours=step_hours)
            socketio.sleep(1)  # Simulate delay
        if updates:
            socketio.emit('update', updates[-1], namespace='/', room=shipment.tracking)
        if simulation_state.status == 'running':
            with db.session.begin():
                simulation_state.status = 'completed'
        logger.info(f"Simulation completed for {shipment.tracking}")
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error in simulation for {shipment_id}: {e}")
    except Exception as e:
        logger.error(f"Simulation error for {shipment_id}: {e}")

if __name__ == '__main__':
    socketio.run(app, host='0.0.0.0', port=5000, debug=True)
