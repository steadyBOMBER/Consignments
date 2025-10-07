import os
import logging
import smtplib
import random
import json
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, current_app, redirect, url_for
from flask_jwt_extended import JWTManager, jwt_required, create_access_token
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_socketio import SocketIO, emit, join_room
from flask_caching import Cache
from flask_restx import Api, Resource, fields
from flask_wtf import FlaskForm
from wtforms import StringField, SelectField, SubmitField, TextAreaField, IntegerField
from wtforms.validators import DataRequired, Length, Optional, NumberRange
from pydantic import BaseModel, validator, ValidationError
from typing import Optional as TypingOptional, Dict, List, Union
from celery import Celery, shared_task
from retry import retry
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from werkzeug.security import check_password_hash
from werkzeug.exceptions import HTTPException
import requests
from environs import Env
from math import radians, sin, cos, sqrt, atan2
from twilio.rest import Client

# Environment validation
env = Env()
env.read_env()

required_vars = [
    'DATABASE_URL', 'REDIS_URL', 'TELEGRAM_TOKEN', 'ADMIN_PASSWORD_HASH',
    'SMTP_HOST', 'SMTP_PORT', 'SMTP_USER', 'SMTP_PASS', 'SMTP_FROM', 'APP_BASE_URL'
]
for var in required_vars:
    if not env(var):
        raise ValueError(f"Missing required environment variable: {var}")

TWILIO_ACCOUNT_SID = env('TWILIO_ACCOUNT_SID', None)
TWILIO_AUTH_TOKEN = env('TWILIO_AUTH_TOKEN', None)
TWILIO_PHONE_NUMBER = env('TWILIO_PHONE_NUMBER', None)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Flask app setup
app = Flask(__name__)
app.config.from_prefixed_env()
app.config['SQLALCHEMY_DATABASE_URI'] = env('DATABASE_URL').replace('postgresql://', 'postgresql+psycopg://')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['RATELIMIT_STORAGE_URL'] = env('REDIS_URL')
app.config['ADMIN_PASSWORD_HASH'] = env('ADMIN_PASSWORD_HASH')
app.config['TELEGRAM_TOKEN'] = env('TELEGRAM_TOKEN')
app.config['SMTP_HOST'] = env('SMTP_HOST')
app.config['SMTP_PORT'] = env.int('SMTP_PORT')
app.config['SMTP_USER'] = env('SMTP_USER')
app.config['SMTP_PASS'] = env('SMTP_PASS')
app.config['SMTP_FROM'] = env('SMTP_FROM')
app.config['APP_BASE_URL'] = env('APP_BASE_URL')
app.config['SECRET_KEY'] = env.str('SECRET_KEY', default=os.urandom(24))

socketio = SocketIO(app, async_mode='threading')
jwt = JWTManager(app)
limiter = Limiter(
    app=app,
    key_func=get_remote_address,
    storage_uri="memory://",  # Changed to in-memory storage to bypass Redis issue
    storage_options={},
    default_limits=["200 per day", "50 per hour"]
)
limiter.init_app(app)
db = SQLAlchemy(app)
migrate = Migrate(app, db)
cache = Cache(app, config={'CACHE_TYPE': 'redis', 'CACHE_REDIS_URL': env('REDIS_URL')})
api = Api(app, version='1.0', title='Courier Tracking API', description='Advanced API for tracking shipments')
celery = Celery(app.name, broker=env('REDIS_URL'), backend=env('REDIS_URL'))
celery.conf.update(app.config)

# WTForms for admin
class ShipmentForm(FlaskForm):
    tracking = StringField('Tracking Number', validators=[DataRequired(), Length(min=1, max=50)])
    title = StringField('Title', validators=[DataRequired(), Length(min=1, max=100)], default='Consignment')
    origin = StringField('Origin (address or lat,lng)', validators=[DataRequired()])
    destination = StringField('Destination (address or lat,lng)', validators=[DataRequired()])
    status = SelectField('Status', choices=[
        ('Created', 'Created'),
        ('In Transit', 'In Transit'),
        ('Out for Delivery', 'Out for Delivery'),
        ('Delivered', 'Delivered')
    ], validators=[DataRequired()])
    submit = SubmitField('Create Shipment')

class CheckpointForm(FlaskForm):
    tracking = StringField('Tracking Number', validators=[DataRequired(), Length(min=1, max=50)])
    location = StringField('Location (address or lat,lng)', validators=[DataRequired()])
    label = StringField('Label', validators=[DataRequired(), Length(min=1, max=100)])
    note = TextAreaField('Note', validators=[Optional(), Length(max=500)])
    status = SelectField('Status', choices=[
        ('', 'No Change'),
        ('Created', 'Created'),
        ('In Transit', 'In Transit'),
        ('Out for Delivery', 'Out for Delivery'),
        ('Delivered', 'Delivered')
    ], validators=[Optional()])
    proof_photo = StringField('Proof Photo URL', validators=[Optional(), Length(max=500)])
    submit = SubmitField('Add Checkpoint')

class SimulationForm(FlaskForm):
    tracking = StringField('Tracking Number', validators=[DataRequired(), Length(min=1, max=50)])
    num_points = IntegerField('Number of Checkpoints', validators=[DataRequired(), NumberRange(min=2, max=10)])
    step_hours = IntegerField('Step Hours', validators=[DataRequired(), NumberRange(min=1, max=24)])
    submit = SubmitField('Start Simulation')

# Anti-Crash: Global exception handlers
@app.errorhandler(Exception)
def handle_unhandled_exception(e):
    logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
    return render_template('error.html', error='Internal server error'), 500

@app.errorhandler(HTTPException)
def handle_http_exception(e):
    logger.warning(f"HTTP exception: {e.code} - {str(e)}")
    return render_template('error.html', error=f"{e.code} - {e.name}: {e.description}"), e.code

@app.errorhandler(500)
def internal_error(error):
    logger.error(f"500 Internal Server Error: {str(error)}", exc_info=True)
    return render_template('error.html', error='Internal server error'), 500

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

# Reverse geocoding for stop naming
@cache.memoize(timeout=86400)
def reverse_geocode(lat: float, lng: float) -> str:
    try:
        response = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"lat": lat, "lon": lng, "format": "json"},
            headers={"User-Agent": "CourierTrackingApp/1.0 (contact@yourdomain.com)"},
            timeout=5
        )
        response.raise_for_status()
        data = response.json()
        return data.get('display_name', f"Location at {lat:.4f}, {lng:.4f}")
    except requests.RequestException as e:
        logger.error(f"Reverse geocoding error: {e}")
        return f"Location at {lat:.4f}, {lng:.4f}"

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

# Helper function to generate realistic waypoints
def generate_waypoints(origin_lat, origin_lng, dest_lat, dest_lng, num_points):
    waypoints = []
    total_distance = haversine(origin_lat, origin_lng, dest_lat, dest_lng)
    num_stops = min(max(1, int(total_distance // 200)), 3)
    if num_points < num_stops + 2:
        num_points = num_stops + 2
    for i in range(num_points):
        fraction = i / (num_points - 1)
        lat = origin_lat + (dest_lat - origin_lat) * fraction + random.uniform(-0.05, 0.05)
        lng = origin_lng + (dest_lng - origin_lng) * fraction + random.uniform(-0.05, 0.05)
        lat = max(min(lat, 90), -90)
        lng = max(min(lng, 180), -180)
        waypoints.append((lat, lng))
    return waypoints

# Helper function to calculate variable speed
def calculate_speed(distance_km, is_urban=False):
    if distance_km > 100:
        base_speed = random.uniform(60, 100)
    else:
        base_speed = random.uniform(30, 60)
    if is_urban:
        base_speed *= random.uniform(0.7, 0.9)
    return base_speed

# Async email task
@shared_task(bind=True, max_retries=3, retry_backoff=2, retry_jitter=True)
def send_checkpoint_email_async(self, shipment: dict, checkpoint: dict, email: str):
    try:
        utc_time = datetime.fromisoformat(checkpoint['timestamp'].replace('Z', '+00:00'))
        wat_time = utc_time + timedelta(hours=1)
        wat_time_str = wat_time.strftime("%Y-%m-%d %I:%M:%S %p WAT")
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Update: Shipment {shipment['tracking']} - {checkpoint['label']}"
        msg['From'] = current_app.config['SMTP_FROM']
        msg['To'] = email
        text = f"""Courier Tracking Update

Shipment: {shipment['title']} ({shipment['tracking']})
Status: {shipment['status']}
Updated: {wat_time_str}

Checkpoint:
- Label: {checkpoint['label']}
- Location: ({checkpoint['lat']:.4f}, {checkpoint['lng']:.4f})
- Note: {checkpoint['note'] or 'None'}

Track: {current_app.config['APP_BASE_URL']}/track/{shipment['tracking']}
Unsubscribe: {current_app.config['APP_BASE_URL']}/unsubscribe/{shipment['tracking']}?email={email}
"""
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Shipment Update</title>
    <link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet">
</head>
<body class="bg-gray-100 font-sans">
    <div class="max-w-2xl mx-auto bg-white shadow-md rounded-lg overflow-hidden">
        <div class="bg-blue-600 text-white text-center py-4">
            <h1 class="text-2xl font-bold mt-2">Shipment Update</h1>
        </div>
        <div class="p-6">
            <h2 class="text-xl font-semibold text-gray-800">Shipment: {shipment['title']} ({shipment['tracking']})</h2>
            <div class="mt-4 space-y-2">
                <p><span class="font-medium text-gray-700">Status:</span> {shipment['status']}</p>
                <p><span class="font-medium text-gray-700">Updated:</span> {wat_time_str}</p>
            </div>
            <h3 class="text-lg font-semibold text-gray-800 mt-6">Latest Checkpoint</h3>
            <div class="mt-2 space-y-2">
                <p><span class="font-medium text-gray-700">Label:</span> {checkpoint['label']}</p>
                <p><span class="font-medium text-gray-700">Location:</span> ({checkpoint['lat']:.4f}, {checkpoint['lng']:.4f})</p>
                <p><span class="font-medium text-gray-700">Note:</span> {checkpoint['note'] or 'None'}</p>
            </div>
            <div class="mt-6 text-center">
                <a href="{current_app.config['APP_BASE_URL']}/track/{shipment['tracking']}" class="inline-block bg-blue-600 text-white font-semibold py-2 px-4 rounded hover:bg-blue-700">
                    Track Shipment
                </a>
            </div>
        </div>
        <div class="bg-gray-200 text-gray-600 text-center py-4 text-sm">
            <p>You're receiving this email because you're subscribed to updates for this shipment.</p>
            <p><a href="{current_app.config['APP_BASE_URL']}/unsubscribe/{shipment['tracking']}?email={email}" class="text-blue-600 hover:underline">Unsubscribe</a></p>
            <p>&copy; {datetime.now().year} Courier Tracking Service. All rights reserved.</p>
        </div>
    </div>
</body>
</html>
"""
        msg.attach(MIMEText(text, 'plain'))
        msg.attach(MIMEText(html, 'html'))
        @retry(tries=3, delay=2, backoff=2, exceptions=(smtplib.SMTPException, smtplib.SMTPServerDisconnected))
        def send_email():
            with smtplib.SMTP(current_app.config['SMTP_HOST'], current_app.config['SMTP_PORT'], timeout=10) as server:
                server.starttls()
                server.login(current_app.config['SMTP_USER'], current_app.config['SMTP_PASS'])
                server.send_message(msg)
                logger.info(f"Sent email to {email} for checkpoint {checkpoint['id']} of shipment {shipment['tracking']}")
        send_email()
    except smtplib.SMTPAuthenticationError:
        logger.error(f"Authentication error sending email to {email} for checkpoint {checkpoint['id']}: SMTP credentials invalid")
        raise self.retry(exc=Exception("SMTP authentication failed"))
    except smtplib.SMTPConnectError:
        logger.error(f"Connection error sending email to {email} for checkpoint {checkpoint['id']}: SMTP server unreachable")
        raise self.retry(exc=Exception("SMTP server unreachable"))
    except smtplib.SMTPException as e:
        logger.error(f"SMTP error sending email to {email} for checkpoint {checkpoint['id']}: {e}")
        raise self.retry(exc=e)
    except Exception as e:
        logger.error(f"Unexpected error sending email to {email} for checkpoint {checkpoint['id']}: {e}")
        raise

# Celery task for SMS
@celery.task
def send_checkpoint_sms_async(shipment_dict, checkpoint_dict, phone):
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
    except Exception as e:
        logger.error(f"Failed to send SMS to {phone}: {e}")

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
        if not v.strip():
            raise ValueError('Label cannot be empty')
        return v
    @validator('address', always=True)
    def check_coordinates_or_address(cls, v, values):
        if not v and (values.get('lat') is None or values.get('lng') is None):
            raise ValueError('Either address or lat/lng must be provided')
        if v and (values.get('lat') is not None or values.get('lng') is not None):
            raise ValueError('Provide either address or lat/lng, not both')
        return v

class ShipmentCreate(BaseModel):
    tracking_number: str
    title: str = "Consignment"
    origin: Union[Dict[str, float], str]
    destination: Union[Dict[str, float], str]
    status: str = "Created"
    @validator('tracking_number')
    def check_tracking_number(cls, v):
        if not v.strip():
            raise ValueError('Tracking number cannot be empty')
        return v
    @validator('origin', 'destination')
    def check_coordinates_or_address(cls, v):
        if isinstance(v, dict):
            if 'lat' not in v or 'lng' not in v:
                raise ValueError('Coordinates must be a dict with lat and lng')
            Coordinate(**v)
        elif not isinstance(v, str) or not v.strip():
            raise ValueError('Address must be a non-empty string')
        return v

# SQLAlchemy models
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
    email = db.Column(db.String(120), nullable=False)
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

# Flask-RESTx namespace
ns = api.namespace('shipments', description='Advanced Shipment operations')

shipment_model = api.model('Shipment', {
    'tracking': fields.String(required=True),
    'title': fields.String,
    'origin': fields.Raw(required=True, description='Coordinates {lat, lng} or address string'),
    'destination': fields.Raw(required=True, description='Coordinates {lat, lng} or address string'),
    'status': fields.String
})

checkpoint_model = api.model('Checkpoint', {
    'lat': fields.Float,
    'lng': fields.Float,
    'address': fields.String(description='Address string, alternative to lat/lng'),
    'label': fields.String(required=True),
    'note': fields.String,
    'status': fields.String,
    'proof_photo': fields.String
})

# Admin dashboard route with WTForms
@app.route('/admin', methods=['GET', 'POST'])
@jwt_required()
def admin():
    try:
        shipment_form = ShipmentForm(prefix='shipment')
        checkpoint_form = CheckpointForm(prefix='checkpoint')
        simulation_form = SimulationForm(prefix='simulation')
        shipments = Shipment.query.all()
        simulation_states = SimulationState.query.all()

        if shipment_form.submit.data and shipment_form.validate_on_submit():
            data = {
                'tracking_number': shipment_form.tracking.data,
                'title': shipment_form.title.data,
                'origin': shipment_form.origin.data,
                'destination': shipment_form.destination.data,
                'status': shipment_form.status.data
            }
            try:
                ShipmentCreate(**data)
                origin = data['origin']
                destination = data['destination']
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
                    tracking=data['tracking_number'],
                    title=data['title'],
                    origin_lat=origin_lat,
                    origin_lng=origin_lng,
                    dest_lat=dest_lat,
                    dest_lng=dest_lng,
                    origin_address=origin_address,
                    dest_address=dest_address,
                    status=data['status']
                )
                shipment.calculate_distance_and_eta()
                db.session.add(shipment)
                status_history = StatusHistory(shipment=shipment, status=data['status'])
                db.session.add(status_history)
                db.session.commit()
                socketio.emit('update', shipment.to_dict(), namespace='/', room=shipment.tracking)
                return redirect(url_for('admin'))
            except ValidationError as e:
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
            except ValueError as e:
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
            except IntegrityError:
                db.session.rollback()
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Tracking number already exists'), 409
            except SQLAlchemyError as e:
                db.session.rollback()
                logger.error(f"Database error: {e}")
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Database error'), 500
            except Exception as e:
                logger.error(f"Shipment creation error: {e}")
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Failed to create shipment'), 500

        if checkpoint_form.submit.data and checkpoint_form.validate_on_submit():
            tracking = checkpoint_form.tracking.data
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Shipment not found'), 404
            data = {
                'address': checkpoint_form.location.data if ',' not in checkpoint_form.location.data else None,
                'lat': float(checkpoint_form.location.data.split(',')[0]) if ',' in checkpoint_form.location.data and all(x.replace('.', '').isdigit() for x in checkpoint_form.location.data.split(',')) else None,
                'lng': float(checkpoint_form.location.data.split(',')[1]) if ',' in checkpoint_form.location.data and all(x.replace('.', '').isdigit() for x in checkpoint_form.location.data.split(',')) else None,
                'label': checkpoint_form.label.data,
                'note': checkpoint_form.note.data or None,
                'status': checkpoint_form.status.data or None,
                'proof_photo': checkpoint_form.proof_photo.data or None
            }
            try:
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
                db.session.add(checkpoint)
                if data['status']:
                    shipment.status = data['status']
                    status_history = StatusHistory(shipment=shipment, status=data['status'])
                    db.session.add(status_history)
                    if shipment.status == 'Delivered':
                        shipment.eta = checkpoint.timestamp
                db.session.commit()
                socketio.emit('update', shipment.to_dict(), namespace='/', room=tracking)
                for subscriber in shipment.subscribers:
                    if subscriber.is_active:
                        send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
                        if subscriber.phone:
                            send_checkpoint_sms_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.phone)
                return redirect(url_for('admin'))
            except ValidationError as e:
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
            except ValueError as e:
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
            except SQLAlchemyError as e:
                db.session.rollback()
                logger.error(f"Database error: {e}")
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Database error'), 500
            except Exception as e:
                logger.error(f"Checkpoint creation error for {tracking}: {e}")
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Failed to add checkpoint'), 500

        if simulation_form.submit.data and simulation_form.validate_on_submit():
            tracking = simulation_form.tracking.data
            num_points = simulation_form.num_points.data
            step_hours = simulation_form.step_hours.data
            try:
                shipment = Shipment.query.filter_by(tracking=tracking).first()
                if not shipment:
                    return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Shipment not found'), 404
                if shipment.status == 'Delivered':
                    return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Shipment already delivered'), 400
                simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
                if simulation_state and simulation_state.status == 'running':
                    return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Simulation already running'), 400
                run_simulation(shipment, num_points, step_hours, simulation_state)
                return redirect(url_for('admin'))
            except ValueError as e:
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error=str(e)), 400
            except SQLAlchemyError as e:
                db.session.rollback()
                logger.error(f"Simulation error: {e}")
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Database error'), 500
            except Exception as e:
                logger.error(f"Simulation error for {tracking}: {e}")
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states, error='Failed to start simulation'), 500

        return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=shipments, simulation_states=simulation_states)
    except Exception as e:
        logger.error(f"Admin dashboard error: {e}")
        return render_template('error.html', error='Failed to load admin dashboard'), 500

# Simulation logic
def run_simulation(shipment, num_points, step_hours, simulation_state=None):
    try:
        position = db.session.query(db.func.max(Checkpoint.position)).filter_by(shipment_id=shipment.id).scalar() or 0
        status_sequence = ['Created', 'At Warehouse', 'In Transit', 'At Sorting Facility', 'Out for Delivery', 'Delivered']
        max_status_index = len(status_sequence) - 1

        if simulation_state and simulation_state.status == 'paused':
            waypoints = json.loads(simulation_state.waypoints)
            current_time = simulation_state.current_time
            start_position = simulation_state.current_position
            num_points = simulation_state.num_points
            step_hours = simulation_state.step_hours
        else:
            if num_points < 2 or step_hours < 1:
                raise ValueError("Number of points must be at least 2, step hours must be at least 1")
            waypoints = generate_waypoints(shipment.origin_lat, shipment.origin_lng, shipment.dest_lat, shipment.dest_lng, num_points)
            current_time = datetime.utcnow()
            start_position = 0
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
            db.session.add(simulation_state)
            db.session.commit()

        total_distance = haversine(shipment.origin_lat, shipment.origin_lng, shipment.dest_lat, shipment.dest_lng)
        for i, (lat, lng) in enumerate(waypoints[start_position:], start_position + 1):
            prev_lat = shipment.origin_lat if i == 1 else waypoints[i-2][0]
            prev_lng = shipment.origin_lng if i == 1 else waypoints[i-2][1]
            segment_distance = haversine(prev_lat, prev_lng, lat, lng)
            is_urban = i == 1 or i == num_points or segment_distance < 50
            speed = calculate_speed(segment_distance, is_urban)
            travel_time = calculate_eta(segment_distance, speed)
            delay = timedelta(minutes=random.uniform(0, 120))
            current_time += travel_time + delay
            status_index = min(i * max_status_index // num_points, max_status_index)
            status = status_sequence[status_index] if i < num_points else 'Delivered'
            location_name = reverse_geocode(lat, lng)
            label = f"{status} at {location_name.split(',')[0]}"
            note = f"Processed at {location_name}" if status in ['At Warehouse', 'At Sorting Facility'] else f"En route to {shipment.dest_address or 'destination'}"
            checkpoint = Checkpoint(
                shipment_id=shipment.id,
                position=position + i,
                lat=lat,
                lng=lng,
                label=label,
                note=note,
                status=status,
                timestamp=current_time
            )
            db.session.add(checkpoint)
            if status and status != shipment.status:
                shipment.status = status
                status_history = StatusHistory(shipment=shipment, status=status)
                db.session.add(status_history)
                if status == 'Delivered':
                    shipment.eta = current_time
            simulation_state.current_position = i
            simulation_state.current_time = current_time
            db.session.commit()
            socketio.emit('update', shipment.to_dict(), namespace='/', room=shipment.tracking)
            for subscriber in shipment.subscribers:
                if subscriber.is_active:
                    send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
                    if subscriber.phone:
                        send_checkpoint_sms_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.phone)
            db.session.refresh(simulation_state)
            if simulation_state.status == 'paused':
                logger.info(f"Simulation paused for {shipment.tracking} at position {i}")
                break
        else:
            SimulationState.query.filter_by(shipment_id=shipment.id).delete()
            db.session.commit()
            logger.info(f"Simulation completed for {shipment.tracking}")
    except Exception as e:
        db.session.rollback()
        logger.error(f"Simulation error for {shipment.tracking}: {e}")
        raise

# Pause simulation route
@app.route('/pause_simulation/<tracking>', methods=['POST'])
@jwt_required()
def pause_simulation(tracking):
    try:
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipment_form=ShipmentForm(prefix='shipment'), checkpoint_form=CheckpointForm(prefix='checkpoint'), simulation_form=SimulationForm(prefix='simulation'), shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
        simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
        if not simulation_state or simulation_state.status != 'running':
            return render_template('admin.html', shipment_form=ShipmentForm(prefix='shipment'), checkpoint_form=CheckpointForm(prefix='checkpoint'), simulation_form=SimulationForm(prefix='simulation'), shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='No active simulation to pause'), 400
        simulation_state.status = 'paused'
        db.session.commit()
        logger.info(f"Simulation paused for {tracking}")
        return redirect(url_for('admin'))
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Pause simulation error: {e}")
        return render_template('admin.html', shipment_form=ShipmentForm(prefix='shipment'), checkpoint_form=CheckpointForm(prefix='checkpoint'), simulation_form=SimulationForm(prefix='simulation'), shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Database error'), 500
    except Exception as e:
        logger.error(f"Pause simulation error for {tracking}: {e}")
        return render_template('admin.html', shipment_form=ShipmentForm(prefix='shipment'), checkpoint_form=CheckpointForm(prefix='checkpoint'), simulation_form=SimulationForm(prefix='simulation'), shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to pause simulation'), 500

# Continue simulation route
@app.route('/continue_simulation/<tracking>', methods=['POST'])
@jwt_required()
def continue_simulation(tracking):
    try:
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('admin.html', shipment_form=ShipmentForm(prefix='shipment'), checkpoint_form=CheckpointForm(prefix='checkpoint'), simulation_form=SimulationForm(prefix='simulation'), shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
        simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
        if not simulation_state or simulation_state.status != 'paused':
            return render_template('admin.html', shipment_form=ShipmentForm(prefix='shipment'), checkpoint_form=CheckpointForm(prefix='checkpoint'), simulation_form=SimulationForm(prefix='simulation'), shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='No paused simulation to continue'), 400
        simulation_state.status = 'running'
        db.session.commit()
        run_simulation(shipment, simulation_state.num_points, simulation_state.step_hours, simulation_state)
        return redirect(url_for('admin'))
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Continue simulation error: {e}")
        return render_template('admin.html', shipment_form=ShipmentForm(prefix='shipment'), checkpoint_form=CheckpointForm(prefix='checkpoint'), simulation_form=SimulationForm(prefix='simulation'), shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Database error'), 500
    except Exception as e:
        logger.error(f"Continue simulation error for {tracking}: {e}")
        return render_template('admin.html', shipment_form=ShipmentForm(prefix='shipment'), checkpoint_form=CheckpointForm(prefix='checkpoint'), simulation_form=SimulationForm(prefix='simulation'), shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to continue simulation'), 500

# Keep-Alive / Ping endpoint
@app.route('/ping', methods=['GET'])
@limiter.limit("1000 per day")
def ping():
    try:
        db.session.execute('SELECT 1')
        return jsonify({'status': 'alive', 'timestamp': datetime.utcnow().isoformat() + 'Z'}), 200
    except Exception as e:
        logger.error(f"Ping failed: {e}")
        return jsonify({'status': 'unhealthy'}), 500

# Health endpoint
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

# Routes
@app.route('/login', methods=['GET', 'POST'])
def login():
    try:
        if request.method == 'POST':
            data = request.form
            if data.get('password') and check_password_hash(app.config['ADMIN_PASSWORD_HASH'], data['password']):
                access_token = create_access_token(identity='admin')
                response = redirect(url_for('admin'))
                response.set_cookie('access_token', access_token, httponly=True)
                return response
            return render_template('error.html', error='Invalid password'), 401
        return '''
        <form method="POST">
            <label>Password: <input type="password" name="password"></label>
            <button type="submit">Login</button>
        </form>
        '''
    except Exception as e:
        logger.error(f"Login error: {e}")
        return render_template('error.html', error='Login failed'), 400

@app.route('/track', methods=['GET'])
def track_redirect():
    try:
        tracking = request.args.get('tracking')
        if tracking:
            return redirect(url_for('track', tracking=tracking))
        return render_template('error.html', error='Tracking number required'), 400
    except Exception as e:
        logger.error(f"Track redirect error: {e}")
        return render_template('error.html', error='Failed to redirect to tracking'), 500

@app.route('/track/<tracking>')
@cache.cached(timeout=300, query_string=True)
def track(tracking):
    try:
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 10, type=int)
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('error.html', error='Shipment not found'), 404
        pagination = Checkpoint.query.filter_by(shipment_id=shipment.id).order_by(Checkpoint.position).paginate(page=page, per_page=per_page, error_out=False)
        checkpoints = [cp.to_dict() for cp in pagination.items]
        history = [h.to_dict() for h in shipment.history]
        return render_template(
            'track.html',
            shipment=shipment,
            checkpoints=checkpoints,
            history=history,
            pagination=pagination,
            origin_lat=shipment.origin_lat,
            origin_lng=shipment.origin_lng,
            dest_lat=shipment.dest_lat,
            dest_lng=shipment.dest_lng
        )
    except Exception as e:
        logger.error(f"Track error for {tracking}: {e}")
        return render_template('error.html', error='Failed to retrieve tracking data'), 500

@socketio.on('connect', namespace='/')
def handle_connect():
    try:
        tracking = request.args.get('tracking')
        if tracking:
            join_room(tracking)
            logger.info(f'Client connected to WebSocket for {tracking}')
            emit('status', {'message': 'Connected'})
    except Exception as e:
        logger.error(f"WebSocket connect error: {e}")

@socketio.on('subscribe')
def handle_subscribe(tracking):
    try:
        join_room(tracking)
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if shipment:
            emit('update', shipment.to_dict(), room=tracking)
    except Exception as e:
        logger.error(f"WebSocket subscribe error for {tracking}: {e}")
        emit('error', {'message': 'Failed to subscribe'})

@ns.route('/')
class ShipmentList(Resource):
    @jwt_required()
    @api.marshal_list_with(shipment_model)
    def get(self):
        try:
            shipments = Shipment.query.all()
            return [s.to_dict() for s in shipments]
        except Exception as e:
            logger.error(f"Shipment list error: {e}")
            return {'error': 'Failed to retrieve shipments'}, 500

    @jwt_required()
    @api.expect(shipment_model)
    def post(self):
        try:
            data = ShipmentCreate(**request.get_json() or request.form).dict()
            origin = data['origin']
            destination = data['destination']
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
                tracking=data['tracking_number'],
                title=data['title'],
                origin_lat=origin_lat,
                origin_lng=origin_lng,
                dest_lat=dest_lat,
                dest_lng=dest_lng,
                origin_address=origin_address,
                dest_address=dest_address,
                status=data['status']
            )
            shipment.calculate_distance_and_eta()
            db.session.add(shipment)
            status_history = StatusHistory(shipment=shipment, status=data['status'])
            db.session.add(status_history)
            db.session.commit()
            socketio.emit('update', shipment.to_dict(), namespace='/', room=shipment.tracking)
            if request.form:
                return redirect(url_for('admin'))
            return shipment.to_dict(), 201
        except ValidationError as e:
            if request.form:
                shipment_form = ShipmentForm(prefix='shipment', formdata=request.form)
                checkpoint_form = CheckpointForm(prefix='checkpoint')
                simulation_form = SimulationForm(prefix='simulation')
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=str(e)), 400
            return {'error': str(e)}, 400
        except ValueError as e:
            if request.form:
                shipment_form = ShipmentForm(prefix='shipment', formdata=request.form)
                checkpoint_form = CheckpointForm(prefix='checkpoint')
                simulation_form = SimulationForm(prefix='simulation')
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=str(e)), 400
            return {'error': str(e)}, 400
        except IntegrityError:
            db.session.rollback()
            if request.form:
                shipment_form = ShipmentForm(prefix='shipment', formdata=request.form)
                checkpoint_form = CheckpointForm(prefix='checkpoint')
                simulation_form = SimulationForm(prefix='simulation')
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Tracking number already exists'), 409
            return {'error': 'Tracking number already exists'}, 409
        except SQLAlchemyError as e:
            db.session.rollback()
            logger.error(f"Database error: {e}")
            if request.form:
                shipment_form = ShipmentForm(prefix='shipment', formdata=request.form)
                checkpoint_form = CheckpointForm(prefix='checkpoint')
                simulation_form = SimulationForm(prefix='simulation')
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Database error'), 500
            return {'error': 'Database error'}, 500
        except Exception as e:
            logger.error(f"Shipment creation error: {e}")
            if request.form:
                shipment_form = ShipmentForm(prefix='shipment', formdata=request.form)
                checkpoint_form = CheckpointForm(prefix='checkpoint')
                simulation_form = SimulationForm(prefix='simulation')
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to create shipment'), 500
            return {'error': 'Failed to create shipment'}, 500

@ns.route('/<tracking>/checkpoints')
class CheckpointList(Resource):
    @jwt_required()
    @api.expect(checkpoint_model)
    def post(self, tracking):
        try:
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                if request.form:
                    shipment_form = ShipmentForm(prefix='shipment')
                    checkpoint_form = CheckpointForm(prefix='checkpoint', formdata=request.form)
                    simulation_form = SimulationForm(prefix='simulation')
                    return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Shipment not found'), 404
                return {'error': 'Shipment not found'}, 404
            data = CheckpointCreate(**request.get_json() or request.form).dict()
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
                proof_photo=data.get('proof_photo')
            )
            db.session.add(checkpoint)
            if data['status']:
                shipment.status = data['status']
                status_history = StatusHistory(shipment=shipment, status=data['status'])
                db.session.add(status_history)
                if shipment.status == 'Delivered':
                    shipment.eta = checkpoint.timestamp
            db.session.commit()
            socketio.emit('update', shipment.to_dict(), namespace='/', room=tracking)
            for subscriber in shipment.subscribers:
                if subscriber.is_active:
                    send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
                    if subscriber.phone:
                        send_checkpoint_sms_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.phone)
            if request.form:
                return redirect(url_for('admin'))
            return checkpoint.to_dict(), 201
        except ValidationError as e:
            if request.form:
                shipment_form = ShipmentForm(prefix='shipment')
                checkpoint_form = CheckpointForm(prefix='checkpoint', formdata=request.form)
                simulation_form = SimulationForm(prefix='simulation')
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=str(e)), 400
            return {'error': str(e)}, 400
        except ValueError as e:
            if request.form:
                shipment_form = ShipmentForm(prefix='shipment')
                checkpoint_form = CheckpointForm(prefix='checkpoint', formdata=request.form)
                simulation_form = SimulationForm(prefix='simulation')
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error=str(e)), 400
            return {'error': str(e)}, 400
        except SQLAlchemyError as e:
            db.session.rollback()
            logger.error(f"Database error: {e}")
            if request.form:
                shipment_form = ShipmentForm(prefix='shipment')
                checkpoint_form = CheckpointForm(prefix='checkpoint', formdata=request.form)
                simulation_form = SimulationForm(prefix='simulation')
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Database error'), 500
            return {'error': 'Database error'}, 500
        except Exception as e:
            logger.error(f"Checkpoint creation error for {tracking}: {e}")
            if request.form:
                shipment_form = ShipmentForm(prefix='shipment')
                checkpoint_form = CheckpointForm(prefix='checkpoint', formdata=request.form)
                simulation_form = SimulationForm(prefix='simulation')
                return render_template('admin.html', shipment_form=shipment_form, checkpoint_form=checkpoint_form, simulation_form=simulation_form, shipments=Shipment.query.all(), simulation_states=SimulationState.query.all(), error='Failed to add checkpoint'), 500
            return {'error': 'Failed to add checkpoint'}, 500

@ns.route('/<tracking>/subscribe')
class Subscribe(Resource):
    def post(self, tracking):
        try:
            data = request.get_json()
            email = data.get('email')
            phone = data.get('phone')
            if not email and not phone:
                return {'error': 'Email or phone is required'}, 400
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                return {'error': 'Shipment not found'}, 404
            subscriber = Subscriber(shipment_id=shipment.id, email=email, phone=phone)
            db.session.add(subscriber)
            db.session.commit()
            return {'message': 'Subscribed successfully'}, 201
        except IntegrityError:
            db.session.rollback()
            return {'error': 'Already subscribed'}, 409
        except SQLAlchemyError as e:
            db.session.rollback()
            logger.error(f"Database error: {e}")
            return {'error': 'Database error'}, 500
        except Exception as e:
            logger.error(f"Subscription error for {tracking}: {e}")
            return {'error': 'Failed to subscribe'}, 500

@ns.route('/track_multiple')
class TrackMultiple(Resource):
    @api.expect(fields.List(fields.String))
    def post(self):
        try:
            tracking_numbers = request.get_json()
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

# Telegram webhook
@app.route('/telegram/webhook/<token>', methods=['POST'])
def telegram_webhook(token):
    if token != app.config['TELEGRAM_TOKEN']:
        return jsonify({'error': 'Invalid token'}), 403
    update = request.get_json()
    message = update.get('message', {})
    chat_id = message.get('chat', {}).get('id')
    text = message.get('text', '')
    if not chat_id or not text:
        return jsonify({'error': 'Invalid message'}), 400

    def send_message(text, reply_markup=None):
        try:
            payload = {'chat_id': chat_id, 'text': text}
            if reply_markup:
                payload['reply_markup'] = reply_markup
            requests.post(
                f"https://api.telegram.org/bot{app.config['TELEGRAM_TOKEN']}/sendMessage",
                json=payload,
                timeout=5
            )
        except requests.RequestException as e:
            logger.error(f"Telegram send message error: {e}")

    def get_navigation_keyboard(tracking=None):
        buttons = [
            [{'text': 'Create Shipment', 'callback_data': '/create'}],
            [{'text': 'Subscribe', 'callback_data': '/subscribe'}],
            [{'text': 'Add Checkpoint', 'callback_data': '/addcp'}],
            [{'text': 'Simulate', 'callback_data': '/simulate'}],
            [{'text': 'Track Multiple', 'callback_data': '/track_multiple'}]
        ]
        if tracking:
            buttons.append([{'text': f'Track {tracking}', 'url': f'{app.config["APP_BASE_URL"]}/track/{tracking}'}])
        return {'inline_keyboard': buttons}

    try:
        command, *args = text.split(' ', 1)
        args = args[0].split('|') if args else []

        if command == '/create' and len(args) == 4:
            tracking, title, origin, destination = args
            try:
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
            except ValueError as e:
                send_message(f"Invalid coordinates or address: {str(e)}", reply_markup=get_navigation_keyboard())
                return jsonify({'error': 'Invalid coordinates or address'}), 400
            shipment_data = {
                'tracking_number': tracking,
                'title': title,
                'origin': {'lat': origin_lat, 'lng': origin_lng},
                'destination': {'lat': dest_lat, 'lng': dest_lng},
                'status': 'Created'
            }
            ShipmentCreate(**shipment_data)
            shipment = Shipment(
                tracking=tracking,
                title=title,
                origin_lat=origin_lat,
                origin_lng=origin_lng,
                dest_lat=dest_lat,
                dest_lng=dest_lng,
                origin_address=origin_address,
                dest_address=dest_address,
                status='Created'
            )
            shipment.calculate_distance_and_eta()
            db.session.add(shipment)
            status_history = StatusHistory(shipment=shipment, status='Created')
            db.session.add(status_history)
            db.session.commit()
            socketio.emit('update', shipment.to_dict(), namespace='/', room=tracking)
            send_message(f"Shipment {tracking} created. Distance: {shipment.distance_km:.2f}km, ETA: {shipment.eta}", reply_markup=get_navigation_keyboard(tracking))
            return jsonify({'message': 'OK'})

        elif command == '/subscribe' and len(args) >= 2:
            tracking, contact = args[0].split(':', 1)
            contact_type, value = 'email' if '@' in contact else 'phone', contact
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                send_message("Shipment not found", reply_markup=get_navigation_keyboard())
                return jsonify({'error': 'Shipment not found'}), 404
            subscriber = Subscriber.query.filter_by(shipment_id=shipment.id, **{contact_type: value}).first()
            if subscriber:
                send_message(f"Already subscribed {value} to {tracking}", reply_markup=get_navigation_keyboard(tracking))
                return jsonify({'message': 'Already subscribed'})
            subscriber = Subscriber(shipment_id=shipment.id, **{contact_type: value})
            db.session.add(subscriber)
            db.session.commit()
            send_message(f"Subscribed {value} to {tracking}", reply_markup=get_navigation_keyboard(tracking))
            return jsonify({'message': 'OK'})

        elif command == '/addcp' and len(args) >= 3:
            tracking, location, label, *note = args
            note = '|'.join(note) if note else None
            try:
                if ',' in location and all(x.replace('.', '').isdigit() for x in location.split(',')):
                    lat, lng = map(float, location.split(','))
                else:
                    coords = geocode_address(location)
                    lat, lng = coords['lat'], coords['lng']
            except ValueError as e:
                send_message(f"Invalid coordinates or address: {str(e)}", reply_markup=get_navigation_keyboard())
                return jsonify({'error': 'Invalid coordinates or address'}), 400
            checkpoint_data = {'lat': lat, 'lng': lng, 'label': label, 'note': note}
            CheckpointCreate(**checkpoint_data)
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                send_message("Shipment not found", reply_markup=get_navigation_keyboard())
                return jsonify({'error': 'Shipment not found'}), 404
            position = db.session.query(db.func.max(Checkpoint.position)).filter_by(shipment_id=shipment.id).scalar() or 0
            checkpoint = Checkpoint(shipment_id=shipment.id, position=position + 1, **checkpoint_data)
            db.session.add(checkpoint)
            if checkpoint_data['status']:
                shipment.status = checkpoint_data['status']
                status_history = StatusHistory(shipment=shipment, status=checkpoint_data['status'])
                db.session.add(status_history)
            db.session.commit()
            socketio.emit('update', shipment.to_dict(), namespace='/', room=tracking)
            for subscriber in shipment.subscribers:
                if subscriber.is_active:
                    send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
                    if subscriber.phone:
                        send_checkpoint_sms_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.phone)
            send_message(f"Checkpoint added to {tracking}", reply_markup=get_navigation_keyboard(tracking))
            return jsonify({'message': 'OK'})

        elif command == '/simulate' and len(args) == 3:
            tracking, num_points, step_hours = args
            try:
                num_points, step_hours = int(num_points), int(step_hours)
                if num_points < 2 or step_hours < 1:
                    raise ValueError("Number of points must be at least 2, step hours must be at least 1")
            except ValueError as e:
                send_message(f"Invalid numbers for simulation: {str(e)}", reply_markup=get_navigation_keyboard())
                return jsonify({'error': 'Invalid numbers'}), 400
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                send_message("Shipment not found", reply_markup=get_navigation_keyboard())
                return jsonify({'error': 'Shipment not found'}), 404
            if shipment.status == 'Delivered':
                send_message("Shipment already delivered", reply_markup=get_navigation_keyboard())
                return jsonify({'error': 'Shipment already delivered'}), 400
            simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
            if simulation_state and simulation_state.status == 'running':
                send_message("Simulation already running", reply_markup=get_navigation_keyboard())
                return jsonify({'error': 'Simulation already running'}), 400
            run_simulation(shipment, num_points, step_hours, simulation_state)
            send_message(f"Simulated {num_points} checkpoints for {tracking}. Final status: {shipment.status}", reply_markup=get_navigation_keyboard(tracking))
            return jsonify({'message': 'OK'})

        elif command == '/track_multiple' and args:
            trackings = args[0].split(',')
            shipments_info = []
            for t in trackings:
                shipment = Shipment.query.filter_by(tracking=t.strip()).first()
                if shipment:
                    shipments_info.append(f"{t}: {shipment.status} (ETA: {shipment.eta})")
            send_message("Tracking statuses:\n" + "\n".join(shipments_info), reply_markup=get_navigation_keyboard())
            return jsonify({'message': 'OK'})

        else:
            send_message("Use /start for menu or buttons below.", reply_markup=get_navigation_keyboard())
            return jsonify({'error': 'Invalid command'}), 400

    except ValidationError as e:
        send_message(f"Validation error: {str(e)}", reply_markup=get_navigation_keyboard())
        return jsonify({'error': str(e)}), 400
    except ValueError as e:
        send_message(f"Value error: {str(e)}", reply_markup=get_navigation_keyboard())
        return jsonify({'error': str(e)}), 400
    except IntegrityError as e:
        db.session.rollback()
        send_message("Duplicate entry.", reply_markup=get_navigation_keyboard())
        return jsonify({'error': 'Duplicate'}), 409
    except SQLAlchemyError as e:
        db.session.rollback()
        logger.error(f"Database error: {e}")
        send_message("Database error.", reply_markup=get_navigation_keyboard())
        return jsonify({'error': 'Database error'}), 500
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        send_message("Unexpected error.", reply_markup=get_navigation_keyboard())
        return jsonify({'error': 'Unexpected error'}), 500

@app.route('/unsubscribe/<tracking>')
def unsubscribe(tracking):
    try:
        email = request.args.get('email')
        if not email:
            return render_template('error.html', error='Email is required'), 400
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return render_template('error.html', error='Shipment not found'), 404
        subscriber = Subscriber.query.filter_by(shipment_id=shipment.id, email=email).first()
        if not subscriber:
            return render_template('error.html', error='Subscriber not found'), 404
        subscriber.is_active = False
        db.session.commit()
        return jsonify({'message': 'Unsubscribed successfully'})
    except Exception as e:
        logger.error(f"Unsubscribe error: {e}")
        return render_template('error.html', error='Failed to unsubscribe'), 500

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    socketio.run(app, debug=False, host='0.0.0.0', port=5000)
