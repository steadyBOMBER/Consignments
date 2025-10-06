import os
import logging
from datetime import datetime, timedelta
from flask import Flask, request, jsonify, render_template, current_app
from flask_jwt_extended import JWTManager, jwt_required, create_access_token
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_sqlalchemy import SQLAlchemy
from flask_socketio import SocketIO, emit
from flask_caching import Cache
from flask_restx import Api, Resource, fields
from pydantic import BaseModel, validator
from typing import Optional, Dict
from celery import Celery
from email_utils import send_checkpoint_email_async
from sqlalchemy.exc import IntegrityError
from werkzeug.security import check_password_hash
import requests

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

# Flask app setup
app = Flask(__name__)
app.config.from_prefixed_env()
socketio = SocketIO(app, async_mode='threading')  # Use threading for gthread worker
jwt = JWTManager(app)
# Updated Flask-Limiter 3.x+ initialization:
limiter = Limiter(key_func=get_remote_address, default_limits=["200 per day", "50 per hour"])
limiter.init_app(app)
db = SQLAlchemy(app)
cache = Cache(app, config={'CACHE_TYPE': 'redis', 'CACHE_REDIS_URL': os.getenv('REDIS_URL')})

# Flask-RESTX setup
api = Api(app, version='1.0', title='Courier Tracking API', description='API for tracking shipments')

# Celery setup
celery = Celery(app.name, broker=os.getenv('REDIS_URL'), backend=os.getenv('REDIS_URL'))
celery.conf.update(app.config)

# Pydantic models for validation
class CheckpointCreate(BaseModel):
    lat: float
    lng: float
    label: str
    note: Optional[str] = None
    status: Optional[str] = None

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

    @validator('label')
    def check_label(cls, v):
        if not v.strip():
            raise ValueError('Label cannot be empty')
        return v

class ShipmentCreate(BaseModel):
    tracking_number: str
    title: str = "Consignment"
    origin: Dict[str, float]
    destination: Dict[str, float]
    status: str = "Created"

    @validator('tracking_number')
    def check_tracking_number(cls, v):
        if not v.strip():
            raise ValueError('Tracking number cannot be empty')
        return v

    @validator('origin', 'destination')
    def check_coordinates(cls, v):
        if not isinstance(v, dict) or 'lat' not in v or 'lng' not in v:
            raise ValueError('Coordinates must be a dict with lat and lng')
        if not -90 <= v['lat'] <= 90:
            raise ValueError('Latitude must be between -90 and 90')
        if not -180 <= v['lng'] <= 180:
            raise ValueError('Longitude must be between -180 and 180')
        return v

# SQLAlchemy models
class Shipment(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    tracking = db.Column(db.String(50), unique=True, nullable=False)
    title = db.Column(db.String(100), nullable=False)
    origin_lat = db.Column(db.Float, nullable=False)
    origin_lng = db.Column(db.Float, nullable=False)
    dest_lat = db.Column(db.Float, nullable=False)
    dest_lng = db.Column(db.Float, nullable=False)
    status = db.Column(db.String(50), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    checkpoints = db.relationship('Checkpoint', backref='shipment', lazy=True)
    subscribers = db.relationship('Subscriber', backref='shipment', lazy=True)

    def to_dict(self):
        return {
            'id': self.id,
            'tracking': self.tracking,
            'title': self.title,
            'origin': {'lat': self.origin_lat, 'lng': self.origin_lng},
            'destination': {'lat': self.dest_lat, 'lng': self.dest_lng},
            'status': self.status,
            'created_at': self.created_at.isoformat() + 'Z'
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
            'timestamp': self.timestamp.isoformat() + 'Z'
        }

class Subscriber(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    shipment_id = db.Column(db.Integer, db.ForeignKey('shipment.id'), nullable=False)
    email = db.Column(db.String(120), nullable=False)
    is_active = db.Column(db.Boolean, default=True)

# Flask-RESTX namespace
ns = api.namespace('shipments', description='Shipment operations')

shipment_model = api.model('Shipment', {
    'tracking': fields.String(required=True),
    'title': fields.String,
    'origin': fields.Raw(required=True),
    'destination': fields.Raw(required=True),
    'status': fields.String
})

checkpoint_model = api.model('Checkpoint', {
    'lat': fields.Float(required=True),
    'lng': fields.Float(required=True),
    'label': fields.String(required=True),
    'note': fields.String,
    'status': fields.String
})

# Routes
@app.route('/health')
def health():
    try:
        db.session.execute('SELECT 1')
        return jsonify({'status': 'healthy', 'database': 'connected'})
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return jsonify({'status': 'unhealthy', 'database': 'disconnected'}), 500

@app.route('/login', methods=['POST'])
def login():
    data = request.get_json()
    if data.get('password') and check_password_hash(app.config['ADMIN_PASSWORD'], data['password']):
        access_token = create_access_token(identity='admin')
        return jsonify({'access_token': access_token})
    return jsonify({'error': 'Invalid password'}), 401

@app.route('/unsubscribe/<tracking>')
def unsubscribe(tracking):
    email = request.args.get('email')
    if not email:
        return jsonify({'error': 'Email is required'}), 400
    shipment = Shipment.query.filter_by(tracking=tracking).first()
    if not shipment:
        return jsonify({'error': 'Shipment not found'}), 404
    subscriber = Subscriber.query.filter_by(shipment_id=shipment.id, email=email).first()
    if not subscriber:
        return jsonify({'error': 'Subscriber not found'}), 404
    subscriber.is_active = False
    db.session.commit()
    return jsonify({'message': 'Unsubscribed successfully'})

@app.route('/track/<tracking>')
def track(tracking):
    shipment = Shipment.query.filter_by(tracking=tracking).first()
    if not shipment:
        return jsonify({'error': 'Shipment not found'}), 404
    checkpoints = [cp.to_dict() for cp in shipment.checkpoints]
    return render_template('track.html', shipment=shipment.to_dict(), checkpoints=checkpoints)

@socketio.on('connect', namespace='/track')
def handle_connect():
    logger.info('Client connected to WebSocket')
    emit('status', {'message': 'Connected'})

@ns.route('/')
class ShipmentList(Resource):
    @jwt_required()
    @api.marshal_list_with(shipment_model)
    def get(self):
        shipments = Shipment.query.all()
        return [s.to_dict() for s in shipments]

    @jwt_required()
    @api.expect(shipment_model)
    def post(self):
        try:
            data = ShipmentCreate(**request.get_json()).dict()
            shipment = Shipment(
                tracking=data['tracking_number'],
                title=data['title'],
                origin_lat=data['origin']['lat'],
                origin_lng=data['origin']['lng'],
                dest_lat=data['destination']['lat'],
                dest_lng=data['destination']['lng'],
                status=data['status']
            )
            db.session.add(shipment)
            db.session.commit()
            return shipment.to_dict(), 201
        except ValueError as e:
            return {'error': str(e)}, 400
        except IntegrityError:
            db.session.rollback()
            return {'error': 'Tracking number already exists'}, 409

@ns.route('/<tracking>/checkpoints')
class CheckpointList(Resource):
    @jwt_required()
    @api.expect(checkpoint_model)
    def post(self, tracking):
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return {'error': 'Shipment not found'}, 404
        try:
            data = CheckpointCreate(**request.get_json()).dict()
            position = db.session.query(db.func.max(Checkpoint.position)).filter_by(shipment_id=shipment.id).scalar() or 0
            checkpoint = Checkpoint(
                shipment_id=shipment.id,
                position=position + 1,
                lat=data['lat'],
                lng=data['lng'],
                label=data['label'],
                note=data['note'],
                status=data['status']
            )
            db.session.add(checkpoint)
            if data['status']:
                shipment.status = data['status']
            db.session.commit()
            socketio.emit('checkpoint', checkpoint.to_dict(), namespace='/track')
            for subscriber in shipment.subscribers:
                if subscriber.is_active:
                    send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
            return checkpoint.to_dict(), 201
        except ValueError as e:
            return {'error': str(e)}, 400

@ns.route('/<tracking>/subscribe')
class Subscribe(Resource):
    def post(self, tracking):
        email = request.get_json().get('email')
        if not email:
            return {'error': 'Email is required'}, 400
        shipment = Shipment.query.filter_by(tracking=tracking).first()
        if not shipment:
            return {'error': 'Shipment not found'}, 404
        subscriber = Subscriber(shipment_id=shipment.id, email=email)
        try:
            db.session.add(subscriber)
            db.session.commit()
            return {'message': 'Subscribed successfully'}, 201
        except IntegrityError:
            db.session.rollback()
            return {'error': 'Email already subscribed'}, 409

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

    def send_message(text):
        requests.post(
            f"https://api.telegram.org/bot{app.config['TELEGRAM_TOKEN']}/sendMessage",
            json={'chat_id': chat_id, 'text': text}
        )

    try:
        command, *args = text.split(' ', 1)
        args = args[0].split('|') if args else []

        if command == '/create' and len(args) == 4:
            tracking, title, origin, dest = args
            origin_lat, origin_lng = map(float, origin.split(','))
            dest_lat, dest_lng = map(float, dest.split(','))
            shipment_data = {
                'tracking_number': tracking,
                'title': title,
                'origin': {'lat': origin_lat, 'lng': origin_lng},
                'destination': {'lat': dest_lat, 'lng': dest_lng},
                'status': 'Created'
            }
            ShipmentCreate(**shipment_data)  # Validate
            shipment = Shipment(**shipment_data)
            db.session.add(shipment)
            db.session.commit()
            send_message(f"Shipment {tracking} created successfully")
            return jsonify({'message': 'OK'})

        elif command == '/subscribe' and len(args) == 2:
            tracking, email = args
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                send_message("Shipment not found")
                return jsonify({'error': 'Shipment not found'}), 404
            subscriber = Subscriber(shipment_id=shipment.id, email=email)
            db.session.add(subscriber)
            db.session.commit()
            send_message(f"Subscribed {email} to {tracking}")
            return jsonify({'message': 'OK'})

        elif command == '/addcp' and len(args) >= 3:
            tracking, coords, label, *note = args
            note = note[0] if note else None
            lat, lng = map(float, coords.split(','))
            checkpoint_data = {'lat': lat, 'lng': lng, 'label': label, 'note': note}
            CheckpointCreate(**checkpoint_data)  # Validate
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                send_message("Shipment not found")
                return jsonify({'error': 'Shipment not found'}), 404
            position = db.session.query(db.func.max(Checkpoint.position)).filter_by(shipment_id=shipment.id).scalar() or 0
            checkpoint = Checkpoint(shipment_id=shipment.id, position=position + 1, **checkpoint_data)
            db.session.add(checkpoint)
            db.session.commit()
            socketio.emit('checkpoint', checkpoint.to_dict(), namespace='/track')
            for subscriber in shipment.subscribers:
                if subscriber.is_active:
                    send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
            send_message(f"Checkpoint added to {tracking}")
            return jsonify({'message': 'OK'})

        elif command == '/simulate' and len(args) == 3:
            tracking, num_points, step_hours = args
            num_points, step_hours = int(num_points), int(step_hours)
            shipment = Shipment.query.filter_by(tracking=tracking).first()
            if not shipment:
                send_message("Shipment not found")
                return jsonify({'error': 'Shipment not found'}), 404
            position = db.session.query(db.func.max(Checkpoint.position)).filter_by(shipment_id=shipment.id).scalar() or 0
            for i in range(1, num_points + 1):
                lat = shipment.origin_lat + (shipment.dest_lat - shipment.origin_lat) * i / num_points
                lng = shipment.origin_lng + (shipment.dest_lng - shipment.origin_lng) * i / num_points
                checkpoint = Checkpoint(
                    shipment_id=shipment.id,
                    position=position + i,
                    lat=lat,
                    lng=lng,
                    label=f"Simulated Checkpoint {i}",
                    timestamp=datetime.utcnow() + timedelta(hours=i * step_hours)
                )
                db.session.add(checkpoint)
                socketio.emit('checkpoint', checkpoint.to_dict(), namespace='/track')
                for subscriber in shipment.subscribers:
                    if subscriber.is_active:
                        send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
            shipment.status = 'In Transit'
            db.session.commit()
            send_message(f"Simulated {num_points} checkpoints for {tracking}")
            return jsonify({'message': 'OK'})

        else:
            send_message("Invalid command or arguments")
            return jsonify({'error': 'Invalid command'}), 400

    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        send_message("Error processing command")
        return jsonify({'error': str(e)}), 400

if __name__ == '__main__':
    with app.app_context():
        db.create_all()
    socketio.run(app, debug=True, allow_unsafe_werkzeug=True)
