import json
from flask import request, current_app, jsonify
from app import app, db, Shipment, Subscriber, SimulationState, logger, socketio, admin_sessions, celery
import requests
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
import os
from werkzeug.utils import secure_filename
from PIL import Image
from io import BytesIO
import bleach
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from werkzeug.security import check_password_hash
from enum import Enum
from app.models import Checkpoint, StatusHistory  # Added missing imports
from app.utils import geocode_address, CheckpointCreate  # Added missing imports
from app.tasks import send_checkpoint_email_async, send_checkpoint_sms_async, run_simulation_async  # Added missing imports

class ShipmentStatus(Enum):
    CREATED = "Created"
    IN_TRANSIT = "In Transit"
    OUT_FOR_DELIVERY = "Out for Delivery"
    DELIVERED = "Delivered"

@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    try:
        update = request.get_json(force=True) or {}
        logger.debug(f"Received Telegram update: {json.dumps(update, indent=2)}")

        message = update.get('message', {})
        callback_query = update.get('callback_query', {})

        def send_message(text, chat_id, reply_markup=None, edit_message_id=None):
            """Send or edit a Telegram message."""
            try:
                payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}
                if reply_markup:
                    payload['reply_markup'] = reply_markup
                url = f"https://api.telegram.org/bot{current_app.config['TELEGRAM_TOKEN']}/"
                url += "editMessageText" if edit_message_id else "sendMessage"
                if edit_message_id:
                    payload['message_id'] = edit_message_id
                response = requests.post(url, json=payload, timeout=5)
                response.raise_for_status()
                logger.info(f"{'Edited' if edit_message_id else 'Sent'} message to chat {chat_id}: '{text}'")
            except requests.RequestException as e:
                logger.error(f"Failed to send/edit message to {chat_id}: {e}")

        def compress_image(file_content, max_size=(800, 800), quality=85, max_file_size=2 * 1024 * 1024):
            """Compress an image to reduce file size while maintaining quality."""
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
                if len(compressed_content) > max_file_size:
                    raise ValueError(f"Compressed image size ({len(compressed_content)} bytes) exceeds {max_file_size} bytes")
                logger.debug(f"Compressed image from {len(file_content)} to {len(compressed_content)} bytes")
                return compressed_content
            except Exception as e:
                logger.error(f"Image compression failed: {e}")
                raise ValueError(f"Failed to compress image: {str(e)}")

        def upload_to_s3(file_content, filename):
            """Upload file to AWS S3 and return the URL."""
            try:
                s3_client = boto3.client(
                    's3',
                    aws_access_key_id=current_app.config.get('AWS_ACCESS_KEY_ID'),
                    aws_secret_access_key=current_app.config.get('AWS_SECRET_ACCESS_KEY')
                )
                bucket = current_app.config.get('S3_BUCKET')
                key = f"proof_photos/{secure_filename(filename)}"
                s3_client.put_object(Bucket=bucket, Key=key, Body=file_content, ContentType='image/jpeg')
                url = f"https://{bucket}.s3.amazonaws.com/{key}"
                logger.info(f"Uploaded photo to S3: {url}")
                return url
            except ClientError as e:
                logger.error(f"S3 upload failed: {e}")
                return None

        def save_locally(file_content, filename):
            """Save file locally and return the URL."""
            try:
                upload_dir = os.path.join(current_app.config.get('UPLOAD_FOLDER', 'uploads'), 'proof_photos')
                os.makedirs(upload_dir, exist_ok=True)
                file_path = os.path.join(upload_dir, secure_filename(filename))
                with open(file_path, 'wb') as f:
                    f.write(file_content)
                url = f"{current_app.config['APP_BASE_URL']}/{file_path}"
                logger.info(f"Saved photo locally: {url}")
                return url
            except Exception as e:
                logger.error(f"Local file save failed: {e}")
                return None

        def get_navigation_keyboard(tracking=None):
            """Generate the main navigation keyboard."""
            buttons = [
                [{'text': 'Create Shipment', 'callback_data': '/create'}],
                [{'text': 'Subscribe', 'callback_data': '/subscribe'}],
                [{'text': 'Add Checkpoint', 'callback_data': '/addcp'}],
                [{'text': 'Simulate', 'callback_data': '/simulate'}],
                [{'text': 'Track Multiple', 'callback_data': '/track_multiple'}]
            ]
            session = admin_sessions.get(chat_id, {})
            if session.get('authenticated') and datetime.utcnow() < session.get('expires', datetime.utcnow()):
                buttons.insert(0, [{'text': 'Admin Panel', 'callback_data': '/admin_menu'}])
            if tracking:
                buttons.append([{'text': f'Track {tracking}', 'url': f"{current_app.config['APP_BASE_URL']}/track/{tracking}"}])
            return {'inline_keyboard': buttons}

        def get_admin_keyboard():
            """Generate the admin panel keyboard."""
            return {
                'inline_keyboard': [
                    [{'text': 'List Shipments', 'callback_data': '/admin_list_shipments'}],
                    [{'text': 'Create Shipment', 'callback_data': '/admin_create_shipment'}],
                    [{'text': 'Update Status', 'callback_data': '/admin_update_status'}],
                    [{'text': 'Delete Shipment', 'callback_data': '/admin_delete_shipment'}],
                    [{'text': 'List Subscribers', 'callback_data': '/admin_list_subscribers'}],
                    [{'text': 'Unsubscribe User', 'callback_data': '/admin_unsubscribe'}],
                    [{'text': 'Pause Simulation', 'callback_data': '/admin_pause_sim'}],
                    [{'text': 'Continue Simulation', 'callback_data': '/admin_continue_sim'}],
                    [{'text': 'Health Check', 'callback_data': '/admin_health'}],
                    [{'text': 'Logout', 'callback_data': '/admin_logout'}]
                ]
            }

        def get_shipment_selection_keyboard(command, prefix="select_shipment"):
            """Generate keyboard for selecting shipments."""
            shipments = Shipment.query.all()
            buttons = [
                [{'text': f"{s.tracking}: {s.title}", 'callback_data': f"{prefix}|{command}|{s.tracking}"}]
                for s in shipments[:10]
            ]
            buttons.append([{'text': 'Back to Admin Menu', 'callback_data': '/admin_menu'}])
            return {'inline_keyboard': buttons}

        def get_status_selection_keyboard(tracking):
            """Generate keyboard for selecting shipment status."""
            buttons = [
                [{'text': status.value, 'callback_data': f"select_status|/admin_update_status|{tracking}|{status.value}"}]
                for status in ShipmentStatus
            ]
            buttons.append([{'text': 'Back to Admin Menu', 'callback_data': '/admin_menu'}])
            return {'inline_keyboard': buttons}

        chat_id = message.get('chat', {}).get('id') or callback_query.get('message', {}).get('chat', {}).get('id')
        if not chat_id:
            logger.error("No chat ID found in Telegram update")
            return jsonify({'error': 'No chat ID found'}), 400

        if message and message.get('text'):
            text = message['text'].strip()
            session = admin_sessions.get(chat_id, {})

            if text.startswith('/start'):
                send_message("📦 Welcome to Courier Tracking! Use the buttons below to manage shipments. 🔍", chat_id, get_navigation_keyboard())
                return jsonify({'message': 'Welcome sent'})

            elif text.startswith('/admin_login'):
                parts = text.split(maxsplit=1)
                if len(parts) != 2:
                    send_message("🚫 Usage: /admin_login <password> 🔐", chat_id, get_navigation_keyboard())
                    return jsonify({'error': 'Invalid command format'}), 400
                password = parts[1]
                if check_password_hash(current_app.config['ADMIN_PASSWORD_HASH'], password):
                    admin_sessions[chat_id] = {
                        'authenticated': True,
                        'expires': datetime.utcnow() + timedelta(hours=1)
                    }
                    send_message("🔐 Admin login successful! 🔧", chat_id, get_admin_keyboard())
                    logger.info(f"Admin login successful for chat {chat_id}")
                    return jsonify({'message': 'Admin logged in'})
                send_message("🚫 Incorrect password! 🔐", chat_id, get_navigation_keyboard())
                return jsonify({'error': 'Invalid password'}), 401

            elif 'state' in session:
                state = session['state']
                if state['step'] == 'tracking':
                    tracking = bleach.clean(text)
                    if state['command'] in ['/create', '/admin_create_shipment']:
                        session['state'] = {'command': state['command'], 'step': 'title', 'tracking': tracking}
                        send_message(f"📦 Enter title for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    elif state['command'] == '/subscribe':
                        session['state'] = {'command': '/subscribe', 'step': 'contact', 'tracking': tracking}
                        send_message(f"📩 Enter email or phone for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    elif state['command'] == '/addcp':
                        session['state'] = {'command': '/addcp', 'step': 'location', 'tracking': tracking}
                        send_message(f"📍 Enter location (lat,lng or address) for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    elif state['command'] == '/simulate':
                        session['state'] = {'command': '/simulate', 'step': 'num_points', 'tracking': tracking}
                        send_message(f"🚚 Enter number of points (2-10) for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted next step'})

                elif state['step'] == 'title':
                    title = bleach.clean(text)
                    session['state']['step'] = 'origin'
                    session['state']['title'] = title
                    send_message(f"📍 Enter origin (lat,lng or address) for {state['tracking']} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted origin'})

                elif state['step'] == 'origin':
                    origin = bleach.clean(text)
                    session['state']['step'] = 'destination'
                    session['state']['origin'] = origin
                    send_message(f"📍 Enter destination (lat,lng or address) for {state['tracking']} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted destination'})

                elif state['step'] == 'destination':
                    destination = bleach.clean(text)
                    session['state']['step'] = 'status'
                    session['state']['destination'] = destination
                    send_message(f"📋 Enter status (Created, In Transit, Out for Delivery, Delivered) for {state['tracking']} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted status'})

                elif state['step'] == 'status' and state['command'] in ['/create', '/admin_create_shipment']:
                    status = bleach.clean(text)
                    if status not in [s.value for s in ShipmentStatus]:
                        send_message(
                            f"🚫 Invalid status! Use: {', '.join(s.value for s in ShipmentStatus)} (or press Cancel)",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        return jsonify({'error': 'Invalid status'})
                    session['state']['step'] = 'photo'
                    session['state']['status'] = status
                    send_message(
                        f"📸 Upload a proof photo for {state['tracking']} (or press Skip/Cancel):",
                        chat_id,
                        {'inline_keyboard': [
                            [{'text': 'Skip', 'callback_data': f'skip_photo|/addcp|{state['tracking']}'}],
                            [{'text': 'Cancel', 'callback_data': '/cancel'}]
                        ]}
                    )
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted photo'})

                elif state['step'] == 'contact' and state['command'] == '/subscribe':
                    contact = bleach.clean(text)
                    tracking = state['tracking']
                    session.pop('state', None)
                    admin_sessions[chat_id] = session
                    if '@' in contact:
                        email = contact
                        phone = None
                    else:
                        email = None
                        phone = contact
                    if not email and not phone:
                        send_message(
                            f"🚫 Email or phone required for {tracking} (or press Cancel)",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        return jsonify({'error': 'Email or phone required'}), 400
                    shipment = Shipment.query.filter_by(tracking=tracking).first()
                    if not shipment:
                        send_message(
                            f"🔍 Shipment {tracking} not found! (or press Cancel)",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        return jsonify({'error': 'Shipment not found'}), 404
                    subscriber = Subscriber(shipment_id=shipment.id, email=email, phone=phone)
                    try:
                        with db.session.begin():
                            db.session.add(subscriber)
                        send_message(
                            f"📩 Subscribed {contact} to {tracking} successfully! 🔍",
                            chat_id,
                            get_navigation_keyboard(tracking)
                        )
                        logger.info(f"Subscribed {contact} to {tracking} via Telegram by chat {chat_id}")
                        return jsonify({'message': 'Subscribed'})
                    except IntegrityError:
                        db.session.rollback()
                        send_message(
                            f"🚫 Already subscribed {contact} to {tracking}! (or press Cancel)",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        return jsonify({'error': 'Already subscribed'}), 409
                    except SQLAlchemyError as e:
                        db.session.rollback()
                        logger.error(f"Database error in Telegram subscription: {e}")
                        send_message(
                            f"💾 Database error! (or press Cancel)",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        return jsonify({'error': 'Database error'}), 500

                elif state['step'] == 'location' and state['command'] == '/addcp':
                    location = bleach.clean(text)
                    session['state']['step'] = 'label'
                    session['state']['location'] = location
                    send_message(
                        f"📍 Enter label for {state['tracking']} (or press Cancel):",
                        chat_id,
                        {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                    )
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted label'})

                elif state['step'] == 'label':
                    label = bleach.clean(text)
                    session['state']['step'] = 'note'
                    session['state']['label'] = label
                    send_message(
                        f"📝 Enter note for {state['tracking']} (optional, or press Cancel):",
                        chat_id,
                        {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                    )
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted note'})

                elif state['step'] == 'note':
                    note = bleach.clean(text) or None
                    session['state']['step'] = 'status'
                    session['state']['note'] = note
                    send_message(
                        f"📋 Enter status (optional: Created, In Transit, Out for Delivery, Delivered) for {state['tracking']} (or press Skip/Cancel):",
                        chat_id,
                        {'inline_keyboard': [
                            [{'text': 'Skip', 'callback_data': f'skip_status|/addcp|{state['tracking']}'}],
                            [{'text': 'Cancel', 'callback_data': '/cancel'}]
                        ]}
                    )
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted status'})

                elif state['step'] == 'status' and state['command'] == '/addcp':
                    status = bleach.clean(text) or None
                    if status and status not in [s.value for s in ShipmentStatus]:
                        send_message(
                            f"🚫 Invalid status! Use: {', '.join(s.value for s in ShipmentStatus)} (or press Skip/Cancel)",
                            chat_id,
                            {'inline_keyboard': [
                                [{'text': 'Skip', 'callback_data': f'skip_status|/addcp|{state['tracking']}'}],
                                [{'text': 'Cancel', 'callback_data': '/cancel'}]
                            ]}
                        )
                        return jsonify({'error': 'Invalid status'})
                    session['state']['step'] = 'photo'
                    session['state']['status'] = status
                    send_message(
                        f"📸 Upload a proof photo for {state['tracking']} (or press Skip/Cancel):",
                        chat_id,
                        {'inline_keyboard': [
                            [{'text': 'Skip', 'callback_data': f'skip_photo|/addcp|{state['tracking']}'}],
                            [{'text': 'Cancel', 'callback_data': '/cancel'}]
                        ]}
                    )
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted photo'})

                elif state['step'] == 'num_points' and state['command'] == '/simulate':
                    try:
                        num_points = int(bleach.clean(text))
                        if num_points < 2 or num_points > 10:
                            send_message(
                                "🚫 Number of points must be 2-10 (or press Cancel)",
                                chat_id,
                                {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                            )
                            return jsonify({'error': 'Invalid number of points'}), 400
                        session['state']['step'] = 'step_hours'
                        session['state']['num_points'] = num_points
                        send_message(
                            f"⏱ Enter step hours (1-24) for {state['tracking']} (or press Cancel):",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        admin_sessions[chat_id] = session
                        return jsonify({'message': 'Prompted step hours'})
                    except ValueError:
                        send_message(
                            "🚫 Invalid number! Enter 2-10 (or press Cancel)",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        return jsonify({'error': 'Invalid number'}), 400

                elif state['step'] == 'step_hours' and state['command'] == '/simulate':
                    try:
                        step_hours = int(bleach.clean(text))
                        if step_hours < 1 or step_hours > 24:
                            send_message(
                                "🚫 Step hours must be 1-24 (or press Cancel)",
                                chat_id,
                                {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                            )
                            return jsonify({'error': 'Invalid step hours'}), 400
                        tracking = state['tracking']
                        num_points = state['num_points']
                        session.pop('state', None)
                        admin_sessions[chat_id] = session
                        shipment = Shipment.query.filter_by(tracking=tracking).first()
                        if not shipment:
                            send_message(
                                f"🔍 Shipment {tracking} not found! (or press Cancel)",
                                chat_id,
                                {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                            )
                            return jsonify({'error': 'Shipment not found'}), 404
                        if shipment.status == ShipmentStatus.DELIVERED.value:
                            send_message(
                                f"🚫 Shipment {tracking} already delivered!",
                                chat_id,
                                get_navigation_keyboard(tracking)
                            )
                            return jsonify({'error': 'Shipment already delivered'}), 400
                        simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
                        if simulation_state and simulation_state.status == 'running':
                            send_message(
                                f"🚫 Simulation already running for {tracking}!",
                                chat_id,
                                get_navigation_keyboard(tracking)
                            )
                            return jsonify({'error': 'Simulation already running'}), 400
                        run_simulation_async.delay(shipment.id, num_points, step_hours)
                        send_message(
                            f"🚚 Simulation started for {tracking} with {num_points} points and {step_hours} hours/step!",
                            chat_id,
                            get_navigation_keyboard(tracking)
                        )
                        logger.info(f"Simulation started for {tracking} via Telegram by chat {chat_id}")
                        return jsonify({'message': 'Simulation started'})
                    except ValueError:
                        send_message(
                            "🚫 Invalid number! Enter 1-24 (or press Cancel)",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        return jsonify({'error': 'Invalid number'}), 400
                    except SQLAlchemyError as e:
                        db.session.rollback()
                        logger.error(f"Database error in Telegram simulation: {e}")
                        send_message(
                            f"💾 Database error! (or press Cancel)",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        return jsonify({'error': 'Database error'}), 500

                elif state['step'] == 'tracking_contact' and state['command'] == '/admin_unsubscribe':
                    parts = text.split(maxsplit=1)
                    if len(parts) != 2:
                        send_message(
                            "🚫 Usage: <tracking> <email or phone> (or press Cancel)",
                            chat_id,
                            {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                        )
                        return jsonify({'error': 'Invalid format'}), 400
                    tracking, contact = parts
                    tracking = bleach.clean(tracking)
                    contact = bleach.clean(contact)
                    shipment = Shipment.query.filter_by(tracking=tracking).first()
                    if not shipment:
                        send_message(
                            f"🔍 Shipment {tracking} not found!",
                            chat_id,
                            get_admin_keyboard()
                        )
                        return jsonify({'error': 'Shipment not found'}), 404
                    subscriber = Subscriber.query.filter_by(shipment_id=shipment.id).filter(
                        (Subscriber.email == contact) | (Subscriber.phone == contact)
                    ).first()
                    if not subscriber:
                        send_message(
                            f"📩 Subscriber {contact} not found for {tracking}!",
                            chat_id,
                            get_admin_keyboard()
                        )
                        return jsonify({'error': 'Subscriber not found'}), 404
                    try:
                        with db.session.begin():
                            db.session.delete(subscriber)
                        send_message(
                            f"📩 Unsubscribed {contact} from {tracking}!",
                            chat_id,
                            get_admin_keyboard()
                        )
                        logger.info(f"Unsubscribed {contact} from {tracking} via Telegram by chat {chat_id}")
                        return jsonify({'message': 'Unsubscribed'})
                    except SQLAlchemyError as e:
                        db.session.rollback()
                        logger.error(f"Database error in Telegram unsubscribe: {e}")
                        send_message(
                            f"💾 Database error!",
                            chat_id,
                            get_admin_keyboard()
                        )
                        return jsonify({'error': 'Database error'}), 500

            elif state['step'] == 'tracking_numbers' and state['command'] == '/track_multiple':
                tracking_numbers = [bleach.clean(t.strip()) for t in text.split(',')]
                session.pop('state', None)
                admin_sessions[chat_id] = session
                valid_shipments = []
                for tracking in tracking_numbers:
                    shipment = Shipment.query.filter_by(tracking=tracking).first()
                    if shipment:
                        valid_shipments.append(shipment)
                if not valid_shipments:
                    send_message(
                        "🔍 No valid shipments found for the provided tracking numbers!",
                        chat_id,
                        get_navigation_keyboard()
                    )
                    return jsonify({'error': 'No shipments found'}), 404
                text = "\n".join([f"📦 {s.tracking}: {s.title} ({s.status})" for s in valid_shipments])
                buttons = [
                    [{'text': f'Track {s.tracking}', 'url': f"{current_app.config['APP_BASE_URL']}/track/{s.tracking}"}]
                    for s in valid_shipments
                ]
                buttons.append([{'text': 'Back to Menu', 'callback_data': '/cancel'}])
                send_message(
                    f"🔍 Tracking Results:\n{text}",
                    chat_id,
                    {'inline_keyboard': buttons}
                )
                logger.info(f"Tracked multiple shipments via Telegram by chat {chat_id}")
                return jsonify({'message': 'Tracked multiple shipments'})

        elif message and message.get('photo') and 'state' in admin_sessions.get(chat_id, {}):
            session = admin_sessions[chat_id]
            state = session['state']
            if state['step'] != 'photo':
                send_message(
                    "🚫 Unexpected photo upload! Please start over.",
                    chat_id,
                    get_navigation_keyboard()
                )
                return jsonify({'error': 'Unexpected photo'}), 400
            tracking = state['tracking']
            location = state['location']
            label = state['label']
            note = state.get('note')
            status = state.get('status')
            session.pop('state', None)
            admin_sessions[chat_id] = session
            photo = message['photo'][-1]  # Highest quality photo
            file_id = photo['file_id']
            file_size = photo['file_size']
            if file_size > 10 * 1024 * 1024:  # 10MB limit
                send_message(
                    f"🚫 Photo too large (max 10MB)! Try again or press Cancel.",
                    chat_id,
                    {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                )
                return jsonify({'error': 'Photo too large'}), 400
            try:
                # Get file path from Telegram
                response = requests.get(
                    f"https://api.telegram.org/bot{current_app.config['TELEGRAM_TOKEN']}/getFile",
                    params={'file_id': file_id},
                    timeout=5
                )
                response.raise_for_status()
                file_path = response.json()['result']['file_path']
                file_url = f"https://api.telegram.org/file/bot{current_app.config['TELEGRAM_TOKEN']}/{file_path}"
                file_response = requests.get(file_url, timeout=10)
                file_response.raise_for_status()
                file_content = file_response.content
                compressed_content = compress_image(file_content, max_size=(800, 800), quality=85)
                filename = f"{tracking}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.jpg"
                photo_url = upload_to_s3(compressed_content, filename) or save_locally(compressed_content, filename)
                if not photo_url:
                    send_message(
                        f"🚫 Failed to store photo! (or press Cancel)",
                        chat_id,
                        {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                    )
                    return jsonify({'error': 'Photo storage failed'}), 500
                shipment = Shipment.query.filter_by(tracking=tracking).first()
                if not shipment:
                    send_message(
                        f"🔍 Shipment {tracking} not found! (or press Cancel)",
                        chat_id,
                        {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                    )
                    return jsonify({'error': 'Shipment not found'}), 404
                checkpoint_data = {
                    'address': location if ',' not in location else None,
                    'lat': float(location.split(',')[0]) if ',' in location and all(x.replace('.', '', 1).isdigit() for x in location.split(',')) else None,
                    'lng': float(location.split(',')[1]) if ',' in location and all(x.replace('.', '', 1).isdigit() for x in location.split(',')) else None,
                    'label': label,
                    'note': note,
                    'status': status,
                    'proof_photo': photo_url
                }
                try:
                    CheckpointCreate(**checkpoint_data)
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
                        label=label,
                        note=note,
                        status=status,
                        proof_photo=photo_url
                    )
                    with db.session.begin():
                        db.session.add(checkpoint)
                        if status:
                            shipment.status = status
                            db.session.add(StatusHistory(shipment=shipment, status=status))
                            if shipment.status == ShipmentStatus.DELIVERED.value:
                                shipment.eta = checkpoint.timestamp
                    socketio.emit('update', shipment.to_dict(), namespace='/', room=tracking)
                    for subscriber in shipment.subscribers:
                        if subscriber.is_active:
                            if subscriber.email:
                                send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
                            if subscriber.phone:
                                send_checkpoint_sms_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.phone)
                    send_message(
                        f"📍 Checkpoint added to {tracking} with photo successfully! 🔍",
                        chat_id,
                        get_navigation_keyboard(tracking)
                    )
                    logger.info(f"Checkpoint added to {tracking} with compressed photo via Telegram by chat {chat_id}")
                    return jsonify({'message': 'Checkpoint added'})
                except ValidationError as e:
                    send_message(
                        f"🚫 Error: {str(e)} (or press Cancel)",
                        chat_id,
                        {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                    )
                    return jsonify({'error': str(e)}), 400
                except ValueError as e:
                    send_message(
                        f"🚫 Error: {str(e)} (or press Cancel)",
                        chat_id,
                        {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                    )
                    return jsonify({'error': str(e)}), 400
                except SQLAlchemyError as e:
                    db.session.rollback()
                    logger.error(f"Database error in Telegram checkpoint creation: {e}")
                    send_message(
                        f"💾 Database error! (or press Cancel)",
                        chat_id,
                        {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                    )
                    return jsonify({'error': 'Database error'}), 500
            except requests.RequestException as e:
                logger.error(f"Telegram file download failed: {e}")
                send_message(
                    f"🚫 Failed to download photo! (or press Cancel)",
                    chat_id,
                    {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}
                )
                return jsonify({'error': 'Photo download failed'}), 500

        elif callback_query:
            data = callback_query['data']
            message_id = callback_query['message']['message_id']
            session = admin_sessions.get(chat_id, {})

            if data == '/create':
                admin_sessions[chat_id] = {'state': {'command': '/create', 'step': 'tracking'}}
                send_message("📦 Enter tracking number:", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}, message_id)
                return jsonify({'message': 'Prompted tracking number'})

            elif data == '/subscribe':
                admin_sessions[chat_id] = {'state': {'command': '/subscribe', 'step': 'tracking'}}
                send_message("📩 Enter tracking number to subscribe:", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}, message_id)
                return jsonify({'message': 'Prompted tracking number'})

            elif data == '/addcp':
                admin_sessions[chat_id] = {'state': {'command': '/addcp', 'step': 'tracking'}}
                send_message("📍 Enter tracking number for checkpoint:", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}, message_id)
                return jsonify({'message': 'Prompted tracking number'})

            elif data == '/simulate':
                admin_sessions[chat_id] = {'state': {'command': '/simulate', 'step': 'tracking'}}
                send_message("🚚 Enter tracking number to simulate:", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}, message_id)
                return jsonify({'message': 'Prompted tracking number'})

            elif data == '/track_multiple':
                admin_sessions[chat_id] = {'state': {'command': '/track_multiple', 'step': 'tracking_numbers'}}
                send_message("🔍 Enter tracking numbers (comma-separated):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}, message_id)
                return jsonify({'message': 'Prompted tracking numbers'})

            elif data == '/admin_menu':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                send_message("🔧 Admin Panel:", chat_id, get_admin_keyboard(), message_id)
                return jsonify({'message': 'Admin menu displayed'})

            elif data == '/admin_create_shipment':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                admin_sessions[chat_id]['state'] = {'command': '/admin_create_shipment', 'step': 'tracking'}
                send_message("📦 Enter tracking number:", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]}, message_id)
                return jsonify({'message': 'Prompted tracking number'})

            elif data == '/admin_list_shipments':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                shipments = Shipment.query.all()
                if not shipments:
                    send_message("📦 No shipments found.", chat_id, get_admin_keyboard(), message_id)
                else:
                    text = "\n".join([f"📦 {s.tracking}: {s.title} ({s.status})" for s in shipments[:10]])
                    send_message(f"📋 Shipments:\n{text}", chat_id, get_admin_keyboard(), message_id)
                return jsonify({'message': 'Listed shipments'})

            elif data == '/admin_delete_shipment':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                send_message("📦 Select a shipment to delete:", chat_id, get_shipment_selection_keyboard('/admin_delete_shipment'), message_id)
                return jsonify({'message': 'Prompted shipment selection'})

            elif data.startswith('select_shipment|/admin_delete_shipment|'):
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                tracking = data.split('|')[-1]
                shipment = Shipment.query.filter_by(tracking=tracking).first()
                if not shipment:
                    send_message(f"🔍 Shipment {tracking} not found!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Shipment not found'}), 404
                try:
                    with db.session.begin():
                        db.session.delete(shipment)
                    send_message(f"🗑 Shipment {tracking} deleted successfully!", chat_id, get_admin_keyboard(), message_id)
                    logger.info(f"Shipment {tracking} deleted via Telegram by chat {chat_id}")
                    return jsonify({'message': 'Shipment deleted'})
                except SQLAlchemyError as e:
                    db.session.rollback()
                    logger.error(f"Database error in Telegram shipment deletion: {e}")
                    send_message(f"💾 Database error!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Database error'}), 500

            elif data == '/admin_update_status':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                send_message("📦 Select a shipment to update status:", chat_id, get_shipment_selection_keyboard('/admin_update_status'), message_id)
                return jsonify({'message': 'Prompted shipment selection'})

            elif data.startswith('select_shipment|/admin_update_status|'):
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                tracking = data.split('|')[-1]
                send_message(f"📋 Select new status for {tracking}:", chat_id, get_status_selection_keyboard(tracking), message_id)
                return jsonify({'message': 'Prompted status selection'})

            elif data.startswith('select_status|/admin_update_status|'):
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                _, _, tracking, status = data.split('|')
                shipment = Shipment.query.filter_by(tracking=tracking).first()
                if not shipment:
                    send_message(f"🔍 Shipment {tracking} not found!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Shipment not found'}), 404
                try:
                    with db.session.begin():
                        shipment.status = status
                        db.session.add(StatusHistory(shipment=shipment, status=status))
                        if shipment.status == ShipmentStatus.DELIVERED.value:
                            shipment.eta = datetime.utcnow()
                    socketio.emit('update', shipment.to_dict(), namespace='/', room=tracking)
                    send_message(f"📋 Status for {tracking} updated to {status}!", chat_id, get_admin_keyboard(), message_id)
                    logger.info(f"Status for {tracking} updated to {status} via Telegram by chat {chat_id}")
                    return jsonify({'message': 'Status updated'})
                except SQLAlchemyError as e:
                    db.session.rollback()
                    logger.error(f"Database error in Telegram status update: {e}")
                    send_message(f"💾 Database error!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Database error'}), 500

            elif data == '/admin_list_subscribers':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                subscribers = Subscriber.query.all()
                if not subscribers:
                    send_message("📩 No subscribers found.", chat_id, get_admin_keyboard(), message_id)
                else:
                    text = "\n".join([f"📩 {s.email or s.phone} for {s.shipment.tracking}" for s in subscribers[:10]])
                    send_message(f"📋 Subscribers:\n{text}", chat_id, get_admin_keyboard(), message_id)
                return jsonify({'message': 'Listed subscribers'})

            elif data == '/admin_unsubscribe':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                admin_sessions[chat_id]['state'] = {'command': '/admin_unsubscribe', 'step': 'tracking_contact'}
                send_message(
                    "📩 Enter tracking number and email/phone to unsubscribe (e.g., TR12345 user@example.com):",
                    chat_id,
                    {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]},
                    message_id
                )
                return jsonify({'message': 'Prompted tracking and contact'})

            elif data == '/admin_pause_sim':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                send_message("🚚 Select a shipment to pause simulation:", chat_id, get_shipment_selection_keyboard('/admin_pause_sim'), message_id)
                return jsonify({'message': 'Prompted shipment selection'})

            elif data.startswith('select_shipment|/admin_pause_sim|'):
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                tracking = data.split('|')[-1]
                shipment = Shipment.query.filter_by(tracking=tracking).first()
                if not shipment:
                    send_message(f"🔍 Shipment {tracking} not found!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Shipment not found'}), 404
                simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
                if not simulation_state or simulation_state.status != 'running':
                    send_message(f"🚫 No active simulation for {tracking}!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'No active simulation'}), 400
                try:
                    with db.session.begin():
                        simulation_state.status = 'paused'
                    send_message(f"⏸ Simulation paused for {tracking}!", chat_id, get_admin_keyboard(), message_id)
                    logger.info(f"Simulation paused for {tracking} via Telegram by chat {chat_id}")
                    return jsonify({'message': 'Simulation paused'})
                except SQLAlchemyError as e:
                    db.session.rollback()
                    logger.error(f"Database error in Telegram pause simulation: {e}")
                    send_message(f"💾 Database error!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Database error'}), 500

            elif data == '/admin_continue_sim':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                send_message("🚚 Select a shipment to continue simulation:", chat_id, get_shipment_selection_keyboard('/admin_continue_sim'), message_id)
                return jsonify({'message': 'Prompted shipment selection'})

            elif data.startswith('select_shipment|/admin_continue_sim|'):
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                tracking = data.split('|')[-1]
                shipment = Shipment.query.filter_by(tracking=tracking).first()
                if not shipment:
                    send_message(f"🔍 Shipment {tracking} not found!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Shipment not found'}), 404
                simulation_state = SimulationState.query.filter_by(shipment_id=shipment.id).first()
                if not simulation_state or simulation_state.status != 'paused':
                    send_message(f"🚫 No paused simulation for {tracking}!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'No paused simulation'}), 400
                try:
                    with db.session.begin():
                        simulation_state.status = 'running'
                    run_simulation_async.delay(shipment.id, simulation_state.num_points, simulation_state.step_hours)
                    send_message(f"▶ Simulation continued for {tracking}!", chat_id, get_admin_keyboard(), message_id)
                    logger.info(f"Simulation continued for {tracking} via Telegram by chat {chat_id}")
                    return jsonify({'message': 'Simulation continued'})
                except SQLAlchemyError as e:
                    db.session.rollback()
                    logger.error(f"Database error in Telegram continue simulation: {e}")
                    send_message(f"💾 Database error!", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Database error'}), 500

            elif data == '/admin_health':
                if not session.get('authenticated') or datetime.utcnow() >= session.get('expires', datetime.utcnow()):
                    send_message("🚫 Please login with /admin_login <password>", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Not authenticated'}), 401
                try:
                    db.session.execute('SELECT 1')
                    celery_status = celery.control.ping(timeout=1)
                    status = {
                        'status': 'healthy',
                        'database': 'connected',
                        'celery': 'responsive' if celery_status else 'unresponsive',
                        'timestamp': datetime.utcnow().isoformat() + 'Z'
                    }
                    send_message(f"🩺 Health Check:\n{json.dumps(status, indent=2)}", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'message': 'Health check displayed'})
                except SQLAlchemyError as e:
                    logger.error(f"Health check failed: {e}")
                    send_message("🩺 Health Check: Database disconnected", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Database error'}), 500
                except Exception as e:
                    logger.error(f"Health check exception: {e}")
                    send_message("🩺 Health Check: Unhealthy", chat_id, get_admin_keyboard(), message_id)
                    return jsonify({'error': 'Unexpected error'}), 500

            elif data == '/admin_logout':
                admin_sessions.pop(chat_id, None)
                send_message("🔐 Logged out successfully!", chat_id, get_navigation_keyboard(), message_id)
                logger.info(f"Admin logged out for chat {chat_id}")
                return jsonify({'message': 'Logged out'})

            elif data == '/cancel':
                admin_sessions.pop(chat_id, None)
                send_message("❌ Operation cancelled.", chat_id, get_navigation_keyboard(), message_id)
                return jsonify({'message': 'Operation cancelled'})

            elif data.startswith('skip_status|/addcp|'):
                if not session.get('state') or session['state']['tracking'] != data.split('|')[-1]:
                    send_message("🚫 Invalid session! Please start over.", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Invalid session'}), 400
                tracking = data.split('|')[-1]
                state = session['state']
                session['state']['step'] = 'photo'
                session['state']['status'] = None
                admin_sessions[chat_id] = session
                send_message(
                    f"📸 Upload a proof photo for {tracking} (or press Skip/Cancel):",
                    chat_id,
                    {'inline_keyboard': [
                        [{'text': 'Skip', 'callback_data': f'skip_photo|/addcp|{tracking}'}],
                        [{'text': 'Cancel', 'callback_data': '/cancel'}]
                    ]},
                    message_id
                )
                return jsonify({'message': 'Prompted photo'})

            elif data.startswith('skip_photo|/addcp|'):
                if not session.get('state') or session['state']['tracking'] != data.split('|')[-1]:
                    send_message("🚫 Invalid session! Please start over.", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Invalid session'}), 400
                tracking = data.split('|')[-1]
                state = session['state']
                location = state['location']
                label = state['label']
                note = state.get('note')
                status = state.get('status')
                session.pop('state', None)
                admin_sessions[chat_id] = session
                shipment = Shipment.query.filter_by(tracking=tracking).first()
                if not shipment:
                    send_message(f"🔍 Shipment {tracking} not found!", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Shipment not found'}), 404
                try:
                    checkpoint_data = {
                        'address': location if ',' not in location else None,
                        'lat': float(location.split(',')[0]) if ',' in location and all(x.replace('.', '', 1).isdigit() for x in location.split(',')) else None,
                        'lng': float(location.split(',')[1]) if ',' in location and all(x.replace('.', '', 1).isdigit() for x in location.split(',')) else None,
                        'label': label,
                        'note': note,
                        'status': status,
                        'proof_photo': None
                    }
                    CheckpointCreate(**checkpoint_data)
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
                        label=label,
                        note=note,
                        status=status,
                        proof_photo=None
                    )
                    with db.session.begin():
                        db.session.add(checkpoint)
                        if status:
                            shipment.status = status
                            db.session.add(StatusHistory(shipment=shipment, status=status))
                            if shipment.status == ShipmentStatus.DELIVERED.value:
                                shipment.eta = checkpoint.timestamp
                    socketio.emit('update', shipment.to_dict(), namespace='/', room=tracking)
                    for subscriber in shipment.subscribers:
                        if subscriber.is_active:
                            if subscriber.email:
                                send_checkpoint_email_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.email)
                            if subscriber.phone:
                                send_checkpoint_sms_async.delay(shipment.to_dict(), checkpoint.to_dict(), subscriber.phone)
                    send_message(
                        f"📍 Checkpoint added to {tracking} successfully! 🔍",
                        chat_id,
                        get_navigation_keyboard(tracking),
                        message_id
                    )
                    logger.info(f"Checkpoint added to {tracking} via Telegram by chat {chat_id} (skipped photo)")
                    return jsonify({'message': 'Checkpoint added'})
                except ValidationError as e:
                    send_message(f"🚫 Error: {str(e)}", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': str(e)}), 400
                except ValueError as e:
                    send_message(f"🚫 Error: {str(e)}", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': str(e)}), 400
                except SQLAlchemyError as e:
                    db.session.rollback()
                    logger.error(f"Database error in Telegram checkpoint creation (skip photo): {e}")
                    send_message(f"💾 Database error!", chat_id, get_navigation_keyboard(), message_id)
                    return jsonify({'error': 'Database error'}), 500

        return jsonify({'message': 'No action taken'})
    except Exception as e:
        logger.error(f"Unexpected error in Telegram webhook: {e}")
        return jsonify({'error': 'Unexpected error'}), 500
