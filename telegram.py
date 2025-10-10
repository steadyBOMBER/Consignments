import json
from flask import request, current_app, jsonify
from app import app, db, Shipment, Subscriber, SimulationState, logger, socketio, admin_sessions, celery
import requests
from datetime import datetime, timedelta

@app.route('/telegram/webhook', methods=['POST'])
def telegram_webhook():
    try:
        update = request.get_json(force=True) or {}
        logger.debug(f"Got Telegram update: {json.dumps(update, indent=2)}")

        message = update.get('message', {})
        callback_query = update.get('callback_query', {})

        def send_message(text, chat_id, reply_markup=None, edit_message_id=None):
            try:
                payload = {'chat_id': chat_id, 'text': text, 'parse_mode': 'HTML'}
                if reply_markup:
                    payload['reply_markup'] = reply_markup
                if edit_message_id:
                    payload['message_id'] = edit_message_id
                    response = requests.post(
                        f"https://api.telegram.org/bot{current_app.config['TELEGRAM_TOKEN']}/editMessageText",
                        json=payload,
                        timeout=5
                    )
                else:
                    response = requests.post(
                        f"https://api.telegram.org/bot{current_app.config['TELEGRAM_TOKEN']}/sendMessage",
                        json=payload,
                        timeout=5
                    )
                response.raise_for_status()
                logger.info(f"Sent {'edited' if edit_message_id else 'new'} message to chat {chat_id}: '{text}'")
            except requests.RequestException as e:
                logger.error(f"Telegram message to {chat_id} failed: {e}")

        def get_navigation_keyboard(tracking=None):
            buttons = [
                [{'text': 'Create Shipment', 'callback_data': '/create'}],
                [{'text': 'Subscribe', 'callback_data': '/subscribe'}],
                [{'text': 'Add Checkpoint', 'callback_data': '/addcp'}],
                [{'text': 'Simulate', 'callback_data': '/simulate'}],
                [{'text': 'Track Multiple', 'callback_data': '/track_multiple'}]
            ]
            session = admin_sessions.get(chat_id)
            if session and session.get('authenticated') and datetime.utcnow() < session['expires']:
                buttons.insert(0, [{'text': 'Admin Panel', 'callback_data': '/admin_menu'}])
            if tracking:
                buttons.append([{'text': f'Track {tracking}', 'url': f'{current_app.config["APP_BASE_URL"]}/track/{tracking}'}])
            return {'inline_keyboard': buttons}

        def get_admin_keyboard():
            return {'inline_keyboard': [
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
            ]}

        def get_shipment_selection_keyboard(command, prefix="select_shipment"):
            shipments = Shipment.query.all()
            buttons = [
                [{'text': f"{s.tracking}: {s.title}", 'callback_data': f"{prefix}|{command}|{s.tracking}"}]
                for s in shipments[:10]
            ]
            buttons.append([{'text': 'Back to Admin Menu', 'callback_data': '/admin_menu'}])
            return {'inline_keyboard': buttons}

        def get_status_selection_keyboard(tracking):
            buttons = [
                [{'text': status.value, 'callback_data': f"select_status|/admin_update_status|{tracking}|{status.value}"}]
                for status in ShipmentStatus
            ]
            buttons.append([{'text': 'Back to Admin Menu', 'callback_data': '/admin_menu'}])
            return {'inline_keyboard': buttons}

        chat_id = message.get('chat', {}).get('id') if message else callback_query.get('message', {}).get('chat', {}).get('id')
        if not chat_id:
            return jsonify({'error': 'No chat ID found'}), 400

        if message and message.get('text'):
            text = message['text'].strip()
            if text.startswith('/start'):
                send_message("📦 Welcome to Courier Tracking! Use the buttons below to manage shipments. 🔍", chat_id, get_navigation_keyboard())
                return jsonify({'message': 'Welcome sent'})
            elif text.startswith('/admin_login'):
                parts = text.split()
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
            elif text and 'state' in admin_sessions.get(chat_id, {}):
                session = admin_sessions[chat_id]
                state = session['state']
                if state['step'] == 'tracking':
                    tracking = bleach.clean(text)
                    if state['command'] == '/create':
                        session['state']['step'] = 'title'
                        session['state']['tracking'] = tracking
                        send_message(f"📦 Enter title for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    elif state['command'] == '/subscribe':
                        session['state']['step'] = 'contact'
                        session['state']['tracking'] = tracking
                        send_message(f"📩 Enter email or phone for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    elif state['command'] == '/addcp':
                        session['state']['step'] = 'location'
                        session['state']['tracking'] = tracking
                        send_message(f"📍 Enter location (lat,lng or address) for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    elif state['command'] == '/simulate':
                        session['state']['step'] = 'num_points'
                        session['state']['tracking'] = tracking
                        send_message(f"🚚 Enter number of points (2-10) for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    elif state['command'] == '/admin_create_shipment':
                        session['state']['step'] = 'title'
                        session['state']['tracking'] = tracking
                        send_message(f"📦 Enter title for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted next step'})
                elif state['step'] == 'title':
                    title = bleach.clean(text)
                    tracking = state['tracking']
                    if state['command'] == '/create' or state['command'] == '/admin_create_shipment':
                        session['state']['step'] = 'origin'
                        session['state']['title'] = title
                        send_message(f"📍 Enter origin (lat,lng or address) for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted origin'})
                elif state['step'] == 'origin':
                    origin = bleach.clean(text)
                    tracking = state['tracking']
                    title = state['title']
                    session['state']['step'] = 'destination'
                    session['state']['origin'] = origin
                    send_message(f"📍 Enter destination (lat,lng or address) for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted destination'})
                elif state['step'] == 'destination':
                    destination = bleach.clean(text)
                    tracking = state['tracking']
                    title = state['title']
                    origin = state['origin']
                    session['state']['step'] = 'status'
                    session['state']['destination'] = destination
                    send_message(f"📋 Enter status (Created, In Transit, Out for Delivery, Delivered) for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted status'})
                elif state['step'] == 'status':
                    status = bleach.clean(text)
                    tracking = state['tracking']
                    title = state['title']
                    origin = state['origin']
                    destination = state['destination']
                    if status not in [s.value for s in ShipmentStatus]:
                        send_message(f"🚫 Invalid status! Use: {', '.join(s.value for s in ShipmentStatus)} (or press Cancel)", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                        return jsonify({'error': 'Invalid status'})
                    session.pop('state', None)
                    admin_sessions[chat_id] = session
                    data = {
                        'tracking_number': tracking,
                        'title': title,
                        'origin': origin,
                        'destination': destination,
                        'status': status
                    }
                    try:
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
                        send_message(f"📦 Shipment {tracking} created successfully! 🔍", chat_id, get_navigation_keyboard(tracking))
                        logger.info(f"Shipment {tracking} created via Telegram by chat {chat_id}")
                        return jsonify({'message': 'Shipment created'})
                    except ValidationError as e:
                        send_message(f"🚫 Error: {str(e)} (or press Cancel)", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                        return jsonify({'error': str(e)}), 400
                    except ValueError as e:
                        send_message(f"🚫 Error: {str(e)} (or press Cancel)", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                        return jsonify({'error': str(e)}), 400
                    except IntegrityError:
                        db.session.rollback()
                        send_message(f"🚫 Tracking {tracking} already exists! (or press Cancel)", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                        return jsonify({'error': 'Tracking number already exists'}), 409
                    except SQLAlchemyError as e:
                        db.session.rollback()
                        logger.error(f"Database error in Telegram shipment creation: {e}")
                        send_message(f"💾 Database error! (or press Cancel)", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                        return jsonify({'error': 'Database error'}), 500
                elif state['step'] == 'contact':
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
                        send_message(f"🚫 Email or phone required for {tracking} (or press Cancel)", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                        return jsonify({'error': 'Email or phone required'}), 400
                    shipment = Shipment.query.filter_by(tracking=tracking).first()
                    if not shipment:
                        send_message(f"🔍 Shipment {tracking} not found! (or press Cancel)", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                        return jsonify({'error': 'Shipment not found'}), 404
                    subscriber = Subscriber(shipment_id=shipment.id, email=email, phone=phone)
                    try:
                        with db.session.begin():
                            db.session.add(subscriber)
                        send_message(f"📩 Subscribed {contact} to {tracking} successfully! 🔍", chat_id, get_navigation_keyboard(tracking))
                        logger.info(f"Subscribed {contact} to {tracking} via Telegram by chat {chat_id}")
                        return jsonify({'message': 'Subscribed'})
                    except IntegrityError:
                        db.session.rollback()
                        send_message(f"🚫 Already subscribed {contact} to {tracking}! (or press Cancel)", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                        return jsonify({'error': 'Already subscribed'}), 409
                    except SQLAlchemyError as e:
                        db.session.rollback()
                        logger.error(f"Database error in Telegram subscription: {e}")
                        send_message(f"💾 Database error! (or press Cancel)", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                        return jsonify({'error': 'Database error'}), 500
                elif state['step'] == 'location':
                    location = bleach.clean(text)
                    tracking = state['tracking']
                    session['state']['step'] = 'label'
                    session['state']['location'] = location
                    send_message(f"📍 Enter label for {tracking} (or press Cancel):", chat_id, {'inline_keyboard': [[{'text': 'Cancel', 'callback_data': '/cancel'}]]})
                    admin_sessions[chat_id] = session
                    return jsonify({'message': 'Prompted label'})
                elif state['step'] == 'label':
                    label = bleach.clean(text)
                    tracking = state['tracking']
                    location = state['location']
                    session['state']['step'] = 'note'
                    session['state']['label'] = label
                    send_message(f"📝 Enter note for {tracking} (optional, or press Cancel):", chat_id, {'inline_keyboard
