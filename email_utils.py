import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import current_app
from celery import shared_task
from retry import retry
import logging
from datetime import datetime, timedelta

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

@shared_task(bind=True, max_retries=3, retry_backoff=2, retry_jitter=True)
def send_checkpoint_email_async(self, shipment: dict, checkpoint: dict, email: str):
    """
    Asynchronously send an email notification for a shipment checkpoint update using Gmail SMTP.
    """
    try:
        # Convert UTC timestamp to WAT (UTC+1)
        utc_time = datetime.fromisoformat(checkpoint['timestamp'].replace('Z', '+00:00'))
        wat_time = utc_time + timedelta(hours=1)
        wat_time_str = wat_time.strftime("%Y-%m-%d %I:%M:%S %p WAT")

        # Log SMTP configuration for debugging
        logger.debug(f"Sending email with SMTP_HOST={current_app.config['SMTP_HOST']}, SMTP_PORT={current_app.config['SMTP_PORT']}, SMTP_USER={current_app.config['SMTP_USER']}")

        # Create MIME message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Update: Shipment {shipment['tracking']} - {checkpoint['label']}"
        msg['From'] = current_app.config['SMTP_FROM']
        msg['To'] = email

        # Plain text version
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
        text_part = MIMEText(text, 'plain')

        # HTML version with Tailwind CSS
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
        <!-- Header -->
        <div class="bg-blue-600 text-white text-center py-4">
            <img src="{current_app.config['APP_BASE_URL']}/static/logo.png" alt="Courier Logo" class="h-12 mx-auto" style="max-width: 150px;">
            <h1 class="text-2xl font-bold mt-2">Shipment Update</h1>
        </div>
        <!-- Content -->
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
        <!-- Footer -->
        <div class="bg-gray-200 text-gray-600 text-center py-4 text-sm">
            <p>You're receiving this email because you're subscribed to updates for this shipment.</p>
            <p><a href="{current_app.config['APP_BASE_URL']}/unsubscribe/{shipment['tracking']}?email={email}" class="text-blue-600 hover:underline">Unsubscribe</a></p>
            <p>&copy; {datetime.now().year} Courier Tracking Service. All rights reserved.</p>
        </div>
    </div>
</body>
</html>
"""
        html_part = MIMEText(html, 'html')

        # Attach parts
        msg.attach(text_part)
        msg.attach(html_part)

        @retry(tries=3, delay=2, backoff=2, exceptions=(smtplib.SMTPException, smtplib.SMTPServerDisconnected))
        def send_email():
            try:
                # Use SMTP_SSL for Gmail
                with smtplib.SMTP(current_app.config['SMTP_HOST'], current_app.config['SMTP_PORT'], timeout=10) as server:
                    server.starttls()
                    server.login(current_app.config['SMTP_USER'], current_app.config['SMTP_PASS'])
                    server.send_message(msg)
                    logger.info(f"Sent email to {email} for checkpoint {checkpoint['id']} of shipment {shipment['tracking']}")
            except smtplib.SMTPAuthenticationError:
                logger.error(f"SMTP authentication failed for {email}: Invalid credentials")
                raise
            except smtplib.SMTPConnectError:
                logger.error(f"SMTP connection failed for {email}: Server unreachable")
                raise
            except smtplib.SMTPException as e:
                logger.error(f"SMTP error sending email to {email}: {e}")
                raise

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
