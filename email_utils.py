import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import current_app
from celery import shared_task
from retry import retry
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

@shared_task(bind=True, max_retries=3, retry_backoff=True)
def send_checkpoint_email_async(self, shipment: dict, checkpoint: dict, email: str):
    """
    Asynchronously send an email notification for a checkpoint update.
    
    Args:
        shipment (dict): Shipment details (id, tracking, title, status, origin_lat, origin_lng, dest_lat, dest_lng, updated_at).
        checkpoint (dict): Checkpoint details (id, shipment_id, position, lat, lng, label, note, timestamp).
        email (str): Recipient email address.
    """
    try:
        # Construct email
        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Shipment Update: {shipment['tracking']}"
        msg['From'] = current_app.config['SMTP_FROM']
        msg['To'] = email

        # Plain text version
        text = f"""
Courier Tracking Update

Shipment: {shipment['title']} ({shipment['tracking']})
Status: {shipment['status']}
Updated: {shipment['updated_at']}

Latest Checkpoint:
- Label: {checkpoint['label']}
- Location: ({checkpoint['lat']:.4f}, {checkpoint['lng']:.4f})
- Time: {checkpoint['timestamp']}
- Note: {checkpoint['note'] or 'None'}

Track: {current_app.config['APP_BASE_URL']}/track/{shipment['tracking']}
"""
        text_part = MIMEText(text, 'plain')

        # HTML version
        html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, sans-serif; color: #333; line-height: 1.6; }}
        .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
        .header {{ background: #007bff; color: white; padding: 10px; text-align: center; }}
        .content {{ padding: 20px; border: 1px solid #ddd; border-radius: 5px; }}
        .button {{ display: inline-block; padding: 10px 20px; background: #007bff; color: white; text-decoration: none; border-radius: 5px; }}
        .footer {{ font-size: 12px; color: #777; text-align: center; margin-top: 20px; }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h2>Courier Tracking Update</h2>
        </div>
        <div class="content">
            <h3>Shipment: {shipment['title']} ({shipment['tracking']})</h3>
            <p><strong>Status:</strong> {shipment['status']}</p>
            <p><strong>Updated:</strong> {shipment['updated_at']}</p>
            <h4>Latest Checkpoint</h4>
            <p><strong>Label:</strong> {checkpoint['label']}</p>
            <p><strong>Location:</strong> ({checkpoint['lat']:.4f}, {checkpoint['lng']:.4f})</p>
            <p><strong>Time:</strong> {checkpoint['timestamp']}</p>
            <p><strong>Note:</strong> {checkpoint['note'] or 'None'}</p>
            <p>
                <a href="{current_app.config['APP_BASE_URL']}/track/{shipment['tracking']}" class="button">
                    Track Shipment
                </a>
            </p>
        </div>
        <div class="footer">
            <p>You're receiving this email because you're subscribed to updates for this shipment.</p>
            <p>&copy; {datetime.now().year} Courier Tracking Service</p>
        </div>
    </div>
</body>
</html>
"""
        html_part = MIMEText(html, 'html')

        # Attach both parts
        msg.attach(text_part)
        msg.attach(html_part)

        # Send email with retry
        @retry(tries=3, delay=2, backoff=2, exceptions=(smtplib.SMTPException,))
        def send_email():
            with smtplib.SMTP(current_app.config['SMTP_HOST'], current_app.config['SMTP_PORT']) as server:
                server.starttls()
                server.login(current_app.config['SMTP_USER'], current_app.config['SMTP_PASS'])
                server.send_message(msg)
                logger.info(f"Sent email to {email} for checkpoint {checkpoint['id']} of shipment {shipment['tracking']}")

        send_email()
    except smtplib.SMTPException as e:
        logger.error(f"Failed to send email to {email} for checkpoint {checkpoint['id']}: {e}")
        raise self.retry(exc=e)
    except Exception as e:
        logger.error(f"Unexpected error sending email to {email} for checkpoint {checkpoint['id']}: {e}")
        raise
