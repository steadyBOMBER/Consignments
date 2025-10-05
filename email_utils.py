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

@shared_task(bind=True, max_retries=3, retry_backoff=True)
def send_checkpoint_email_async(self, shipment: dict, checkpoint: dict, email: str):
    try:
        # Convert UTC timestamp to WAT (UTC+1) for display
        utc_time = datetime.fromisoformat(checkpoint['timestamp'].replace('Z', '+00:00'))
        wat_time = utc_time + timedelta(hours=1)
        wat_time_str = wat_time.strftime("%Y-%m-%d %H:%M:%S WAT")

        msg = MIMEMultipart('alternative')
        msg['Subject'] = f"Update: Shipment {shipment['tracking']}"
        msg['From'] = current_app.config['SMTP_FROM']
        msg['To'] = email

        # Plain text email
        text = f"""Courier Tracking Update

Shipment: {shipment['title']} ({shipment['tracking']})
Status: {shipment['status']}
Updated: {wat_time_str}

Latest Checkpoint:
Label: {checkpoint['label']}
Location: ({checkpoint['lat']:.4f}, {checkpoint['lng']:.4f})
Time: {wat_time_str}
Note: {checkpoint['note'] or 'None'}

Track your shipment: {current_app.config['APP_BASE_URL']}/track/{shipment['tracking']}

You're receiving this email because you subscribed to updates for this shipment.
© {datetime.now().year} Courier Tracking Service
"""

        # HTML email with optimized design
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Shipment Update</title>
</head>
<body style="margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Arial, sans-serif; background-color: #f4f4f4;">
    <table role="presentation" style="width: 100%; max-width: 600px; margin: 20px auto; border-collapse: collapse; background-color: #ffffff; box-shadow: 0 2px 4px rgba(0,0,0,0.1);">
        <tr>
            <td style="background-color: #007bff; padding: 20px; text-align: center; color: #ffffff;">
                <h1 style="margin: 0; font-size: 24px; font-weight: 600;">Courier Tracking Update</h1>
            </td>
        </tr>
        <tr>
            <td style="padding: 20px;">
                <h2 style="font-size: 20px; margin: 0 0 10px; color: #333333;">Shipment: {shipment['title']} ({shipment['tracking']})</h2>
                <p style="margin: 5px 0; color: #555555;"><strong>Status:</strong> {shipment['status']}</p>
                <p style="margin: 5px 0; color: #555555;"><strong>Updated:</strong> {wat_time_str}</p>
                <h3 style="font-size: 18px; margin: 20px 0 10px; color: #333333;">Latest Checkpoint</h3>
                <p style="margin: 5px 0; color: #555555;"><strong>Label:</strong> {checkpoint['label']}</p>
                <p style="margin: 5px 0; color: #555555;"><strong>Location:</strong> ({checkpoint['lat']:.4f}, {checkpoint['lng']:.4f})</p>
                <p style="margin: 5px 0; color: #555555;"><strong>Time:</strong> {wat_time_str}</p>
                <p style="margin: 5px 0; color: #555555;"><strong>Note:</strong> {checkpoint['note'] or 'None'}</p>
                <p style="margin: 20px 0; text-align: center;">
                    <a href="{current_app.config['APP_BASE_URL']}/track/{shipment['tracking']}" style="display: inline-block; padding: 12px 24px; background-color: #007bff; color: #ffffff; text-decoration: none; border-radius: 4px; font-weight: 600;" role="button" aria-label="Track your shipment">Track Shipment</a>
                </p>
            </td>
        </tr>
        <tr>
            <td style="background-color: #f4f4f4; padding: 15px; text-align: center; font-size: 12px; color: #777777;">
                <p style="margin: 0;">You're receiving this email because you subscribed to updates for this shipment.</p>
                <p style="margin: 5px 0;">© {datetime.now().year} Courier Tracking Service</p>
            </td>
        </tr>
    </table>
</body>
</html>
"""

        text_part = MIMEText(text, 'plain')
        html_part = MIMEText(html, 'html')
        msg.attach(text_part)
        msg.attach(html_part)

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
