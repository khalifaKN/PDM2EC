from utils.logger import get_logger
from config.smtp_config import SMTP_CONFIG
from config.email_config import (
    EMAIL_THREAD)
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import smtplib
import traceback

logger = get_logger("send_except_email")


@staticmethod
def send_error_notification(error_message: str, error_type: str = "Pipeline Error", 
                             exception: Exception = None, context: dict = None,
                             smtp_config=SMTP_CONFIG, email_thread=EMAIL_THREAD):
    """
    Send emergency notification when critical errors occur (database failures, API errors, etc.).
    Can be called from anywhere in the codebase without needing run_id or postgres_connector.
    
    Args:
        error_message: Brief description of the error
        error_type: Type of error (e.g., "Database Connection Error", "API Timeout")
        exception: The exception object (optional)
        context: Additional context dict (e.g., {'run_id': '...', 'step': 'Step 5'})
        smtp_config: SMTP configuration (uses default if not provided)
        email_thread: Email recipients (uses default if not provided)
    """
    try:
        
        # Build error details
        error_details = f"""
        <div style="font-family: Arial, sans-serif; max-width: 800px;">
            <h2 style="color: #d32f2f;">🚨 {error_type}</h2>
            <p style="background-color: #ffebee; padding: 15px; border-left: 4px solid #d32f2f;">
                <strong>Error:</strong> {error_message}
            </p>
        """
        
        if exception:
            error_details += f"""
            <h3>Exception Details:</h3>
            <pre style="background-color: #f5f5f5; padding: 10px; overflow-x: auto;">
            {exception.__class__.__name__}: {str(exception)}
            
            {traceback.format_exc()}
            </pre>
            """
        
        if context:
            error_details += "<h3>Context:</h3><ul>"
            for key, value in context.items():
                error_details += f"<li><strong>{key}:</strong> {value}</li>"
            error_details += "</ul>"
        
        error_details += f"""
            <p style="color: #666; font-size: 12px; margin-top: 30px;">
                Timestamp: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
            </p>
        </div>
        """
        
        # Create email
        msg = MIMEMultipart()
        msg['Subject'] = f"🚨 URGENT: {error_type}"
        msg['From'] = smtp_config['from_email']
        msg['To'] = ", ".join(email_thread['TO'])
        msg['Cc'] = ", ".join(email_thread['CC'])
        
        msg.attach(MIMEText(error_details, 'html'))
        
        recipients = email_thread['TO'] + email_thread['CC']
        
        with smtplib.SMTP(smtp_config['host'], smtp_config['port']) as server:
            if smtp_config.get('use_tls'):
                server.starttls()
            server.sendmail(smtp_config['from_email'], recipients, msg.as_string())
        
        logger.info(f"Emergency error notification sent: {error_type}")
        
    except Exception as e:
        logger.error(f"Failed to send error notification: {e}")
