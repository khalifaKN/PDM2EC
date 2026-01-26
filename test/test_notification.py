"""
Test script for NotificationHandler
Demonstrates how to send HTML email notifications with pipeline history from database.

Usage:
    python -m test.test_notification <run_id>
    
Example:
    python -m test.test_notification c8df81d7-409e-4fc7-962f-5676064ead80
"""

import sys
from orchestrator.notification_processing import NotificationHandler
from db.psycopg2_connection import Psycopg2DatabaseConnection
from config.db import postgres_url
from utils.logger import get_logger

logger = get_logger('test_notification')


def test_notification(run_id: str):
    """
    Tests notification system by sending email for a specific pipeline run.
    
    Args:
        run_id: UUID of the pipeline run to generate notification for
    """
    logger.info(f"Testing notification for run_id: {run_id}")
    
    # Initialize database connector
    postgres_connector = Psycopg2DatabaseConnection(postgres_url)
    
    # Create notification handler
    handler = NotificationHandler(
        run_id=run_id,
        postgres_connector=postgres_connector
    )
    
    # Send notification (fetches history from DB automatically)
    handler.send_pipeline_notification()
    
    logger.info("Notification test completed")


def preview_html(run_id: str):
    """
    Generates HTML email body and saves to file for preview (doesn't send email).
    
    Args:
        run_id: UUID of the pipeline run
    """
    logger.info(f"Generating HTML preview for run_id: {run_id}")
    
    postgres_connector = Psycopg2DatabaseConnection(postgres_url)
    handler = NotificationHandler(
        run_id=run_id,
        postgres_connector=postgres_connector
    )
    
    # Generate HTML
    html_body = handler._prepare_email_body()
    
    # Save to file
    output_file = f"./test/test_output/notification_preview_{run_id}.html"
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html_body)
    
    logger.info(f"HTML preview saved to: {output_file}")
    print(f"\n✓ HTML preview saved to: {output_file}")
    print("Open this file in your browser to preview the email.\n")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python -m test.test_notification <run_id> [--preview]")
        print("Example: python -m test.test_notification c8df81d7-409e-4fc7-962f-5676064ead80 --preview")
        sys.exit(1)
    
    run_id = sys.argv[1]
    preview_mode = "--preview" in sys.argv
    
    if preview_mode:
        preview_html(run_id)
    else:
        print("\n⚠️  WARNING: This will send an actual email!")
        confirm = input("Continue? (yes/no): ")
        if confirm.lower() == 'yes':
            test_notification(run_id)
        else:
            print("Cancelled.")
