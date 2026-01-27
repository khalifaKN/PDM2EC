from utils.logger import get_logger
from config.smtp_config import SMTP_CONFIG
from config.email_config import (
    EMAIL_THREAD,
    EMAIL_BODY_SECTION_TEMPLATE,
    EMAIL_BODY_TEMPLATE,
    EMAIL_TABLE_CELL_TEMPLATE,
    EMAIL_TABLE_HEADER_TEMPLATE,
    EMAIL_TABLE_ROW_TEMPLATE,
    EMAIL_BODY_SUMMARY_TEMPLATE,
    SUMMARY_TABLE_ROW_TEMPLATE,
)
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from datetime import datetime
import json
import os
import smtplib


logger = get_logger("notification_handler")


class NotificationHandler:
    """
    A class to handle sending notification emails with pipeline execution history.
    Pulls data from database history tables: pipeline_run_summary, user_sync_results, employee_field_changes_batches.
    """

    def __init__(
        self,
        run_id: str,
        postgres_connector,
        table_names: dict,
        smtp_config=SMTP_CONFIG,
        email_thread=EMAIL_THREAD,
        output_dir="./test/test_output",
    ):
        self.smtp_config = smtp_config
        self.output_dir = output_dir
        self.email_thread = email_thread
        self.run_id = run_id
        self.postgres_connector = postgres_connector
        self.table_names = table_names
        self.run_summary = None
        self.results = []
        self.batches = []
        self.field_changes = []  # Detailed field changes with old/new values

    def _fetch_run_summary(self):
        """Fetches pipeline run summary from database."""
        query = f"""
            SELECT run_id, started_at, finished_at, status, total_records,
                   created_count, updated_count, terminated_count, 
                   failed_count, warning_count, error_message
            FROM {self.table_names["pipeline_run_summary"]}
            WHERE run_id = %s
        """
        connection = None
        cursor = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            cursor.execute(query, (self.run_id,))
            row = cursor.fetchone()
            if row:
                self.run_summary = {
                    "run_id": row[0],
                    "started_at": row[1],
                    "finished_at": row[2],
                    "status": row[3],
                    "total_records": row[4],
                    "created_count": row[5],
                    "updated_count": row[6],
                    "terminated_count": row[7],
                    "failed_count": row[8],
                    "warning_count": row[9],
                    "error_message": row[10],
                }
        except Exception as e:
            logger.error(f"Failed to fetch run summary: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _fetch_results(self):
        """Fetches user sync results (successes, failures, and warnings) from database."""
        query = f"""
            SELECT user_id, operation, status, error_message, 
                   warning_message, success_message, payload_snapshot, created_at, success_entities, failed_entities, skipped_entities
            FROM {self.table_names["user_sync_results"]}
            WHERE run_id = %s
            ORDER BY created_at DESC
        """
        connection = None
        cursor = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            cursor.execute(query, (self.run_id,))
            rows = cursor.fetchall()
            for row in rows:
                self.results.append(
                    {
                        "user_id": row[0],
                        "operation": row[1],
                        "status": row[2],
                        "error_message": row[3],
                        "warning_message": row[4],
                        "success_message": row[5],
                        "payload_snapshot": json.loads(row[6])
                        if row[6] and isinstance(row[6], str)
                        else row[6],
                        "created_at": row[7],
                        "success_entities": json.loads(row[8])
                        if row[8] and isinstance(row[8], str)
                        else row[8],
                        "failed_entities": json.loads(row[9])
                        if row[9] and isinstance(row[9], str)
                        else row[9],
                        "skipped_entities": json.loads(row[10])
                        if row[10] and isinstance(row[10], str)
                        else row[10],
                    }
                )
        except Exception as e:
            logger.error(f"Failed to fetch results: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _fetch_entities_from_results(
        self,
    ) -> dict:
        """Fetches entities status (failed and successful) from user sync results."""
        entities_status = {}
        for record in self.results:
            user_id = record.get("user_id")
            entities_status[user_id] = {
                "failed_entities": record.get("failed_entities", []),
                "successful_entities": record.get("success_entities", []),
                "skipped_entities": record.get("skipped_entities", []),
            }
        return entities_status

    def _fetch_batches(self):
        """Fetches field change batches with context from database."""
        query = f"""
            SELECT batch_id, batch_context, started_at, finished_at, 
                   status, total_users, users_with_changes
            FROM {self.table_names["employee_field_changes_batches"]}
            WHERE run_id = %s
            ORDER BY started_at
        """
        connection = None
        cursor = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            cursor.execute(query, (self.run_id,))
            rows = cursor.fetchall()
            for row in rows:
                self.batches.append(
                    {
                        "batch_id": row[0],
                        "batch_context": row[1],
                        "started_at": row[2],
                        "finished_at": row[3],
                        "status": row[4],
                        "total_users": row[5],
                        "users_with_changes": row[6],
                    }
                )
        except Exception as e:
            logger.error(f"Failed to fetch batches: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def _fetch_updated_users_with_warnings(self) -> set:
        """Fetches user IDs of users who were updated with warnings."""
        query = f"""
            SELECT DISTINCT user_id
            FROM {self.table_names["user_sync_results"]}
            WHERE run_id = %s AND operation = 'UPDATE' AND status = 'WARNING'
        """
        connection = None
        cursor = None
        updated_users_with_warnings = set()
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            cursor.execute(query, (self.run_id,))
            rows = cursor.fetchall()
            updated_users_with_warnings = {row[0] for row in rows}
        except Exception as e:
            logger.error(f"Failed to fetch updated users with warnings: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()
        return updated_users_with_warnings

    def _fetch_field_changes(self, limit=100):
        """Fetches detailed field changes from employee_field_changes table.

        Args:
            limit: Maximum number of field changes to fetch (default 100)
        """
        query = f"""
            SELECT fc.batch_id, fc.userid, fc.field_name, fc.ec_value, 
                   fc.pdm_value, fc.detected_at, b.batch_context
            FROM {self.table_names["employee_field_changes"]} fc
            JOIN {self.table_names["employee_field_changes_batches"]} b ON fc.batch_id = b.batch_id
            WHERE b.run_id = %s
            ORDER BY b.batch_context, fc.userid, fc.detected_at DESC
            LIMIT %s
        """
        connection = None
        cursor = None
        try:
            connection = self.postgres_connector.get_postgres_db_connection()
            cursor = connection.cursor()
            cursor.execute(query, (self.run_id, limit))
            rows = cursor.fetchall()
            for row in rows:
                self.field_changes.append(
                    {
                        "batch_id": row[0],
                        "user_id": row[1],
                        "field_name": row[2],
                        "ec_value": row[3],
                        "pdm_value": row[4],
                        "detected_at": row[5],
                        "batch_context": row[6],
                    }
                )
        except Exception as e:
            logger.error(f"Failed to fetch field changes: {e}")
        finally:
            if cursor:
                cursor.close()
            if connection:
                connection.close()

    def send_notification(self, body: str, attachment_paths: list = None):
        """
        Sends a notification email with the specified body and optional attachments.

        Args:
            body (str): The body content of the email.
            attachment_paths (list): List of file paths to attach (optional).
        """
        try:
            # Create multipart message to support attachments
            msg = MIMEMultipart()
            msg["Subject"] = self.email_thread["SUBJECT"]
            msg["From"] = self.smtp_config["from_email"]
            msg["To"] = ", ".join(self.email_thread["TO"])
            msg["Cc"] = ", ".join(self.email_thread["CC"])

            # Attach HTML body
            msg.attach(MIMEText(body, "html"))

            # Attach files if provided
            if attachment_paths:
                for attachment_path in attachment_paths:
                    if attachment_path and os.path.exists(attachment_path):
                        filename = os.path.basename(attachment_path)

                        # Determine MIME type based on file extension
                        if filename.endswith(".json"):
                            mime_type = "application"
                            mime_subtype = "json"
                        elif filename.endswith(".html"):
                            mime_type = "text"
                            mime_subtype = "html"
                        else:
                            mime_type = "application"
                            mime_subtype = "octet-stream"

                        with open(attachment_path, "rb") as f:
                            part = MIMEBase(mime_type, mime_subtype)
                            part.set_payload(f.read())
                            encoders.encode_base64(part)
                            part.add_header(
                                "Content-Disposition",
                                f'attachment; filename="{filename}"',
                            )
                            msg.attach(part)
                        logger.info(f"Attached file: {filename}")

            try:
                with open(
                    f"logs/pdm2ec_log_{datetime.now().strftime('%Y-%m-%d_%H')}.log",
                    "rb",
                ) as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header(
                        "Content-Disposition",
                        f'attachment; filename="pdm2ec_log_{datetime.now().strftime("%Y-%m-%d_%H")}.log"',
                    )
                    msg.attach(part)
                    logger.info(
                        f"Attached log file: pdm2ec_log_{datetime.now().strftime('%Y-%m-%d_%H')}.log"
                    )
            except FileNotFoundError:
                logger.warning("Log file not found for attachment")

            recipients = self.email_thread["TO"] + self.email_thread["CC"]

            with smtplib.SMTP(
                self.smtp_config["host"], self.smtp_config["port"]
            ) as server:
                if self.smtp_config.get("use_tls"):
                    server.starttls()
                server.sendmail(
                    self.smtp_config["from_email"], recipients, msg.as_string()
                )

            logger.info(f"Notification email sent successfully for run {self.run_id}")
        except Exception as e:
            logger.error(f"Failed to send notification email: {e}")

    def send_pipeline_notification(self):
        """
        Main entry point: Fetches history from DB and sends HTML notification email.
        """
        try:
            body, attachment_files = self._prepare_email_body()
            self.send_notification(body, attachment_paths=attachment_files)
            logger.info(
                f"Pipeline notification prepared and sent for run {self.run_id}"
            )
        except Exception as e:
            logger.error(f"Failed to prepare/send pipeline notification: {e}")

    def _save_detailed_report(self, sections: list) -> str:
        """
        Saves detailed HTML report with all sections to a file.
        Used when email body is too large for inline display.

        Args:
            sections (list): List of HTML section strings

        Returns:
            str: File path to the saved report
        """

        os.makedirs(self.output_dir, exist_ok=True)

        # Combine sections into full HTML document
        content = "\\n".join(sections)
        full_html = EMAIL_BODY_TEMPLATE.format(content_sections=content)

        # Save to file
        filename = f"detailed_report_{self.run_id}.html"
        filepath = os.path.join(self.output_dir, filename)

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(full_html)
            logger.info(f"Detailed report saved to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save detailed report: {e}")
            return None

    def _save_entities_status_to_file(self) -> str:
        """
        Saves entities status (failed or successful) to a beautified JSON file.
        Returns the file path.

        Args:
            entities_status (dict): Dictionary of entities and their statuses
            status_type (str): Type of status ('failed' or 'successful')

        Returns:
            str: File path to the saved JSON file
        """
        data = self._fetch_entities_from_results()

        os.makedirs(self.output_dir, exist_ok=True)
        filename = f"entities_status_{self.run_id}.json"
        filepath = os.path.join(self.output_dir, filename)
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logger.info(f"Entities status saved to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save entities status file: {e}")
            return None

    def _save_payloads_to_file(self) -> str:
        """
        Saves all results payloads to a beautified JSON file.
        Returns the file path.
        """
        if not self.results:
            return None

        # Create output directory if not exists

        os.makedirs(self.output_dir, exist_ok=True)

        # Build payload data structure
        payload_data = {
            "run_id": self.run_id,
            "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "total_failures": len([f for f in self.results if f["status"] == "FAILED"]),
            "total_warnings": len(
                [f for f in self.results if f["status"] == "WARNING"]
            ),
            "total_successes": len(
                [f for f in self.results if f["status"] == "SUCCESS"]
            ),
            "results": [],
        }

        for result in self.results:
            if result.get("payload_snapshot"):
                payload_data["results"].append(
                    {
                        "user_id": result["user_id"],
                        "operation": result["operation"],
                        "status": result["status"],
                        "error_message": result.get("error_message"),
                        "warning_message": result.get("warning_message"),
                        "success_message": result.get("success_message"),
                        "is_scm": result.get("is_scm"),
                        "is_im": result.get("is_im"),
                        "payloads": result["payload_snapshot"],
                        "timestamp": result["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                        if result["created_at"]
                        else None,
                    }
                )

        # Save to file
        filename = f"payloads_{self.run_id}.json"
        filepath = os.path.join(self.output_dir, filename)

        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(payload_data, f, indent=2, ensure_ascii=False)
            logger.info(f"Payloads saved to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Failed to save payloads file: {e}")
            return None

    def _prepare_email_body(self):
        """
        Prepares comprehensive HTML email body from database history.

        Returns:
            tuple: (email_body: str, attachment_files: list)
        """
        # Reset previously cached data on the handler to avoid duplication
        # when this method is called multiple times on the same instance
        self.run_summary = None
        self.results = []
        self.batches = []
        self.field_changes = []

        # Fetch all data from database
        self._fetch_run_summary()
        self._fetch_results()
        self._fetch_batches()
        self._fetch_field_changes()

        # Collect attachment files
        attachment_files = []

        # Save payloads to file if there are failures/warnings
        if self.results:
            payloads_file = self._save_payloads_to_file()
            entities_status_file = self._save_entities_status_to_file()
            if payloads_file:
                attachment_files.append(payloads_file)
            if entities_status_file:
                attachment_files.append(entities_status_file)

        if not self.run_summary:
            return (
                "<html><body><h2>Error: Pipeline run not found in database</h2></body></html>",
                [],
            )

        # Build content sections
        sections = []

        # 1. Pipeline Run Overview
        overview_section = self._build_overview_section()
        sections.append(overview_section)

        # 2. Batch Processing Summary
        if self.batches:
            batch_section = self._build_batch_section()
            sections.append(batch_section)

        # 3. Field Changes Details
        if self.field_changes:
            field_changes_section = self._build_field_changes_section()
            sections.append(field_changes_section)

        # 4. Failures , Warnings and Successes
        if self.results:
            failures_section = self._build_failures_section()
            sections.append(failures_section)

        # Use detailed template if data fits, otherwise summary with attached report
        if len(self.results) > 100:  # Threshold for "too large"
            # Save detailed report to file
            report_file = self._save_detailed_report(sections)
            # Save entities status to file
            if report_file:
                attachment_files.append(report_file)

            # Return summary email body
            return self._build_summary_email(), attachment_files
        else:
            # Combine all sections for inline email
            content = "\\n".join(sections)
            return EMAIL_BODY_TEMPLATE.format(
                content_sections=content
            ), attachment_files

    def _build_overview_section(self) -> str:
        """Builds pipeline run overview HTML section."""
        duration = "N/A"
        if self.run_summary["started_at"] and self.run_summary["finished_at"]:
            delta = self.run_summary["finished_at"] - self.run_summary["started_at"]
            duration = str(delta).split(".")[0]  # Remove microseconds

        headers = "".join(
            [EMAIL_TABLE_HEADER_TEMPLATE.format(header=h) for h in ["Metric", "Value"]]
        )

        # Calculate created with/without warnings
        created_with_warnings = len(
            {
                f["user_id"]
                for f in self.results
                if f["operation"] == "CREATE" and f["status"] == "WARNING"
            }
        )
        created_without_warnings = max(
            0, (self.run_summary["created_count"] or 0) - created_with_warnings
        )

        # Get existing employee metrics from batches
        total_existing_employees = sum(b.get("total_users", 0) for b in self.batches)
        users_with_changes = sum(b.get("users_with_changes", 0) for b in self.batches)

        metrics = [
            ("Run ID", self.run_summary["run_id"]),
            (
                "Status",
                f"<span style='color: {'green' if self.run_summary['status'] == 'SUCCESS' else 'red'};'><b>{self.run_summary['status']}</b></span>",
            ),
            (
                "Started At",
                self.run_summary["started_at"].strftime("%Y-%m-%d %H:%M:%S")
                if self.run_summary["started_at"]
                else "N/A",
            ),
            (
                "Finished At",
                self.run_summary["finished_at"].strftime("%Y-%m-%d %H:%M:%S")
                if self.run_summary["finished_at"]
                else "N/A",
            ),
            ("Duration", duration),
            ("Total Records Processed", self.run_summary["total_records"]),
            ("━━━ New Employees ━━━", "<b></b>"),
            (
                "  • Created (Success)",
                f"<span style='color: green;'><b>{created_without_warnings}</b></span>",
            ),
            (
                "  • Created (With Warnings)",
                f"<span style='color: orange;'><b>{created_with_warnings}</b></span>"
                if created_with_warnings > 0
                else "0",
            ),
            ("  • Total Created", f"<b>{self.run_summary['created_count']}</b>"),
            ("━━━ Existing Employees ━━━", "<b></b>"),
            ("  • Total Existing Users", f"<b>{total_existing_employees}</b>"),
            ("  • Users with Changes", f"<b>{users_with_changes}</b>"),
            (
                "  • Updated Successfully",
                f"<span style='color: green;'><b>{self.run_summary['updated_count'] - len({f['user_id'] for f in self.results if f['operation'] == 'UPDATE' and f['status'] == 'WARNING'})}</b></span>",
            ),
            (
                "  • Updated with Warnings",
                f"<span style='color: orange;'><b>{len({f['user_id'] for f in self.results if f['operation'] == 'UPDATE' and f['status'] == 'WARNING'})}</b></span>"
                if len(
                    {
                        f["user_id"]
                        for f in self.results
                        if f["operation"] == "UPDATE" and f["status"] == "WARNING"
                    }
                )
                > 0
                else "0",
            ),
            ("━━━ Terminations ━━━", "<b></b>"),
            ("  • Terminated", self.run_summary["terminated_count"]),
            ("━━━ Issues ━━━", "<b></b>"),
            (
                "  • Failed",
                f"<span style='color: red;'><b>{self.run_summary['failed_count']}</b></span>"
                if self.run_summary["failed_count"] > 0
                else "0",
            ),
            (
                "  • Total Warnings",
                f"<span style='color: orange;'><b>{self.run_summary['warning_count']}</b></span>"
                if self.run_summary["warning_count"] > 0
                else "0",
            ),
        ]

        rows = "".join(
            [
                EMAIL_TABLE_ROW_TEMPLATE.format(
                    row_cells=EMAIL_TABLE_CELL_TEMPLATE.format(cell=metric)
                    + EMAIL_TABLE_CELL_TEMPLATE.format(cell=value)
                )
                for metric, value in metrics
            ]
        )

        return EMAIL_BODY_SECTION_TEMPLATE.format(
            section_title="📊 Pipeline Run Overview",
            table_headers=headers,
            table_rows=rows,
        )

    def _build_batch_section(self) -> str:
        "Builds field change batches HTML section."
        headers = "".join(
            [
                EMAIL_TABLE_HEADER_TEMPLATE.format(header=h)
                for h in [
                    "Batch Context",
                    "Total Users",
                    "Users with Changes",
                    "Status",
                    "Duration",
                ]
            ]
        )

        rows = []
        for batch in self.batches:
            duration = "N/A"
            if batch["started_at"] and batch["finished_at"]:
                delta = batch["finished_at"] - batch["started_at"]
                duration = str(delta).split(".")[0]

            cells = [
                EMAIL_TABLE_CELL_TEMPLATE.format(
                    cell=f"<b>{batch['batch_context'] or 'Unknown'}</b>"
                ),
                EMAIL_TABLE_CELL_TEMPLATE.format(cell=batch["total_users"]),
                EMAIL_TABLE_CELL_TEMPLATE.format(cell=batch["users_with_changes"]),
                EMAIL_TABLE_CELL_TEMPLATE.format(
                    cell=f"<span style='color: green;'>{batch['status']}</span>"
                    if batch["status"] == "COMPLETED"
                    else batch["status"]
                ),
                EMAIL_TABLE_CELL_TEMPLATE.format(cell=duration),
            ]
            rows.append(EMAIL_TABLE_ROW_TEMPLATE.format(row_cells="".join(cells)))

        return EMAIL_BODY_SECTION_TEMPLATE.format(
            section_title="📦 Field Change Batches",
            table_headers=headers,
            table_rows="".join(rows),
        )

    def _build_field_changes_section(self) -> str:
        """Builds detailed field changes HTML section."""
        # Group changes by batch context
        changes_by_batch = {}
        for change in self.field_changes:
            context = change["batch_context"] or "Unknown Batch"
            if context not in changes_by_batch:
                changes_by_batch[context] = []
            changes_by_batch[context].append(change)

        # Build sections for each batch
        batch_sections = []
        for batch_context, changes in changes_by_batch.items():
            headers = "".join(
                [
                    EMAIL_TABLE_HEADER_TEMPLATE.format(header=h)
                    for h in [
                        "User ID",
                        "Field Name",
                        "Old Value (EC)",
                        "New Value (PDM)",
                    ]
                ]
            )

            rows = []
            for change in changes:
                # Truncate long values
                ec_val = str(change["ec_value"] or "N/A")
                pdm_val = str(change["pdm_value"] or "N/A")
                if len(ec_val) > 50:
                    ec_val = ec_val[:50] + "..."
                if len(pdm_val) > 50:
                    pdm_val = pdm_val[:50] + "..."

                cells = [
                    EMAIL_TABLE_CELL_TEMPLATE.format(
                        cell=f"<b>{change['user_id']}</b>"
                    ),
                    EMAIL_TABLE_CELL_TEMPLATE.format(
                        cell=f"<code>{change['field_name']}</code>"
                    ),
                    EMAIL_TABLE_CELL_TEMPLATE.format(
                        cell=f"<span style='color: #999;'>{ec_val}</span>"
                    ),
                    EMAIL_TABLE_CELL_TEMPLATE.format(
                        cell=f"<span style='color: #0055A4;'><b>{pdm_val}</b></span>"
                    ),
                ]
                rows.append(EMAIL_TABLE_ROW_TEMPLATE.format(row_cells="".join(cells)))

            batch_section = EMAIL_BODY_SECTION_TEMPLATE.format(
                section_title=f"🔄 Field Changes: {batch_context} ({len(changes)} changes)",
                table_headers=headers,
                table_rows="".join(rows),
            )
            batch_sections.append(batch_section)

        return "\n".join(batch_sections)

    def _build_failures_section(self) -> str:
        "Builds failures and warnings HTML section with full error messages. Payloads attached to email."
        # Separate failures from warnings
        errors = [f for f in self.results if f["status"] == "FAILED"]
        warnings = [f for f in self.results if f["status"] == "WARNING"]

        sections_html = []

        if self.results and any(f.get("payload_snapshot") for f in self.results):
            note = "<p><strong>📎 Note:</strong> Detailed payload snapshots are attached to this email as a JSON file.</p>"
            sections_html.append(note)

        if errors:
            sections_html.append(
                self._build_failure_table(errors, "❌ Failures", is_error=True)
            )

        if warnings:
            sections_html.append(
                self._build_failure_table(warnings, "⚠️ Warnings", is_error=False)
            )

        return "\n".join(sections_html)

    def _build_failure_table(self, items, title, is_error=True) -> str:
        "Builds HTML table for failures or warnings with full error messages."
        headers = "".join(
            [
                EMAIL_TABLE_HEADER_TEMPLATE.format(header=h)
                for h in ["User ID", "Operation", "Full Message"]
            ]
        )

        rows = []
        for item in items:
            message = item["error_message"] if is_error else item["warning_message"]

            # Show full message (no truncation)
            display_message = message or "N/A"

            # Escape HTML characters to prevent rendering issues
            display_message = display_message.replace("<", "&lt;").replace(">", "&gt;")

            cells = [
                EMAIL_TABLE_CELL_TEMPLATE.format(cell=f"<b>{item['user_id']}</b>"),
                EMAIL_TABLE_CELL_TEMPLATE.format(cell=item["operation"]),
                EMAIL_TABLE_CELL_TEMPLATE.format(
                    cell=f"<span style='color: {'red' if is_error else 'orange'}; word-wrap: break-word; white-space: pre-wrap;'>{display_message}</span>"
                ),
            ]
            rows.append(EMAIL_TABLE_ROW_TEMPLATE.format(row_cells="".join(cells)))

        return EMAIL_BODY_SECTION_TEMPLATE.format(
            section_title=title, table_headers=headers, table_rows="".join(rows)
        )

    def _build_summary_email(self) -> str:
        "Builds summary email when detailed data is too large."
        # Add attachments note
        attachments_note = ""
        notes = []

        if self.results and any(f.get("payload_snapshot") for f in self.results):
            notes.append(
                "<li><strong>payloads_{run_id}.json</strong> - All payload snapshots for debugging</li>".replace(
                    "{run_id}", self.run_id
                )
            )

        notes.append(
            "<li><strong>detailed_report_{run_id}.html</strong> - Complete pipeline report with all sections (batches, field changes, failures/warnings)</li>".replace(
                "{run_id}", self.run_id
            )
        )

        if notes:
            attachments_note = f"<p><strong>📎 Attached Files:</strong></p><ul>{''.join(notes)}</ul><p><em>Open the HTML report for full details about this pipeline run.</em></p>"

        # Compute failed and warning counts as distinct users per operation
        def failed_unique_count(op_name: str) -> int:
            return len(
                {
                    f["user_id"]
                    for f in self.results
                    if f["operation"] == op_name and f["status"] == "FAILED"
                }
            )

        def warning_unique_count(op_name: str) -> int:
            return len(
                {
                    f["user_id"]
                    for f in self.results
                    if f["operation"] == op_name and f["status"] == "WARNING"
                }
            )

        # New employees metrics
        created_total = self.run_summary.get("created_count", 0) or 0
        created_failed = failed_unique_count("CREATE")
        created_warnings = warning_unique_count("CREATE")
        created_success = max(0, created_total - created_failed - created_warnings)

        # Existing employees metrics from batches
        total_existing_employees = sum(b.get("total_users", 0) for b in self.batches)
        users_with_changes = sum(b.get("users_with_changes", 0) for b in self.batches)
        updated_total = self.run_summary.get("updated_count", 0) or 0
        updated_failed = failed_unique_count("UPDATE")
        updated_success = max(0, updated_total - updated_failed)

        # Terminations
        terminated_total = self.run_summary.get("terminated_count", 0) or 0
        terminated_failed = failed_unique_count("TERMINATE")
        terminated_success = max(0, terminated_total - terminated_failed)

        summary_rows = [
            SUMMARY_TABLE_ROW_TEMPLATE.format(
                category="New Employees (Success)",
                total=created_total,
                successful=created_success,
                failed=created_failed,
            ),
            SUMMARY_TABLE_ROW_TEMPLATE.format(
                category="New Employees (With Warnings)",
                total=created_warnings,
                successful=created_warnings,
                failed=0,
            ),
            SUMMARY_TABLE_ROW_TEMPLATE.format(
                category="Existing Employees (Total)",
                total=total_existing_employees,
                successful=total_existing_employees,
                failed=0,
            ),
            SUMMARY_TABLE_ROW_TEMPLATE.format(
                category="Existing Employees (With Changes)",
                total=users_with_changes,
                successful=updated_success,
                failed=updated_failed,
            ),
            SUMMARY_TABLE_ROW_TEMPLATE.format(
                category="Terminations",
                total=terminated_total,
                successful=terminated_success,
                failed=terminated_failed,
            ),
        ]

        # Insert attachments note after the summary paragraph
        template = EMAIL_BODY_SUMMARY_TEMPLATE.replace(
            "<h3>Summary:</h3>", f"{attachments_note}<h3>Summary:</h3>"
        )

        return template.format(summary_rows="".join(summary_rows))
