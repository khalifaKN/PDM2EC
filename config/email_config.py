EMAIL_THREAD={
    "TO": ["khalifa.saidi@kuehne-nagel.com"],
    "CC": [""],
    "SUBJECT": "PDM to EC Pipeline Notification",
}

"""First Template: Body when the output can be shown in the email\
    This templates has:
        New Employees Report
        - For new employees: Table with the next columns:
            - User ID
            - Full Name
            - Country
            - Entities successfully created in EC
            - Entities failed to be created in EC
            - Error Messages (if any) 
            - Warning Messages (if any)
        Then Line break
        Field Updates Report
        - For field updates: Table with the next columns:
            - User ID
            - Full Name
            - Country
            - Field Name
            - Old Value
            - New Value
        Then Line break
        Inactive Users Report
        - For inactive users: Table with the next columns:
            - User ID
            - Full Name
            - Country
            - Deactivation Status
            - Termination Status
Nice Tables with html formatting respecting Kuhene+Nagel branding (colors, font, etc)
"""



EMAIL_BODY_TEMPLATE = """\
<html>
  <head>
    <style>
      body {{
        font-family: Arial, sans-serif;
        color: #333333;
        background-color: #f9f9f9;
        padding: 20px;
      }}
      .container {{
        max-width: 1200px;
        margin: 0 auto;
        background-color: white;
        padding: 30px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
      }}
      h2 {{
        color: #0055A4;
        border-bottom: 3px solid #0055A4;
        padding-bottom: 10px;
      }}
      h3 {{
        color: #0055A4;
        margin-top: 25px;
      }}
      table {{
        width: 100%;
        border-collapse: collapse;
        margin-bottom: 25px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
      }}
      th, td {{
        border: 1px solid #dddddd;
        text-align: left;
        padding: 12px;
      }}
      th {{
        background-color: #0055A4;
        color: white;
        font-weight: bold;
      }}
      tr:nth-child(even) {{
        background-color: #f9f9f9;
      }}
      tr:hover {{
        background-color: #f0f0f0;
      }}
      .footer {{
        margin-top: 30px;
        padding-top: 20px;
        border-top: 1px solid #dddddd;
        color: #666666;
        font-size: 0.9em;
      }}
    </style>
  </head>
  <body>
    <div class="container">
      <h2>📧 PDM to EC Pipeline Notification</h2>
      <p>The PDM to EC data pipeline has completed. Below are the detailed execution results:</p>
      {content_sections}
      <div class="footer">
        <p>For any questions or further information, please contact the HRIS team.</p>
        <p><strong>Best regards,</strong><br/>HRIS Integrations Team<br/>Kuehne+Nagel</p>
      </div>
    </div>
  </body>
</html>
"""


EMAIL_BODY_SECTION_TEMPLATE = """\
    <h3>{section_title}</h3>
    <table>
      <tr>
        {table_headers}
      </tr>
      {table_rows}
    </table>
"""


EMAIL_TABLE_HEADER_TEMPLATE = "<th>{header}</th>"
EMAIL_TABLE_ROW_TEMPLATE = "<tr>{row_cells}</tr>"
EMAIL_TABLE_CELL_TEMPLATE = "<td>{cell}</td>"

"""
Second Template: Body when the output is too large to be shown in the email
    This template has:
        - A message indicating that the output is too large to be displayed in the email.
        - A report will be attached to the email instead.
        - A summary showing:
            - Total number of new employees processed:
                - Total number of successful creations
                - Total number of failed creations
            - Total number of field updates processed:
                - Total number of successful updates
                - Total number of failed updates
            - Total number of inactive users processed:
                - Total number of successful deactivations
                - Total number of successful terminations
Html formatting respecting Kuhene+Nagel branding (colors, font, etc)
"""
EMAIL_BODY_SUMMARY_TEMPLATE = """\
<html>
    <head>
        <style>
        body {{
            font-family: Arial, sans-serif;
            color: #333333;
        }}
        h2 {{
            color: #0055A4;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-bottom: 20px;
        }}
        th, td {{
            border: 1px solid #dddddd;
            text-align: left;
            padding: 8px;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        </style>
    </head>
    <body>
        <h2>PDM to EC Pipeline Notification</h2>
        <p>The PDM to EC data pipeline has completed successfully. However, the output is too large to be displayed in this email. Please find the attached report for detailed information.</p>
        <h3>Summary:</h3>
        <table>
        <tr>
            <th>Category</th>
            <th>Total Processed</th>
            <th>Successful</th>
            <th>Failed</th>
        </tr>
        {summary_rows}
        </table>
        <p>For any questions or further information, please contact the HRIS team.</p>
        <p>Best regards,<br/>HRIS Integrations Team</p>
        </body>
</html>
"""
SUMMARY_TABLE_ROW_TEMPLATE = "<tr><td>{category}</td><td>{total}</td><td>{successful}</td><td>{failed}</td></tr>"