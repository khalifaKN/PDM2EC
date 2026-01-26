def get_position_payload():
    """
    returns position template payload

    Returns:
    {
            "__metadata": {
            "uri": "Position",
            "type": "SFOData.Position"
            },
            "effectiveStartDate": "/Date(-2208988800000)/",
            "effectiveStatus": "A",
            "company": "",
            "costCenter": "",
            "cust_Email_Address_Required": False,  # Correct boolean value
            "cust_Country_Of_Registration": "",
            "division": "",
            "jobCode": "",
            "location": "",
            "changeReason": "import",
            "standardHours": "40",
            "technicalParameters": "SYNC",
            "cust_geographicalScope": "",
            "cust_subUnit": "",
    }
    """

    return {
            "__metadata": {     
                "uri": "Position",
                "type": "SFOData.Position"
                },
            "effectiveStartDate": "/Date(-2208988800000)/",
            "effectiveStatus": "A",
            "company": "",
            "costCenter": "",
            "cust_Email_Address_Required": False, 
            "cust_Country_Of_Registration": "",
            "division": "",
            "jobCode": "",
            "location": "",
            "changeReason": "import",
            "standardHours": "40",
            "technicalParameters": "SYNC",
            "cust_geographicalScope": "",
            "cust_subUnit": ""
        }