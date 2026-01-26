def get_per_email_payload():
    """
    returns peremail template payload

    Returns:
    {
        "__metadata": {
        "uri": "PerEmail",
        "type": "SFOData.PerEmail"
        },
        # Use a different emailType code for private vs. business emails
        "emailType": "",
        "personIdExternal": "",
        "emailAddress": "",
        # Set as primary if is_master is true
        "isPrimary": ""
    }
    """

    return {
        "__metadata": {
            "uri": "PerEmail",
            "type": "SFOData.PerEmail"
            },
        # Use a different emailType code for private vs. business emails
        "emailType": "",
        "personIdExternal": "",
        "emailAddress": "",
        # Set as primary if is_master is true
        "isPrimary": ""
    }