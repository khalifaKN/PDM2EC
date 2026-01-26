def get_user_role_payload():
    """
    returns:
    {
        "__metadata": {
            "uri": "User",
            "type": "SFOData.User"
        },
        "userId": user_id,
        "custom08": ""
    } 
    """
    return {
        "__metadata": {
            "uri": "User",
            "type": "SFOData.User"
            },
        "userId": "",
        "custom08": ""
    }
