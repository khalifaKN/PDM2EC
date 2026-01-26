def get_user_inactive_payload():
    """
    returns:
    {
        "__metadata": {
            "uri": "User",
            "type": "SFOData.User"
        },
        "userId": user_id,
        "status": "inactive"
    } 
    """
    return {
        "__metadata": {
            "uri": "User",
            "type": "SFOData.User"
            },
        "userId": "",
        "status": "inactive"
    }
