def get_user_inactive_payload():
    """
    returns:
    {
        "__metadata": {
            "uri": f"User('{user_id}')",
            "type": "SFOData.User"
        },
        "userId": user_id,
        "status": "inactive"
    } 
    """
    return {
        "__metadata": {
            "uri": "User('{user_id}')",
            "type": "SFOData.User"
            },
        "userId": "",
        "status": "inactive"
    }
