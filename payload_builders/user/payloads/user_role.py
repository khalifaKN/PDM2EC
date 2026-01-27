def get_user_role_payload():
    """
    returns:
    {
        "__metadata": {
            "uri": "User('{user_id}')",
            "type": "SFOData.User"
        },
        "userId": user_id,
        "custom08": ""
    } 
    """
    return {
        "__metadata": {
            "uri": "User('{user_id}')",
            "type": "SFOData.User"
            },
        "userId": "",
        "custom08": ""
    }
