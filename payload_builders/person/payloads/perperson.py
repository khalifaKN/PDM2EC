def get_perperson_payload():
    """
    returns perperson template payload

    Returns:
    {
        "__metadata": {
        "uri": "PerPerson",
        "type": "SFOData.PerPerson"
        },
        "personIdExternal": "",
        "dateOfBirth": ""
    }
    """

    return {
        "__metadata": {
            "uri": "PerPerson",
            "type": "SFOData.PerPerson"
            },
        "personIdExternal": "",
        "dateOfBirth": ""
    }   