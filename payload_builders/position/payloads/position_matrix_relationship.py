def get_position_matrix_relationship_payload():
    """
    returns position matrix relationship template payload

    Returns:
    {
        "__metadata": {
            "uri": "PositionMatrixRelationship",
            "type": "SFOData.PositionMatrixRelationship"
        },
        "Position_code": "", -- User Position Code
        "Position_effectiveStartDate": "", -- User Position Effective Start Date
        "matrixRelationshipType": "", -- Relationship Type
        "relatedPosition": "" -- Position of the relation
    }
    """

    return {
        "__metadata": {
            "uri": "PositionMatrixRelationship",
            "type": "SFOData.PositionMatrixRelationship"
        },
        "Position_code": "",
        "Position_effectiveStartDate": "",
        "matrixRelationshipType": "",
        "relatedPosition": ""
    }