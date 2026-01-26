def get_empjob_relationships_payload():
    """
    returns empjobrelationships template payload

    Returns:
            {
                "__metadata": {
                    "uri": "EmpJobRelationships",
                    "type": "SFOData.EmpJobRelationships"
                },
                "userId": '',
                "startDate": '',
                "relationshipType": '',
                "relUserId": '',
                #"operation": "DELIMIT" -- will be added dynamically when needed
            }
    """

    return {
                "__metadata": {
                    "uri": "EmpJobRelationships",
                    "type": "SFOData.EmpJobRelationships"
                },
                "userId": '',
                "startDate": '',
                "relationshipType": '',
                "relUserId": '',
                #"operation": "DELIMIT" -- will be added dynamically when needed
            }