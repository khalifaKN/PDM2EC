def get_emp_job():
    """
    returns empjob template payload
    Returns:
    {
        "__metadata": {
            "uri": "EmpJob",
            "type": "SFOData.EmpJob"
        },
        "userId": "",
        "position": "",
        "startDate": "", 
        "company": "",
        "eventReason": "",
        "seqNumber": "",
        "costCenter": "",
        "managerId": ""
    }
    """

    return {
        "__metadata": {
            "uri": "EmpJob",
            "type": "SFOData.EmpJob"
            },
        "userId": "",
        "position": "",
        "startDate": "", # Use current date for upserts to avoid manager/subordinate date issues
        "company": "",
        "eventReason": "",
        "seqNumber": "",
        "costCenter": "",
        "managerId": "",
    }