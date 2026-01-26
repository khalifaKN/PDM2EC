base_url = "https://api55preview.sapsf.eu"

auth_endpoint = "/oauth/token"

get_apis = {
    "positions": "/odata/v2/Position",
    "employees": "/odata/v2/EmpJob",
    "perPerson": "/odata/v2/PerPerson",
    "perPersonal": "/odata/v2/PerPersonal",
    "perEmail": "/odata/v2/PerEmail",
}

uris_params = {
    "positions": "$format=json&$select=jobCode,jobTitle,code,effectiveStartDate,standardHours,location,externalName_defaultValue,costCenter,company,division,cust_subUnit,cust_geographicalScope,positionCriticality",
    "employees": "$format=json&$select=position,company,managerId,userId,seqNumber,jobTitle,jobCode,location,division,startDate",
    "perPerson": "$format=json&$select=dateOfBirth,placeOfBirth,countryOfBirth,birthName,personIdExternal",
    "perPersonal": "$format=json&$select=firstName,lastName,personIdExternal,middleName,gender,nationality,title,startDate,endDate",
    "perEmail": "$format=json&$select=emailAddress,emailType,personIdExternal,isPrimary",
}

batch_apis = {
    "/odata/v2/$batch"
}