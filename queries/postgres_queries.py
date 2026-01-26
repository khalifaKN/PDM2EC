extract_ec_records_query = "SELECT * FROM ods.successfactors_active_employees where isecrecord ='True'"
extract_jobs_titles_records_query = """SELECT
                                            ID::TEXT as jobcode,
                                            JOBGROUPLEVEL2_ID::TEXT as bufu_id,
                                            JOBGROUPLEVEL4_ID::TEXT as cust_geographicalscope,
                                            JOBGROUPLEVEL6_ID::TEXT as cust_subunit
                                        FROM ods.JOB_TITLES"""
extract_different_userid_personid_query = """SELECT USERID, PERSON_ID_EXTERNAL 
        FROM ods.SUCCESSFACTORS_ACTIVE_EMPLOYEES 
        WHERE ISECRECORD = 'True' 
        AND USERID != PERSON_ID_EXTERNAL"""