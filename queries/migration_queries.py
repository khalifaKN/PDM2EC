migration_query="""
    WITH base_query AS (
        -- Pre-filter to get all active employees
        -- Materialized hint for Oracle to cache this CTE
        SELECT /*+ MATERIALIZE */ DISTINCT PDM_UID
        FROM STAGING.M_HR_PERSON_V2
        WHERE
            (IS_STAFF_MEMBER = 'Y') --external IM requested by Nico
            AND IS_CURRENT_ACTIVE = 'Y'
    ),
    -- Precompute email classification to avoid repeated LOWER() calls
    email_classification AS (
        SELECT 
            PDM_UID,
            CASE 
                WHEN LOWER(E_MAIL) LIKE 'external.%@kuehne-nagel.com' 
                  OR LOWER(E_MAIL) LIKE '%@kn-external.com' 
                THEN 'EXTERNAL'
                ELSE 'INTERNAL'
            END AS email_type
        FROM STAGING.M_HR_PERSON_V2
        WHERE PDM_UID IN (SELECT PDM_UID FROM base_query)
    )
    SELECT DISTINCT
        MHP.PDM_UID AS USERID,
        MHP.E_MAIL AS USERNAME,
        MHP.FIRSTNAME AS FIRSTNAME,
        MHP.MIDDLENAME AS MI,
        MHP.LASTNAME AS LASTNAME,
        MHP.NICKNAME AS NICKNAME,
        MHP.E_MAIL AS EMAIL,
        -- Gender: Set to NULL if value is 'D' (Diverse/Not Specified)
        NULLIF(MHP.GENDER, 'D') AS GENDER,
        TO_CHAR(MHP.DATE_OF_BIRTH, 'MM/DD/YYYY') AS DATE_OF_BIRTH,

        -- Direct Manager with validation
        CASE
            WHEN MHP.IS_CURRENT_ACTIVE = 'Y' AND MHP.DIS_PDM_UID IN (SELECT PDM_UID FROM base_query)
                THEN TO_CHAR(MHP.DIS_PDM_UID)
            ELSE ''
        END AS MANAGER,
        TO_CHAR(mgr.JOB_TITLE_STARTDATE, 'MM/DD/YYYY') AS MANAGER_POSITION_START_DATE,

        -- Matrix Manager with validation
        CASE
            WHEN MHP.IS_CURRENT_ACTIVE = 'Y' AND MHP.TEC_PDM_UID IN (SELECT PDM_UID FROM base_query)
                THEN TO_CHAR(MHP.TEC_PDM_UID)
            ELSE ''
        END AS MATRIX_MANAGER,
        TO_CHAR(mmgr.JOB_TITLE_STARTDATE, 'MM/DD/YYYY') AS MATRIX_MANAGER_POSITION_START_DATE,

        -- HR Manager with validation
        CASE
            WHEN MHP.IS_CURRENT_ACTIVE = 'Y' AND MHP.HRRES_PDM_UID IN (SELECT PDM_UID FROM base_query)
                THEN TO_CHAR(MHP.HRRES_PDM_UID)
            ELSE ''
        END AS HR,
        TO_CHAR(hr.JOB_TITLE_STARTDATE, 'MM/DD/YYYY') AS HR_POSITION_START_DATE,

        -- Job and employment details
        MHP.JOB_TITLE_ID AS JOBCODE,
        TO_CHAR(MHP.DATE_OF_ENTRY_KN_GROUP, 'MM/DD/YYYY') AS HIREDATE,
        TO_CHAR(MHP.BEGIN_DATE, 'MM/DD/YYYY') AS START_OF_EMPLOYMENT,
        TO_CHAR(MHP.JOB_TITLE_STARTDATE, 'MM/DD/YYYY') AS DATE_OF_POSITION,
        MHP.CODE_1 ||'_'|| MHP.COST_CENTER AS COST_CENTER,
        MHP.BRANCH_OFFICE_CID AS ADDRESS_CODE,
        MHP.PHONE AS BIZ_PHONE,
        MHP.MOBILE AS BIZ_MOBILE,
        MHP.BRANCH_OFFICE_TIME_ZONE_CODE AS TIMEZONE,
        MHP.CODE_1 AS COMPANY,
        'SSO' AS LOGIN_METHOD,
        MHP.LOCATION_CODE,
        MHP.HCM_BU_FU_PDM2 as DIVISION,
        MHP.COUNTRY_CODE as COUNTRY_CODE,
        -- Build a position name from several fields
        MHP.JOB_TITLE || '-' || MHP.HCM_BU_FU_PDM2 || '-' || MHP.COUNTRY_CODE ||'_'|| MHP.LOCATION_CODE || '-' || MHP.KN_CODE AS POSITION_NAME,

        -- Flags for special populations (IM/SCM), using business logic and manual overrides
        CASE
            WHEN MHP.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y' AND MHP.PDM_UID != '28470'
                THEN 'Y'
            ELSE ''
        END AS IS_PEOPLEHUB_IM_MANUALLY_INCLUDED,

        CASE
            WHEN MHP.IS_PEOPLEHUB_SCM_MANUALLY_INCLUDED = 'Y'
                THEN 'Y'
            ELSE ''
        END AS IS_PEOPLEHUB_SCM_MANUALLY_INCLUDED,
        
        -- License Assignment Logic (SF custom_string_8)
        -- Priority order: Workforce type → Email type → Manual flags
        CASE
            -- INTERNAL WORKFORCE (W) - Full access by default
            WHEN MHP.WORKFORCE = 'W' 
             AND EC.email_type = 'INTERNAL'
             AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_DISABLED != 'Y' 
                THEN '19680' -- EC + Learning + P&G (Full Suite)
            
            -- INTERNAL BLUE COLLAR (B) with manual overrides
            WHEN MHP.WORKFORCE = 'B' AND EC.email_type = 'INTERNAL' THEN
                CASE
                    WHEN MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y' 
                     AND MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y'
                        THEN '19680' -- EC + Learning + P&G
                    WHEN MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y'
                        THEN '19679' -- EC + P&G
                    WHEN MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y'
                        THEN '19678' -- EC + Learning
                    ELSE '20797' -- Functional license only
                END
            
            -- EXTERNAL users (email-based detection)
            WHEN EC.email_type = 'EXTERNAL' THEN
                CASE
                    WHEN MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y'
                     AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y'
                        THEN '22944' -- EXT Learning + P&G
                    WHEN MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y'
                        THEN '22937' -- EXT Learning only
                    WHEN MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y'
                        THEN '22938' -- EXT P&G only
                    WHEN MHP.IS_PEOPLEHUB_RECRUITMENT_MGMT_MANUALLY_INCLUDED = 'Y'
                        THEN '22939' -- EXT Recruiting only
                    ELSE '19677' -- EC only (minimal access)
                END
            
            -- INTERNAL WORKFORCE with Performance disabled
            WHEN MHP.WORKFORCE = 'W' 
             AND EC.email_type = 'INTERNAL'
             AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_DISABLED = 'Y'
                THEN '19674' -- EC + Learning (No P&G)
            
            -- Default fallback
            ELSE '19677' -- EC + None (minimal access)
        END AS custom_string_8

    FROM STAGING.M_HR_PERSON_V2 MHP

    -- Join email classification CTE
    INNER JOIN email_classification EC ON MHP.PDM_UID = EC.PDM_UID
    
    -- Joins to get manager, matrix manager, and HR details
    LEFT JOIN STAGING.M_HR_PERSON_V2 mgr ON MHP.DIS_PDM_UID = mgr.PDM_UID
    LEFT JOIN STAGING.M_HR_PERSON_V2 mmgr ON MHP.TEC_PDM_UID = mmgr.PDM_UID
    LEFT JOIN STAGING.M_HR_PERSON_V2 hr ON MHP.HRRES_PDM_UID = hr.PDM_UID

    -- Join for IS_PEOPLEHUB_IM_MANUALLY_INCLUDED lookup
    /*
    Purpose: Include managers of employees flagged with IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y'.
    Logic:
      - MHP.PDM_UID = imlookup.DIS_PDM_UID: Matches each employee with their manager.
      - imlookup.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y': Ensures only managers of flagged employees are included.
      - imlookup.PDM_UID IN base_query: Restricts to active population.
    Result:
      - Managers of flagged employees are included.
      - Managers of non-flagged employees are excluded unless they manage another flagged employee.
    */
    LEFT JOIN STAGING.M_HR_PERSON_V2 imlookup
        ON MHP.PDM_UID = imlookup.DIS_PDM_UID
        AND imlookup.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y'
        AND imlookup.PDM_UID IN (SELECT PDM_UID FROM base_query)

    -- INNER JOIN on base_query to restrict to active employees
    INNER JOIN base_query BQ ON MHP.PDM_UID = BQ.PDM_UID

    -- Final WHERE clause applies exclusions and location filters
    WHERE MHP.PDM_UID NOT IN ('7033439', '7033429', '7033440') -- Excluded users
      AND MHP.COUNTRY_CODE IN ('CY')
        """