extract_pdm_records_query = """
       WITH base_query AS (
    -- Pre-filter to get all active employees
    -- Materialized hint for Oracle to cache this CTE
    SELECT /*+ MATERIALIZE */ DISTINCT PDM_UID
    FROM STAGING.M_HR_PERSON_V2 MHPV2
    WHERE
            MHPV2.IS_CURRENT_ACTIVE = 'Y'
        AND (
                -- Staff members
                MHPV2.IS_STAFF_MEMBER = 'Y'

                -- Explicitly allowed external users
                OR MHPV2.PDM_UID IN ('7035818', '7043121')

                -- Include IM and non-staff users (with exclusion)
                OR (
                    MHPV2.IS_STAFF_MEMBER = 'N'
                    AND MHPV2.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y'
                    AND MHPV2.PDM_UID <> '28470'
                )
            )
),
email_classification AS (
    -- Precompute email classification to avoid repeated LOWER() calls
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
    MHP.FIRSTNAME,
    MHP.MIDDLENAME AS MI,
    MHP.LASTNAME,
    MHP.NICKNAME,
    MHP.E_MAIL AS EMAIL,
    MHP.PRIVATE_EMAIL,
    -- Gender: NULL if 'D'
    NULLIF(MHP.GENDER, 'D') AS GENDER,
    TO_CHAR(MHP.DATE_OF_BIRTH, 'MM/DD/YYYY') AS DATE_OF_BIRTH,

    -- Direct Manager
    CASE
        WHEN MHP.IS_CURRENT_ACTIVE = 'Y' AND MHP.DIS_PDM_UID IN (SELECT PDM_UID FROM base_query)
            THEN TO_CHAR(MHP.DIS_PDM_UID)
        ELSE ''
    END AS MANAGER,
    TO_CHAR(mgr.JOB_TITLE_STARTDATE, 'MM/DD/YYYY') AS MANAGER_POSITION_START_DATE,

    -- Matrix Manager
    CASE
        WHEN MHP.IS_CURRENT_ACTIVE = 'Y' AND MHP.TEC_PDM_UID IN (SELECT PDM_UID FROM base_query)
            THEN TO_CHAR(MHP.TEC_PDM_UID)
        ELSE ''
    END AS MATRIX_MANAGER,
    TO_CHAR(mmgr.JOB_TITLE_STARTDATE, 'MM/DD/YYYY') AS MATRIX_MANAGER_POSITION_START_DATE,

    -- HR Manager
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
    MHP.BRANCH_OFFICE_TIME_ZONE_CODE AS TIMEZONE,
    MHP.CODE_1 AS COMPANY,
    'SSO' AS LOGIN_METHOD,
    MHP.LOCATION_CODE,
    MHP.HCM_BU_FU_PDM2 AS DIVISION,
    MHP.COUNTRY_CODE,
    -- Construct POSITION_NAME
    MHP.JOB_TITLE || '-' || MHP.HCM_BU_FU_PDM2 || '-' || MHP.COUNTRY_CODE ||'_'|| MHP.LOCATION_CODE || '-' || MHP.KN_CODE AS POSITION_NAME,

    -- Flags for special populations
    CASE
        WHEN MHP.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y' AND MHP.PDM_UID <> '28470'
            THEN 'Y'
        ELSE ''
    END AS IS_PEOPLEHUB_IM_MANUALLY_INCLUDED,

    CASE
        WHEN MHP.IS_PEOPLEHUB_SCM_MANUALLY_INCLUDED = 'Y'
            THEN 'Y'
        ELSE ''
    END AS IS_PEOPLEHUB_SCM_MANUALLY_INCLUDED,

    -- License assignment logic (custom_string_8)
    CASE
        -- INTERNAL Workforce
        WHEN MHP.WORKFORCE = 'W'
         AND EC.email_type = 'INTERNAL'
         AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_DISABLED <> 'Y'
            THEN '19680'

        -- INTERNAL Blue Collar
        WHEN MHP.WORKFORCE = 'B' AND EC.email_type = 'INTERNAL' THEN
            CASE
                WHEN MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y' 
                 AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y'
                    THEN '19680'
                WHEN MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y'
                    THEN '19679'
                WHEN MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y'
                    THEN '19678'
                ELSE '20797'
            END

        -- EXTERNAL users
        WHEN EC.email_type = 'EXTERNAL' THEN
            CASE
                WHEN MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y' 
                 AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y'
                    THEN '22944'
                WHEN MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y'
                    THEN '22937'
                WHEN MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y'
                    THEN '22938'
                WHEN MHP.IS_PEOPLEHUB_RECRUITMENT_MGMT_MANUALLY_INCLUDED = 'Y'
                    THEN '22939'
                ELSE '19677'
            END

        -- INTERNAL Workforce with Performance disabled
        WHEN MHP.WORKFORCE = 'W'
         AND EC.email_type = 'INTERNAL'
         AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_DISABLED = 'Y'
            THEN '19674'

        -- Default
        ELSE '19677'
    END AS custom_string_8

FROM STAGING.M_HR_PERSON_V2 MHP

-- Join email classification
INNER JOIN email_classification EC ON MHP.PDM_UID = EC.PDM_UID

-- Joins for managers, matrix managers, HR
LEFT JOIN STAGING.M_HR_PERSON_V2 mgr ON MHP.DIS_PDM_UID = mgr.PDM_UID
LEFT JOIN STAGING.M_HR_PERSON_V2 mmgr ON MHP.TEC_PDM_UID = mmgr.PDM_UID
LEFT JOIN STAGING.M_HR_PERSON_V2 hr ON MHP.HRRES_PDM_UID = hr.PDM_UID

-- Lookup for IM manually included managers
LEFT JOIN STAGING.M_HR_PERSON_V2 imlookup
    ON MHP.PDM_UID = imlookup.DIS_PDM_UID
    AND imlookup.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y'
    AND imlookup.PDM_UID IN (SELECT PDM_UID FROM base_query)

-- Restrict to active employees
INNER JOIN base_query BQ ON MHP.PDM_UID = BQ.PDM_UID

-- Apply exclusions and location filters
WHERE MHP.PDM_UID NOT IN (7033439,7033429,7033440)
  AND (
        MHP.CORPORATE_COMPANY = 'CH08'
        OR MHP.IS_PEOPLEHUB_SCM_MANUALLY_INCLUDED = 'Y'
        OR MHP.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y'
        OR imlookup.PDM_UID IS NOT NULL
        OR (
            MHP.COUNTRY_CODE IN ('PA','NI','GT','SV','HN','DO','EE','SI','BA','HR','CO','EC','VE','PE','IE','CL','AR','BO','UY','SE','IS','PT')
            OR (MHP.COUNTRY_CODE = 'CR' AND MHP.CORPORATE_COMPANY <> 'CR80')
            OR (MHP.COUNTRY_CODE = 'PH' AND MHP.CORPORATE_COMPANY = 'PH43')
            OR MHP.CODE_1 IN ('GB31','GB14')
        )
      )
"""

# Get only inactive IM/SCM users who were manually included
extract_pdm_inactive_records_query = """
                SELECT PDM_UID, EXIT_REASON_ID, TO_CHAR(DATE_OF_LEAVE, 'MM/DD/YYYY') AS DATE_OF_LEAVE
                FROM STAGING.M_HR_PERSON_V2
                WHERE PDM_UID IN ({user_ids}) 
                AND
                IS_CURRENT_ACTIVE = 'N'
                AND
                (
                    IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y'
                    OR IS_PEOPLEHUB_SCM_MANUALLY_INCLUDED = 'Y'
                ) 
                AND EXIT_REASON_ID IS NOT NULL
                AND DATE_OF_LEAVE <= TRUNC(SYSDATE)
                AND ADD_MONTHS(DATE_OF_LEAVE, 60) >= TRUNC(SYSDATE)
            """
