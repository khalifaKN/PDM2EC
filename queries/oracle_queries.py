extract_pdm_records_query = """
       WITH base_query AS (
        -- Pre-filter to get all active employees
        SELECT DISTINCT PDM_UID
        FROM STAGING.M_HR_PERSON_V2
        WHERE
            (
                IS_STAFF_MEMBER = 'Y' 
                OR PDM_UID = '7035818' -- external IM requested by Nico.
                OR PDM_UID = '7043121' --  external IM requested by Oscar. 
            )
            AND IS_CURRENT_ACTIVE = 'Y'
    )

    SELECT DISTINCT
        MHP.PDM_UID AS USERID,
        MHP.E_MAIL AS USERNAME,
        MHP.FIRSTNAME AS FIRSTNAME,
        MHP.MIDDLENAME AS MI,
        MHP.LASTNAME AS LASTNAME,
        MHP.NICKNAME AS NICKNAME,
        MHP.E_MAIL AS EMAIL,
        MHP.PRIVATE_EMAIL,
        -- Gender: Set to NULL if value is 'D'
        CASE
            WHEN MHP.GENDER = 'D' THEN NULL
            ELSE MHP.GENDER
        END AS GENDER,
        TO_CHAR(MHP.DATE_OF_BIRTH, 'MM/DD/YYYY') AS DATE_OF_BIRTH,

        -- Manager ID logic: use DIS_PDM_UID if in base_query
        COALESCE(
            CASE
                WHEN MHP.IS_CURRENT_ACTIVE = 'Y' AND MHP.DIS_PDM_UID IN (SELECT PDM_UID FROM base_query)
                    THEN TO_CHAR(MHP.DIS_PDM_UID)
            END,
            ''
        ) AS MANAGER,
        TO_CHAR(mgr.JOB_TITLE_STARTDATE, 'MM/DD/YYYY') AS MANAGER_POSITION_START_DATE,

        -- Matrix Manager logic
        COALESCE(
            CASE
                WHEN MHP.IS_CURRENT_ACTIVE = 'Y' AND MHP.TEC_PDM_UID IN (SELECT PDM_UID FROM base_query)
                    THEN TO_CHAR(MHP.TEC_PDM_UID)
            END,
            ''
        ) AS MATRIX_MANAGER,
        TO_CHAR(mmgr.JOB_TITLE_STARTDATE, 'MM/DD/YYYY') AS MATRIX_MANAGER_POSITION_START_DATE,

        -- HR logic
        COALESCE(
            CASE
                WHEN MHP.IS_CURRENT_ACTIVE = 'Y' AND MHP.HRRES_PDM_UID IN (SELECT PDM_UID FROM base_query)
                    THEN TO_CHAR(MHP.HRRES_PDM_UID)
            END,
            ''
        ) AS HR,
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
        MHP.HCM_BU_FU_PDM2 as DIVISION,
        MHP.COUNTRY_CODE as COUNTRY_CODE,
        -- Build a position name from several fields
        MHP.JOB_TITLE || '-' || MHP.HCM_BU_FU_PDM2 || '-' || MHP.COUNTRY_CODE ||'_'|| MHP.LOCATION_CODE || '-' || MHP.KN_CODE AS POSITION_NAME,

        -- Flags for special populations (IM/SCM), using business logic and manual overrides
        COALESCE(
            CASE
                WHEN (MHP.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y' AND MHP.PDM_UID NOT IN ('28470'))
                 --OR 
                   --  MHP.COUNTRY_CODE IN ('CO', 'EC', 'VE', 'PE')
                --     OR (MHP.COUNTRY_CODE = 'CR' AND MHP.CORPORATE_COMPANY <> 'CR80'))
                THEN 'Y'
            END,
            ''
        ) AS IS_PEOPLEHUB_IM_MANUALLY_INCLUDED,

        COALESCE(
        CASE
            WHEN MHP.IS_PEOPLEHUB_SCM_MANUALLY_INCLUDED = 'Y'
                THEN 'Y'
        END,
        ''
    ) AS IS_PEOPLEHUB_SCM_MANUALLY_INCLUDED,

    CASE
            WHEN MHP.WORKFORCE = 'W' AND LOWER(MHP.E_MAIL) NOT LIKE 'external.%@kuehne-nagel.com' AND LOWER(MHP.E_MAIL) NOT LIKE '%@kn-external.com' AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_DISABLED <> 'Y' THEN '19680' --EC + Learning + P&G
            WHEN MHP.WORKFORCE = 'B' AND LOWER(MHP.E_MAIL) NOT LIKE 'external.%@kuehne-nagel.com' AND LOWER(MHP.E_MAIL) NOT LIKE '%@kn-external.com' AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y' THEN '19679' --EC + P&G
            WHEN MHP.WORKFORCE = 'B' AND LOWER(MHP.E_MAIL) NOT LIKE 'external.%@kuehne-nagel.com' AND LOWER(MHP.E_MAIL) NOT LIKE '%@kn-external.com' AND MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y' THEN '19678' --EC + Learning
            WHEN MHP.WORKFORCE = 'B' AND LOWER(MHP.E_MAIL) NOT LIKE 'external.%@kuehne-nagel.com' AND LOWER(MHP.E_MAIL) NOT LIKE '%@kn-external.com' AND MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y' AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y' THEN '19680' --EC + Learning + P&G
            WHEN MHP.WORKFORCE = 'B' THEN '20797' --Functional license
            WHEN (LOWER(MHP.E_MAIL) LIKE 'external.%@kuehne-nagel.com' OR LOWER(MHP.E_MAIL) LIKE '%@kn-external.com') AND MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y' AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y' THEN '22944' -- EXT Learning + P&G
            WHEN (LOWER(MHP.E_MAIL) LIKE 'external.%@kuehne-nagel.com' OR LOWER(MHP.E_MAIL) LIKE '%@kn-external.com') AND MHP.IS_PEOPLEHUB_LEARNING_MGMT_MANUALLY_INCLUDED = 'Y' THEN '22937' -- EXT Learning 
            WHEN (LOWER(MHP.E_MAIL) LIKE 'external.%@kuehne-nagel.com' OR LOWER(MHP.E_MAIL) LIKE '%@kn-external.com') AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_INCLUDED = 'Y' THEN '22938' -- EXT P&G 
            WHEN (LOWER(MHP.E_MAIL) LIKE 'external.%@kuehne-nagel.com' OR LOWER(MHP.E_MAIL) LIKE '%@kn-external.com') AND MHP.IS_PEOPLEHUB_RECRUITMENT_MGMT_MANUALLY_INCLUDED = 'Y' THEN '22939' -- EXT Recruiting 
            WHEN MHP.WORKFORCE = 'W' AND MHP.IS_PEOPLEHUB_PERFORMANCE_MANUALLY_DISABLED = 'Y' AND LOWER(MHP.E_MAIL) NOT LIKE 'external.%@kuehne-nagel.com' AND LOWER(MHP.E_MAIL) NOT LIKE '%@kn-external.com' THEN '19674' -- EP + Learning
            ELSE '19677'  --EP + None
        END AS custom_string_8

    FROM STAGING.M_HR_PERSON_V2 MHP

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
      - imlookup.PDM_UID IN (SELECT PDM_UID FROM base_query): Restricts to active population.
    Result:
      - Managers of flagged employees are included.
      - Managers of non-flagged employees are excluded unless they manage another flagged employee.
    */
    LEFT JOIN STAGING.M_HR_PERSON_V2 imlookup
        ON MHP.PDM_UID = imlookup.DIS_PDM_UID
        AND imlookup.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y'
        AND imlookup.PDM_UID IN (SELECT PDM_UID FROM base_query)

    -- INNER JOIN on base_query must be before WHERE
    INNER JOIN base_query BQ ON MHP.PDM_UID = BQ.PDM_UID

    -- Final WHERE clause applies location and company filters, and includes special populations
    WHERE  MHP.PDM_UID not in (7033439, 7033429, 7033440) AND (MHP.CORPORATE_COMPANY = 'CH08'
       OR MHP.IS_PEOPLEHUB_SCM_MANUALLY_INCLUDED = 'Y'
       OR MHP.IS_PEOPLEHUB_IM_MANUALLY_INCLUDED = 'Y'
       OR imlookup.PDM_UID IS NOT NULL
       OR (
         MHP.COUNTRY_CODE IN ('PA', 'NI', 'GT', 'SV', 'HN', 'DO', 'EE', 'SI', 'BA', 'HR', 'CO', 'EC', 'VE', 'PE', 'IE', 'CL', 'AR', 'BO', 'UY', 'SE', 'IS', 'PT')
         OR (MHP.COUNTRY_CODE = 'CR' AND MHP.CORPORATE_COMPANY <> 'CR80')
         OR (MHP.COUNTRY_CODE = 'PH' AND MHP.CORPORATE_COMPANY = 'PH43')
         OR MHP.CODE_1 in ('GB31', 'GB14')
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
            """
