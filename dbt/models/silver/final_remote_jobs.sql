WITH source AS (
    SELECT
        "jobTitle",
        "companyName",
        "jobType",
        "jobLevel",
        "annualSalaryMin",
        "annualSalaryMax",
        "salaryCurrency"
    FROM {{source ("ANALYTICS_ENGINEER_DBT", "jobs_list")}}
    WHERE "annualSalaryMin" IS NOT NULL
    OR "annualSalaryMax" IS NOT NULL
),

renamed AS (
    SELECT
        "jobTitle" AS "job_title",
        "companyName" AS "company_name",
        "jobType" AS "job_type",
        "jobLevel" AS "job_level",
        TRY_CAST("annualSalaryMin" AS float) AS "annual_salary_min",
        TRY_CAST("annualSalaryMax" AS float) AS "annual_salary_max",
        "salaryCurrency" AS "currency"
    FROM source
),

final AS (
    SELECT
        "job_title",
        "company_name",
        "job_type",
        "job_level",
        "annual_salary_min" / 12 AS "month_salary_min",
        "annual_salary_min",
        "annual_salary_max" / 12 AS "month_salary_max",
        "annual_salary_max",
        "currency"
    FROM renamed
)

SELECT * FROM final