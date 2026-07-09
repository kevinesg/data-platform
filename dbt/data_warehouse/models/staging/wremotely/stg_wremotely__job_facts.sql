WITH raw_job_facts AS (
    SELECT *
    FROM {{ source('wremotely', 'job_facts') }}
),

renamed AS (
    SELECT
        stage_run_id
        , source_run_id AS job_facts_run_id
        , source_record_index
        , source_artifact_sha256
        , contract_version AS raw_contract_version
        , SAFE_CAST(JSON_VALUE(payload, '$.contract_version') AS INT64)
            AS payload_contract_version
        , JSON_VALUE(payload, '$.candidate_id') AS candidate_id
        , JSON_VALUE(payload, '$.url') AS url
        , JSON_VALUE(payload, '$.final_url') AS final_url
        , JSON_VALUE(payload, '$.source_domain') AS source_domain
        , JSON_VALUE(payload, '$.source_candidate_id') AS source_candidate_id
        , JSON_VALUE(payload, '$.source_url') AS source_url
        , JSON_VALUE(payload, '$.source_url_identity') AS source_url_identity
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_type_guess'))) AS source_type_guess
        , JSON_VALUE(payload, '$.source_platform_guess') AS source_platform_guess
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_review_status'))) AS source_review_status
        , TRIM(UPPER(JSON_VALUE(payload, '$.job_fact_status'))) AS job_fact_status
        , TRIM(UPPER(JSON_VALUE(payload, '$.page_status'))) AS page_status
        , SAFE_CAST(JSON_VALUE(payload, '$.retrieved_at') AS TIMESTAMP) AS retrieved_at
        , SAFE_CAST(JSON_VALUE(payload, '$.job_fact_extracted_at') AS TIMESTAMP)
            AS job_fact_extracted_at
        , JSON_VALUE(payload, '$.job_fact_extractor_version') AS job_fact_extractor_version
        , SAFE_CAST(JSON_VALUE(payload, '$.http_status') AS INT64) AS http_status
        , JSON_VALUE(payload, '$.content_type') AS content_type
        , JSON_VALUE(payload, '$.source_content_sha256') AS source_content_sha256
        , JSON_VALUE(payload, '$.raw_html_path') AS raw_html_path
        , JSON_VALUE(payload, '$.normalized_text_path') AS normalized_text_path
        , JSON_VALUE(payload, '$.normalized_text_sha256') AS normalized_text_sha256
        , JSON_VALUE(payload, '$.jsonld_path') AS jsonld_path
        , JSON_VALUE(payload, '$.jsonld_sha256') AS jsonld_sha256
        , SAFE_CAST(JSON_VALUE(payload, '$.job_posting_count') AS INT64)
            AS job_posting_count
        , SAFE_CAST(JSON_VALUE(payload, '$.jsonld_document_count') AS INT64)
            AS jsonld_document_count
        , SAFE_CAST(JSON_VALUE(payload, '$.jsonld_parse_error_count') AS INT64)
            AS jsonld_parse_error_count
        , JSON_VALUE(payload, '$.declared_language_raw') AS declared_language_raw
        , LOWER(NULLIF(TRIM(JSON_VALUE(payload, '$.declared_language_tag')), ''))
            AS declared_language_tag
        , JSON_VALUE(payload, '$.declared_language_source') AS declared_language_source
        , JSON_QUERY_ARRAY(payload, '$.raw_title_values') AS raw_title_values
        , JSON_VALUE(payload, '$.raw_title_values[0].value') AS raw_title
        , JSON_QUERY_ARRAY(payload, '$.raw_company_name_values') AS raw_company_name_values
        , JSON_VALUE(payload, '$.raw_company_name_values[0].value') AS raw_company_name
        , JSON_QUERY_ARRAY(payload, '$.raw_description_values') AS raw_description_values
        , JSON_VALUE(payload, '$.raw_description_values[0].value') AS raw_description
        , JSON_QUERY_ARRAY(payload, '$.raw_base_salary_values') AS raw_base_salary_values
        , JSON_QUERY(payload, '$.raw_base_salary_values[0].value') AS raw_base_salary_json
        , JSON_QUERY_ARRAY(payload, '$.raw_estimated_salary_values')
            AS raw_estimated_salary_values
        , JSON_QUERY(payload, '$.raw_estimated_salary_values[0].value')
            AS raw_estimated_salary_json
        , JSON_QUERY_ARRAY(payload, '$.raw_employment_type_values')
            AS raw_employment_type_values
        , JSON_VALUE(payload, '$.raw_employment_type_values[0].value')
            AS raw_employment_type
        , JSON_QUERY_ARRAY(payload, '$.raw_date_posted_values') AS raw_date_posted_values
        , COALESCE(
            SAFE_CAST(JSON_VALUE(payload, '$.raw_date_posted_values[0].value') AS TIMESTAMP)
            , TIMESTAMP(SAFE_CAST(
                JSON_VALUE(payload, '$.raw_date_posted_values[0].value') AS DATE
            ))
        ) AS raw_date_posted_at
        , JSON_QUERY_ARRAY(payload, '$.raw_valid_through_values') AS raw_valid_through_values
        , COALESCE(
            SAFE_CAST(JSON_VALUE(payload, '$.raw_valid_through_values[0].value') AS TIMESTAMP)
            , TIMESTAMP(SAFE_CAST(
                JSON_VALUE(payload, '$.raw_valid_through_values[0].value') AS DATE
            ))
        ) AS raw_valid_through_at
        , JSON_QUERY_ARRAY(payload, '$.raw_job_location_type_values')
            AS raw_job_location_type_values
        , JSON_VALUE(payload, '$.raw_job_location_type_values[0].value')
            AS raw_job_location_type
        , JSON_QUERY_ARRAY(payload, '$.raw_job_location_values') AS raw_job_location_values
        , ARRAY_TO_STRING(
            ARRAY(
                SELECT DISTINCT JSON_VALUE(job_location, '$.value')
                FROM UNNEST(
                    COALESCE(
                        JSON_QUERY_ARRAY(payload, '$.raw_job_location_values')
                        , ARRAY<JSON>[]
                    )
                ) AS job_location
                WHERE NULLIF(TRIM(JSON_VALUE(job_location, '$.value')), '') IS NOT NULL
            )
            , ', '
        ) AS raw_job_location_text
        , JSON_QUERY_ARRAY(payload, '$.raw_applicant_location_requirement_values')
            AS raw_applicant_location_requirement_values
        , ARRAY_TO_STRING(
            ARRAY(
                SELECT DISTINCT JSON_VALUE(applicant_location, '$.value')
                FROM UNNEST(
                    COALESCE(
                        JSON_QUERY_ARRAY(payload, '$.raw_applicant_location_requirement_values')
                        , ARRAY<JSON>[]
                    )
                ) AS applicant_location
                WHERE NULLIF(TRIM(JSON_VALUE(applicant_location, '$.value')), '') IS NOT NULL
            )
            , ', '
        ) AS raw_applicant_location_requirement_text
        , TRIM(UPPER(JSON_VALUE(payload, '$.raw_work_arrangement')))
            AS raw_work_arrangement
        , JSON_QUERY_ARRAY(payload, '$.raw_work_arrangement_evidence')
            AS raw_work_arrangement_evidence
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_default_work_arrangement')))
            AS source_default_work_arrangement
        , TRIM(UPPER(JSON_VALUE(payload, '$.source_default_country_eligibility_scope')))
            AS source_default_country_eligibility_scope
        , JSON_QUERY_ARRAY(payload, '$.source_default_country_eligibility_values')
            AS source_default_country_eligibility_values
        , JSON_QUERY_ARRAY(payload, '$.source_default_country_eligibility_evidence')
            AS source_default_country_eligibility_evidence
        , SAFE_CAST(JSON_VALUE(payload, '$.record_updated_at') AS TIMESTAMP) AS record_updated_at
        , TRIM(UPPER(JSON_VALUE(payload, '$.record_updated_by_step'))) AS record_updated_by_step
        , payload AS raw_payload
    FROM raw_job_facts
)

SELECT *
FROM renamed
