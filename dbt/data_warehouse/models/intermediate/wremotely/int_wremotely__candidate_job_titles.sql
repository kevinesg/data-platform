WITH job_facts AS (
    SELECT *
    FROM {{ ref('int_wremotely__latest_job_facts') }}
),

selected_job_urls AS (
    SELECT *
    FROM {{ ref('int_wremotely__latest_selected_job_urls') }}
),

candidate_keys AS (
    SELECT candidate_id
    FROM job_facts

    UNION DISTINCT

    SELECT candidate_id
    FROM selected_job_urls
),

typed_page_title_evidence AS (
    SELECT
        jf.candidate_id
        , title_evidence_index
        , NULLIF(TRIM(JSON_VALUE(title_evidence, '$.value')), '') AS title
        , TRIM(UPPER(JSON_VALUE(title_evidence, '$.source'))) AS title_source
        , CASE TRIM(UPPER(JSON_VALUE(title_evidence, '$.source')))
            WHEN 'JSONLD' THEN 1
            WHEN 'HTML_META_OG_TITLE' THEN 2
            WHEN 'HTML_META_TWITTER_TITLE' THEN 3
        END AS title_source_priority
    FROM job_facts AS jf
    CROSS JOIN UNNEST(
        COALESCE(jf.latest_job_fact_raw_title_values, ARRAY<JSON>[])
    ) AS title_evidence WITH OFFSET AS title_evidence_index
),

ranked_page_titles AS (
    SELECT
        *
        , ROW_NUMBER() OVER (
            PARTITION BY candidate_id
            ORDER BY title_source_priority, title_evidence_index
        ) AS title_rank
    FROM typed_page_title_evidence
    WHERE title_source_priority IS NOT NULL
        AND CHAR_LENGTH(title) <= 500
),

preferred_page_titles AS (
    SELECT
        candidate_id
        , title
        , title_source
    FROM ranked_page_titles
    WHERE title_rank = 1
),

final AS (
    SELECT
        k.candidate_id
        , COALESCE(
            p.title
            , CASE
                WHEN s.source_link_title_candidate_status = 'ACCEPTED'
                    AND CHAR_LENGTH(NULLIF(TRIM(s.source_link_title_candidate), '')) <= 500
                    THEN NULLIF(TRIM(s.source_link_title_candidate), '')
                WHEN s.source_link_title_candidate_status IS NULL
                    AND s.source_link_title_candidate IS NULL
                    AND s.source_link_text_char_count IS NULL
                    AND CHAR_LENGTH(NULLIF(TRIM(s.source_link_text), '')) <= 500
                    THEN NULLIF(TRIM(s.source_link_text), '')
            END
        ) AS title
        , COALESCE(
            p.title_source
            , CASE
                WHEN s.source_link_title_candidate_status = 'ACCEPTED'
                    AND CHAR_LENGTH(NULLIF(TRIM(s.source_link_title_candidate), '')) <= 500
                    THEN 'LINK_TITLE_CANDIDATE'
                WHEN s.source_link_title_candidate_status IS NULL
                    AND s.source_link_title_candidate IS NULL
                    AND s.source_link_text_char_count IS NULL
                    AND CHAR_LENGTH(NULLIF(TRIM(s.source_link_text), '')) <= 500
                    THEN 'LEGACY_BOUNDED_LINK_TEXT'
            END
        ) AS title_source
    FROM candidate_keys AS k
    LEFT JOIN preferred_page_titles AS p
        ON k.candidate_id = p.candidate_id
    LEFT JOIN selected_job_urls AS s
        ON k.candidate_id = s.candidate_id
)

SELECT *
FROM final
