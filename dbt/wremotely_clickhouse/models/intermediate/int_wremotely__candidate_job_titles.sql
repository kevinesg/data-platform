{{ config(materialized='table') }}

with job_facts as (
    select
        candidate_id
        , raw_payload
    from {{ ref('int_wremotely__latest_job_facts') }}
),

typed_page_title_evidence as (
    select
        jf.candidate_id
        , title_evidence_index
        , nullIf(trim(JSONExtractString(title_evidence, 'value')), '') as title
        , nullIf(upper(JSONExtractString(title_evidence, 'source')), '') as title_source
        , case upper(JSONExtractString(title_evidence, 'source'))
            when 'JSONLD' then 1
            when 'HTML_META_OG_TITLE' then 2
            when 'HTML_META_TWITTER_TITLE' then 3
        end as title_source_priority
    from job_facts as jf
    array join
        arrayEnumerate(JSONExtractArrayRaw(jf.raw_payload, 'raw_title_values'))
            as title_evidence_index
        , JSONExtractArrayRaw(jf.raw_payload, 'raw_title_values') as title_evidence
),

ranked_page_titles as (
    select
        *
        , row_number() over (
            partition by candidate_id
            order by title_source_priority, title_evidence_index
        ) as title_rank
    from typed_page_title_evidence
    where title_source_priority is not null
        and lengthUTF8(title) <= 500
),

preferred_page_titles as (
    select
        candidate_id
        , title
        , title_source
    from ranked_page_titles
    where title_rank = 1
),

candidate_keys as (
    select candidate_id
    from {{ ref('int_wremotely__latest_job_facts') }}

    union distinct

    select candidate_id
    from {{ ref('int_wremotely__latest_selected_job_urls') }}
),

selected_job_urls as (
    select
        candidate_id
        , source_link_title_candidate
        , source_link_title_candidate_status
        , source_link_text
        , source_link_text_char_count
    from {{ ref('int_wremotely__latest_selected_job_urls') }}
),

final as (
    select
        k.candidate_id as candidate_id
        , coalesce(
            p.title
            , case
                when s.source_link_title_candidate_status = 'ACCEPTED'
                    and lengthUTF8(nullIf(trim(s.source_link_title_candidate), '')) <= 500
                    then nullIf(trim(s.source_link_title_candidate), '')
                when s.source_link_title_candidate_status is null
                    and s.source_link_title_candidate is null
                    and s.source_link_text_char_count is null
                    and lengthUTF8(nullIf(trim(s.source_link_text), '')) <= 500
                    then nullIf(trim(s.source_link_text), '')
            end
        ) as title
        , coalesce(
            p.title_source
            , case
                when s.source_link_title_candidate_status = 'ACCEPTED'
                    and lengthUTF8(nullIf(trim(s.source_link_title_candidate), '')) <= 500
                    then 'LINK_TITLE_CANDIDATE'
                when s.source_link_title_candidate_status is null
                    and s.source_link_title_candidate is null
                    and s.source_link_text_char_count is null
                    and lengthUTF8(nullIf(trim(s.source_link_text), '')) <= 500
                    then 'LEGACY_BOUNDED_LINK_TEXT'
            end
        ) as title_source
    from candidate_keys as k
    left join preferred_page_titles as p
        on k.candidate_id = p.candidate_id
    left join selected_job_urls as s
        on k.candidate_id = s.candidate_id
)

select *
from final
