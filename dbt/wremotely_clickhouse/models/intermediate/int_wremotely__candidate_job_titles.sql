{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='candidate_id',
    on_schema_change='append_new_columns',
    order_by="(ifNull(candidate_id, ''))"
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['source_updated_at']) %}

with candidate_keys as (
    select
        candidate_id
        , dbt_updated_at as source_updated_at
    from {{ ref('int_wremotely__latest_job_facts') }}
    {% if incremental_watermark_ready %}
    where dbt_updated_at > (
        select coalesce(max(source_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}

    union all

    select
        candidate_id
        , dbt_updated_at as source_updated_at
    from {{ ref('int_wremotely__latest_selected_job_urls') }}
    {% if incremental_watermark_ready %}
    where dbt_updated_at > (
        select coalesce(max(source_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% endif %}
),

candidate_key_rollup as (
    select
        candidate_id
        , max(source_updated_at) as source_updated_at
    from candidate_keys
    group by candidate_id
),

job_facts as (
    select
        facts.candidate_id
        , facts.raw_payload
    from {{ ref('int_wremotely__latest_job_facts') }} as facts
    inner join candidate_key_rollup as changed
        on facts.candidate_id = changed.candidate_id
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

selected_job_urls as (
    select
        selected.candidate_id
        , selected.source_link_title_candidate
        , selected.source_link_title_candidate_status
        , selected.source_link_text
        , selected.source_link_text_char_count
    from {{ ref('int_wremotely__latest_selected_job_urls') }} as selected
    inner join candidate_key_rollup as changed
        on selected.candidate_id = changed.candidate_id
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
        , k.source_updated_at
    from candidate_key_rollup as k
    left join preferred_page_titles as p
        on k.candidate_id = p.candidate_id
    left join selected_job_urls as s
        on k.candidate_id = s.candidate_id
)

select *
from final
