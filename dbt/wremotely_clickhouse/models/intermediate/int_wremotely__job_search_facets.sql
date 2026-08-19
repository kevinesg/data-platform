{{ config(
    materialized='incremental',
    incremental_strategy='delete_insert',
    unique_key='job_id',
    on_schema_change='append_new_columns',
    order_by="(ifNull(job_id, ''))"
) }}

{% set incremental_watermark_ready = is_incremental()
    and relation_has_columns(this, ['dbt_updated_at']) %}
{% set title_cleanup_version_ready = is_incremental()
    and relation_has_columns(this, ['title_cleanup_version']) %}

with publishable_jobs as (
    select *
    from {{ ref('int_wremotely__publishable_job_facts') }}
    {% if incremental_watermark_ready %}
    where dbt_updated_at > (
        select coalesce(max(dbt_updated_at), toDateTime64('1970-01-01 00:00:00', 3))
        from {{ this }}
    )
    {% if title_cleanup_version_ready %}
    or title_cleanup_version != {{ wremotely_title_cleanup_version() }}
    {% else %}
    or 1 = 1
    {% endif %}
    {% endif %}
),

employment_values as (
    select
        job_id
        , arrayJoin(arrayConcat(
            arrayMap(
                item -> JSONExtractString(item, 'value')
                , ifNull(raw_employment_type_values, [])
            )
            , [ifNull(raw_employment_type, '')]
        )) as raw_value
    from publishable_jobs
),

normalized_employment_values as (
    select
        job_id
        , replaceRegexpAll(upperUTF8(trim(raw_value)), '[^A-Z0-9]+', ' ')
            as normalized_raw_value
    from employment_values
    where notEmpty(trim(raw_value))
),

employment_types as (
    select
        job_id
        , arraySort(arrayFilter(
            value -> notEmpty(value)
            , groupUniqArray(multiIf(
                match(normalized_raw_value, '(^| )FULL ?TIME($| )'), 'FULL_TIME'
                , match(normalized_raw_value, '(^| )PART ?TIME($| )'), 'PART_TIME'
                , match(normalized_raw_value, '(^| )(CONTRACT|CONTRACTOR|FREELANCE)($| )'), 'CONTRACTOR'
                , match(normalized_raw_value, '(^| )(TEMP|TEMPORARY|SEASONAL)($| )'), 'TEMPORARY'
                , match(normalized_raw_value, '(^| )(INTERN|INTERNSHIP)($| )'), 'INTERN'
                , match(normalized_raw_value, '(^| )VOLUNTEER($| )'), 'VOLUNTEER'
                , match(normalized_raw_value, '(^| )PER ?DIEM($| )'), 'PER_DIEM'
                , ''
            ))
        )) as employment_types
    from normalized_employment_values
    group by job_id
),

search_text as (
    select
        job_id
        , replaceRegexpAll(lowerUTF8(concat(
            ifNull(title, '')
            , ' '
            , ifNull(company_name, '')
            , ' '
            , ifNull(job_description, '')
        )), '[^a-z0-9]+', ' ') as normalized_search_text
    from publishable_jobs
),

search_tags as (
    select
        search.job_id
        , arraySort(groupUniqArray(taxonomy.tag_code)) as search_tags
    from search_text as search
    cross join {{ ref('wremotely__search_tags') }} as taxonomy
    where match(search.normalized_search_text, taxonomy.match_pattern)
    group by search.job_id
)

select
    jobs.job_id as job_id
    , ifNull(employment.employment_types, []) as employment_types
    , ifNull(tags.search_tags, []) as search_tags
    , jobs.title_cleanup_version
    , jobs.dbt_updated_at as dbt_updated_at
from publishable_jobs as jobs
left join employment_types as employment
    on jobs.job_id = employment.job_id
left join search_tags as tags
    on jobs.job_id = tags.job_id
