with fixtures as (
    select
        'Head of Engineering | Example Co - Example Co' as source_title
        , 'Example Co' as company_name
        , 'Head of Engineering' as expected_title

    union all

    select
        'Data Engineer - Example Co - Example Co' as source_title
        , 'Example Co' as company_name
        , 'Data Engineer' as expected_title

    union all

    select
        'Analytics Engineer – Example Co – Example Co' as source_title
        , 'Example Co' as company_name
        , 'Analytics Engineer' as expected_title

    union all

    select
        'Platform Engineer — Example Co — Example Co' as source_title
        , 'Example Co' as company_name
        , 'Platform Engineer' as expected_title

    union all

    select
        'Engineering Manager at Example Co' as source_title
        , 'Example Co' as company_name
        , 'Engineering Manager at Example Co' as expected_title
),

actual as (
    select
        source_title
        , company_name
        , expected_title
        , {{ wremotely_clean_job_title('source_title', 'company_name') }} as actual_title
    from fixtures
)

select *
from actual
where actual_title != expected_title
