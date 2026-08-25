{% macro wremotely_canonical_candidate_url(url_expr, source_platform_expr="''") -%}
if(
    nullIf(trim(ifNull({{ url_expr }}, '')), '') is not null
    and (
        lowerUTF8(ifNull({{ source_platform_expr }}, '')) = 'ashby'
        or lowerUTF8(ifNull({{ url_expr }}, '')) like 'https://jobs.ashbyhq.com/%'
    )
    , lowerUTF8({{ url_expr }})
    , {{ url_expr }}
)
{%- endmacro %}

{% macro wremotely_canonical_candidate_id(
    candidate_id_expr,
    url_expr,
    source_platform_expr="''"
) -%}
if(
    nullIf(trim(ifNull({{ url_expr }}, '')), '') is not null
    and (
        lowerUTF8(ifNull({{ source_platform_expr }}, '')) = 'ashby'
        or lowerUTF8(ifNull({{ url_expr }}, '')) like 'https://jobs.ashbyhq.com/%'
    )
    , lower(hex(SHA256({{ wremotely_canonical_candidate_url(url_expr, source_platform_expr) }})))
    , {{ candidate_id_expr }}
)
{%- endmacro %}
