{% test no_company_suffix(model, column_name, company_column='company_name') %}

select
    {{ column_name }} as title
    , {{ company_column }} as company_name
from {{ model }}
where notEmpty(trim(ifNull({{ column_name }}, '')))
    and notEmpty(trim(ifNull({{ company_column }}, '')))
    and (
        endsWith(
            lowerUTF8(trim({{ column_name }}))
            , concat(' - ', lowerUTF8(trim({{ company_column }})))
        )
        or endsWith(
            lowerUTF8(trim({{ column_name }}))
            , concat(' – ', lowerUTF8(trim({{ company_column }})))
        )
        or endsWith(
            lowerUTF8(trim({{ column_name }}))
            , concat(' — ', lowerUTF8(trim({{ company_column }})))
        )
        or endsWith(
            lowerUTF8(trim({{ column_name }}))
            , concat(' | ', lowerUTF8(trim({{ company_column }})))
        )
    )

{% endtest %}
