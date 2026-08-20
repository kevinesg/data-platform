{% macro wremotely_clean_job_title(title_expression, company_expression) -%}
arrayFold(
    (cleaned_title, suffix_index) -> multiIf(
        empty(trim(ifNull(cleaned_title, '')))
            or empty(trim(ifNull({{ company_expression }}, '')))
            , trim(cleaned_title)
        , endsWith(
            lowerUTF8(trim(cleaned_title))
            , concat(' - ', lowerUTF8(trim({{ company_expression }})))
        )
            , trim(substringUTF8(
                trim(cleaned_title)
                , 1
                , lengthUTF8(trim(cleaned_title))
                    - lengthUTF8(concat(' - ', trim({{ company_expression }})))
            ))
        , endsWith(
            lowerUTF8(trim(cleaned_title))
            , concat(' – ', lowerUTF8(trim({{ company_expression }})))
        )
            , trim(substringUTF8(
                trim(cleaned_title)
                , 1
                , lengthUTF8(trim(cleaned_title))
                    - lengthUTF8(concat(' – ', trim({{ company_expression }})))
            ))
        , endsWith(
            lowerUTF8(trim(cleaned_title))
            , concat(' — ', lowerUTF8(trim({{ company_expression }})))
        )
            , trim(substringUTF8(
                trim(cleaned_title)
                , 1
                , lengthUTF8(trim(cleaned_title))
                    - lengthUTF8(concat(' — ', trim({{ company_expression }})))
            ))
        , endsWith(
            lowerUTF8(trim(cleaned_title))
            , concat(' | ', lowerUTF8(trim({{ company_expression }})))
        )
            , trim(substringUTF8(
                trim(cleaned_title)
                , 1
                , lengthUTF8(trim(cleaned_title))
                    - lengthUTF8(concat(' | ', trim({{ company_expression }})))
            ))
        , trim(cleaned_title)
    )
    , range(8)
    , {{ title_expression }}
)
{%- endmacro %}
