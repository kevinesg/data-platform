{% macro relation_has_columns(relation, required_column_names) %}
    {% if not execute or relation is none %}
        {{ return(false) }}
    {% endif %}

    {% set existing_column_names = [] %}
    {% for column in adapter.get_columns_in_relation(relation) %}
        {% do existing_column_names.append(column.name | lower) %}
    {% endfor %}

    {% for required_column_name in required_column_names %}
        {% set normalized_required_column_name = required_column_name | lower %}
        {% if normalized_required_column_name not in existing_column_names %}
            {{ return(false) }}
        {% endif %}
    {% endfor %}

    {{ return(true) }}
{% endmacro %}
