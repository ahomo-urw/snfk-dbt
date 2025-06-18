{% macro generate_schema_name(custom_schema_name, node) %}
    {% if custom_schema_name is not none %}
        {{ target.schema }}_{{ custom_schema_name }}
    {% else %}
        {{ target.schema }}
    {% endif %}
{% endmacro %}
