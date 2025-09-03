{% macro get_schema(model) %}
    {% set path_str = model.path | string %}
    {% set folder = path_str.split('/') | first %}
    {% do log("Schema folder: " ~ folder, info=True) %}
    {{ folder }}
{% endmacro %}