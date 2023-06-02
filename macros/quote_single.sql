{% macro quote_single(i) %}
    {{ return("\"%s\""|format(i)) }}   
{% endmacro %}