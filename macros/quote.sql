{% macro quote(arr) %}
    {% set items = [] %}
    {% for i in arr %}
        {% set item = "\"%s\""|format(i) %}
        {% set _ = items.append(item) %}
    {% endfor %}
    {{ return(",".join(items)) }}   
{% endmacro %}