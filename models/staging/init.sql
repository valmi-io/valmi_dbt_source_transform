{% if var("full_refresh") == "true" %}
    {% set do_full_refresh = true %}
{% else %}

    {%- set source_relation = adapter.get_relation(
        database=source("scratch", var("finalized_snapshot")).database,
        schema=source("scratch", var("finalized_snapshot")).schema,
        identifier=source("scratch", var("finalized_snapshot")).name,
    ) -%}

    {% set do_full_refresh = source_relation is none %}
{% endif %}


{% if do_full_refresh %}
    {# drop the finalized snapshot and create again#}
    {% set query %}
        DROP TABLE IF EXISTS {{ source('scratch', var('finalized_snapshot')) }} CASCADE
    {% endset %}
    {% do run_query(query) %}
 
    {% set query %}
            CREATE TABLE {{ source('scratch', var('finalized_snapshot')) }}  
            AS SELECT {{ ",".join(var("columns")) }} 
            FROM {{ source("aliased_source", var("source_table")) }} 
            LIMIT 0
    {% endset %}
    {% do run_query(query) %}

{% else %}

    {% if var("previous_run_status") == "success" %}

        {# delete the last transit snapshot keys#}
        {% set query %}
            DELETE FROM {{ source('scratch', var('finalized_snapshot')) }} WHERE {{ var("id_key") }} in 
            (SELECT  {{ var("id_key") }}   FROM  {{ source('scratch', var('transit_snapshot')) }})
        {% endset %}
        {% do run_query(query) %}

        {# insert the new transit snapshot keys#}
        {% set query %}
            INSERT INTO {{ source('scratch', var('finalized_snapshot')) }} SELECT {{ ",".join(var("columns")) }} FROM {{ source('scratch', var('transit_snapshot')) }}
        {% endset %}
        {% do run_query(query) %}
 
    {% endif %}
{% endif %}
SELECT 0