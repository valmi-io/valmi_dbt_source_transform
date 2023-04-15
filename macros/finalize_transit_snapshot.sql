{% macro finalize_transit_snapshot() %}
    {%- set transit_relation = adapter.get_relation(
        database=source("scratch", var("transit_snapshot")).database,
        schema=source("scratch", var("transit_snapshot")).schema,
        identifier=source("scratch", var("transit_snapshot")).name,
    ) -%}

    {% if transit_relation is not none %}

        {% if var("destination_sync_mode") == "mirror" %}
            {% set query %}
                DELETE FROM {{ source('scratch', var('finalized_snapshot')) }} WHERE {{ var("id_key") }} in 
                (SELECT  {{ var("id_key") }}   
                    FROM  {{ source('scratch', var('transit_snapshot')) }}
                    WHERE _valmi_sync_op IN ('delete')
                )
            {% endset %}
            {% do run_query(query) %}
        {% endif %}

        {# delete the last transit snapshot keys#}
        {% set query %}
            DELETE FROM {{ source('scratch', var('finalized_snapshot')) }} WHERE {{ var("id_key") }} in 
            (SELECT  {{ var("id_key") }}   
                FROM  {{ source('scratch', var('transit_snapshot')) }}
                WHERE _valmi_sync_op IN ('upsert')
            )
        {% endset %}
        {% do run_query(query) %}

        {# insert the new transit snapshot keys#}
        {% set query %}
            INSERT INTO {{ source('scratch', var('finalized_snapshot')) }} 
            SELECT {{ ",".join(var("columns")) }} 
            FROM {{ source('scratch', var('transit_snapshot')) }}
            WHERE _valmi_sync_op IN ('upsert')
        {% endset %}
        {% do run_query(query) %}


        
    {% endif %}


    {# Keep this last to make the operations repeatable#}
    {% set query %}
        DROP TABLE IF EXISTS {{ source('scratch', var('cleanup_snapshot')) }} CASCADE
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}
