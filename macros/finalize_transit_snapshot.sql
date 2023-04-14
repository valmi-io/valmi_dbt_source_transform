{% macro finalize_transit_snapshot() %}
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

    {# Keep this last to make the operations repeatable#}
    {% set query %}
        DROP TABLE IF EXISTS {{ source('scratch', var('delete_snapshot')) }} CASCADE
    {% endset %}
    {% do run_query(query) %}
{% endmacro %}
