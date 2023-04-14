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

    {# Cleanup entries from destination #}
    {# CRITICAL - make sure that Force full_sync is disabled in mirror mode until the last run is a success.
                - Enable Force Full_sync in mirror mode only if last run is a success else Disable Force full_sync.#}
    {% if var("destination_sync_mode") == "mirror" %}
        {%- set source_relation = adapter.get_relation(
            database=source("scratch", var("finalized_snapshot")).database,
            schema=source("scratch", var("finalized_snapshot")).schema,
            identifier=source("scratch", var("finalized_snapshot")).name,
        ) -%}
        {% set delete_records_required = source_relation is not none %}
        {% if delete_records_required %}

            {{ finalize_transit_snapshot() }}

            {# ID key cannot be changed by editing a Sync :: other columns can be changed#}
            {% set query %}
                CREATE TABLE {{ source('scratch', var('delete_snapshot')) }}  
                AS SELECT row_number() over (order by {{ var("id_key") }}) _valmi_row_num,
                'delete' AS _valmi_sync_op, {{ var("id_key") }}
                FROM {{ source("scratch", var("finalized_snapshot")) }}
            {% endset %}
            {% do run_query(query) %}
        {% endif %}
    {% endif %}


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
 
        {{ finalize_transit_snapshot() }}

    {% endif %}

    {% if var("destination_sync_mode") == "mirror" %}

        {# Have the delete table #}
        {% set query %}
            CREATE TABLE IF NOT EXISTS {{ source('scratch', var('delete_snapshot')) }}  
            AS 
            SELECT row_number() over (order by {{ var("id_key") }}) _valmi_row_num,
            'delete' AS _valmi_sync_op, {{ var("id_key") }}
            FROM {{ source("scratch", var("finalized_snapshot")) }}
            LIMIT 0
        {% endset %}
        {% do run_query(query) %}

    {% endif %}
    
{% endif %}

SELECT 0