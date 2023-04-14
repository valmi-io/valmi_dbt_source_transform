{%- set stg_snapshot = adapter.get_relation(
        database=source("scratch", var("stg_snapshot")).database,
        schema=source("scratch", var("stg_snapshot")).schema,
        identifier=source("scratch", var("stg_snapshot")).name,
    ) -%}

{% set stg_snapshot_present = stg_snapshot is not none %}

{# last stg_snapshot failed -- so work with old snapshot only until it succeeds to maintain consistent destination state#}
{% if var("previous_run_status") != "success" and var("destination_sync_mode") == "mirror" and stg_snapshot_present %}

    with old_snapshot as (
        select * from {{ source('scratch', var('stg_snapshot')) }}  
    )
    select * from old_snapshot

{% else %}
    with init as (
        select * from {{ ref( var("init")) }}
    )

    {% if var("destination_sync_mode") == "upsert" %}

        select 'upsert' AS _valmi_sync_op, ADDED.* from (
            select {{ ",".join(var("columns")) }}
            from {{ source("aliased_source", var("source_table")) }}

            except

            select {{ ",".join(var("columns")) }}
            from {{ source("scratch", var("finalized_snapshot")) }}
            ) ADDED

    {% elif var("destination_sync_mode") == "mirror" %}

        select 'upsert'  AS _valmi_sync_op, ADDED.* from (
            select {{ ",".join(var("columns")) }}
            from {{ source("aliased_source", var("source_table")) }}

            except

            select {{ ",".join(var("columns")) }}
            from {{ source("scratch", var("finalized_snapshot")) }}
            ) ADDED

        UNION ALL
        
        select 'delete', DELETED.* from (
            select {{ ",".join(var("columns")) }}
            from {{ source("scratch", var("finalized_snapshot")) }}

            except

            select {{ ",".join(var("columns")) }}
            from {{ source("aliased_source", var("source_table")) }}
            
            ) DELETED

    {% endif %}

{% endif %}