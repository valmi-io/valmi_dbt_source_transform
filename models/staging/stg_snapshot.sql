
{% if var("full_refresh") == "true" %}
    {% set query %}
        DROP TABLE IF EXISTS {{ source('scratch', var('snapshot')) }}    
    {% endset %}
    {% do run_query(query) %}

    {% set do_full_refresh = true %}
{% endif %}

{%- set source_relation = adapter.get_relation(
    database=source("scratch", var("snapshot")).database,
    schema=source("scratch", var("snapshot")).schema,
    identifier=source("scratch", var("snapshot")).name,
) -%}

{% set do_full_refresh = source_relation is none %}

{% if do_full_refresh %}

 select {{ ",".join(var("columns")) }}
        from {{ source("jaffle_shop", var("source")) }}
{% else %}



/* Pass a variable with previous sync_id and commit the transit_snapshot to finalised_snapshot before proceeding .. 
Ideally failed records at the destination should also be written to the warehouse in the finalise call of the DAG, 
and removed from the transit_snapshot before committing.

Also, In case of full_refresh, clean up all temporary tables -  transit , sync_metrics and ignored_snapshots also 
If there is a mode change to mirror mode, we need to get full_refresh flag to clean up stuff.
 */
select {{ ".".join(var("columns")) }}
from {{ source("jaffle_shop", var("source")) }}

except

select {{ ".".join(var("columns")) }}
from {{ source("scratch", var("snapshot")) }}

{% endif %}
