with source as (select * from {{ source("aliased_source", var("source_table")) }})

{# Duplicate keys : Code -100 #}
select {{ ",".join(var("columns")) }}, -100 AS error_code 
from source
where
    {{ var("id_key") }}  in (
        select {{ var("id_key") }}
        from
            (
                select count(*), {{ var("id_key") }}
                from source
                group by {{ var("id_key") }}
                having count(*) > 1
            ) AS ID_COUNTS
    )
    and 
    {{ var("id_key") }} IS NOT NULL 

UNION ALL

{# Null Keys : Code -120 #}
select {{ ",".join(var("columns")) }}, -120 AS error_code 
from source
where  {{ var("id_key") }} IS  NULL



