with
    stg_snapshot as (select * from {{ ref(var("stg_snapshot")) }}),
    ignored_snapshot as (select * from {{ ref(var("ignored_snapshot")) }})

select {{ ",".join(var("columns")) }}
from stg_snapshot
where {{ var("id_key") }} not in (select {{ var("id_key") }} from ignored_snapshot)