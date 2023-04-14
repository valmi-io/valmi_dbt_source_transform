with
    stg_snapshot as (select * from {{  ref(var("stg_snapshot")) }}),
    ignored_snapshot as (select * from {{ ref(var("ignored_snapshot")) }})

select
    row_number() over (order by {{ var("id_key") }}) _valmi_row_num,
    _valmi_sync_op,
    {{ ",".join(var("columns")) }}
from stg_snapshot
where {{ var("id_key") }} not in (select {{ var("id_key") }} from ignored_snapshot)
