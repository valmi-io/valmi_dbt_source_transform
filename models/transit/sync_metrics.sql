with
    ignored_snapshot as (select * from {{ ref(var("ignored_snapshot")) }}),
    transit_snapshot as (select * from {{ ref(var("transit_snapshot")) }}),
    source_table as (select * from {{ source("aliased_source", var("source_table")) }}),
    stg_snapshot as (select * from  {{ ref(var("stg_snapshot")) }})

select count(*), 'invalid' , ignored_code
from ignored_snapshot
group by ignored_code

UNION ALL

select count(*), 'valid',0
from transit_snapshot

UNION ALL

select count(*), 'total',0
from source_table

UNION ALL

select count(*), 'new',0
from stg_snapshot
