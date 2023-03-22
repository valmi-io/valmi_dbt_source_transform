with
    ignored_snapshot as (select * from {{ ref(var("ignored_snapshot")) }}),
    transit_snapshot as (select * from {{ ref(var("transit_snapshot")) }}),
    source_table as (select * from {{ source("aliased_source", var("source_table")) }}),
    stg_snapshot as (select * from  {{ ref(var("stg_snapshot")) }})

select count(*), 'invalid' as kind, error_code
from ignored_snapshot
group by error_code

UNION ALL

select count(*), 'valid' as kind,0 as error_code
from transit_snapshot

UNION ALL

select count(*), 'total' as kind,0 as error_code
from source_table

UNION ALL

select count(*), 'new' as kind,0 as error_code
from stg_snapshot
