with
    ignored_snapshot as (select * from {{ ref(var("ignored_snapshot")) }}),
    transit_snapshot as (select * from {{ ref(var("transit_snapshot")) }})

select count(*), ignored_code
from ignored_snapshot
group by ignored_code

union all

select count(*), 200
from transit_snapshot
