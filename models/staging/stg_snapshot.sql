with init as (
    select * from {{ ref( var("init")) }}
)
select {{ ",".join(var("columns")) }}
from {{ source("aliased_source", var("source_table")) }}

except all

select {{ ",".join(var("columns")) }}
from {{ source("scratch", var("finalized_snapshot")) }}