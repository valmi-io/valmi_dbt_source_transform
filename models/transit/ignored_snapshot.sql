with stg_snapshot as (select * from {{  ref(var("stg_snapshot")) }})
select {{ ",".join(var("columns")) }}, 400 AS ignored_code
from stg_snapshot
where
    {{ var("id_key") }}  in (
        select {{ var("id_key") }}
        from
            (
                select count(*), {{ var("id_key") }}
                from stg_snapshot
                group by {{ var("id_key") }}
                having count(*) > 1
            ) AS ID_COUNTS
    )
