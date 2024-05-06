/*
 * Copyright (c) 2024 valmi.io <https://github.com/valmi-io>
 * 
 * Created Date: Wednesday, March 22nd 2023, 12:15:26 pm
 * Author: Rajashekar Varkala @ valmi.io
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

with init as ( -- This is to trigger init model before running this model
        select * from {{ ref( var("init")) }}
    ),  source as (select * from {{ source("aliased_source", var("source_table")) }})

{# Duplicate keys : Code -100 #}
select {{ quote(var("columns")) }} , -100 AS error_code 
from source
where
    {{ quote_single(var("id_key")) }}  in (
        select {{ quote_single(var("id_key")) }}
        from
            (
                select count(*), {{ quote_single(var("id_key")) }}
                from source
                group by {{ quote_single(var("id_key")) }}
                having count(*) > 1
            ) AS ID_COUNTS
    )
    and 
    {{ var("id_key") }} IS NOT NULL 

UNION ALL

{# Null Keys : Code -120 #}
select {{ quote(var("columns")) }} , -120 AS error_code 
from source
where  {{ quote_single(var("id_key")) }} IS  NULL



