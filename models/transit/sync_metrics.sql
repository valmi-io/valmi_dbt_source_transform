/*
 * Copyright (c) 2024 valmi.io <https://github.com/valmi-io>
 * 
 * Created Date: Wednesday, March 22nd 2023, 12:43:05 pm
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
where _valmi_sync_op IN ('upsert','append', 'create','update')

UNION ALL

select count(*), 'total' as kind,0 as error_code
from source_table

UNION ALL

select count(*), 'new' as kind,0 as error_code
from stg_snapshot
where _valmi_sync_op IN ('upsert','append', 'create','update')
