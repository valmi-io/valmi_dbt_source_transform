/*
 * Copyright (c) 2023 valmi.io <https://github.com/valmi-io>
 * 
 * Created Date: Wednesday, March 22nd 2023, 12:49:11 am
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
    stg_snapshot as (select * from {{  ref(var("stg_snapshot")) }}),
    ignored_snapshot as (select * from {{ ref(var("ignored_snapshot")) }})

select
    row_number() over (order by CASE WHEN _valmi_sync_op = 'delete' THEN 1
              WHEN _valmi_sync_op = 'upsert' THEN 2 
              ELSE 4 END, {{ var("id_key") }}) _valmi_row_num,
    _valmi_sync_op,
    {{ ",".join(var("columns")) }}
from stg_snapshot
where {{ var("id_key") }} not in 
    (select {{ var("id_key") }} 
    from ignored_snapshot 
    where  {{ var("id_key") }} is not NULL)
