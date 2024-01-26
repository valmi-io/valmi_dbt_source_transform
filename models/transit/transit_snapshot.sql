/*
 * Copyright (c) 2024 valmi.io <https://github.com/valmi-io>
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

{% set cleanup_relation_available = False %}

{% if var("destination_sync_mode") == "mirror" %}
    {%- set cleanup_relation = adapter.get_relation(
                database=source("scratch", var("cleanup_snapshot")).database,
                schema=source("scratch", var("cleanup_snapshot")).schema,
                identifier=source("scratch", var("cleanup_snapshot")).name,
        ) -%}
            
    {% set cleanup_relation_available = cleanup_relation is not none %}
{% endif %}

{# columns array without id_key #}
{% set columns_arr =  var("columns") | select("ne",  var("id_key")) | list  %}
{% set null_arr = ["NULL"] *  columns_arr   |  length %} 
{% set _ =  null_arr.insert(0, var("id_key")) %}

with
    stg_snapshot as (select * from {{  ref(var("stg_snapshot")) }}),
    ignored_snapshot as (select * from {{ ref(var("ignored_snapshot")) }})

{% if cleanup_relation_available %}
    , cleanup_snapshot as (select * from {{ source('scratch', var('cleanup_snapshot')) }})
{% endif %}

select
    row_number() over (order by CASE WHEN _valmi_sync_op = 'delete' THEN 1
              WHEN _valmi_sync_op = 'upsert' THEN 2 
              WHEN _valmi_sync_op = 'append' THEN 3
              WHEN _valmi_sync_op =  'create' THEN 4 
              WHEN _valmi_sync_op =  'update' THEN 5
              ELSE 6 END, {{ quote_single(var("id_key")) }}) _valmi_row_num, COMBINED.* FROM

    (   select   _valmi_sync_op, {{ quote(var("columns")) }}
            from stg_snapshot
            where {{ quote_single(var("id_key")) }} not in 
                (select {{ quote_single(var("id_key")) }} 
                from ignored_snapshot 
                where  {{ quote_single(var("id_key")) }} is not NULL)

    {% if cleanup_relation_available %}   
        UNION ALL

        select 'delete' AS _valmi_sync_op ,  {{ ",".join(null_arr)   }}
        from cleanup_snapshot
    {% endif %}
    ) AS COMBINED

    ORDER BY _valmi_row_num ASC
 