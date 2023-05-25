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
{%- set stg_snapshot = adapter.get_relation(
        database=source("scratch", var("stg_snapshot")).database,
        schema=source("scratch", var("stg_snapshot")).schema,
        identifier=source("scratch", var("stg_snapshot")).name,
    ) -%}

{% set stg_snapshot_present = stg_snapshot is not none %}
 
{# last stg_snapshot failed -- so work with old snapshot only until it succeeds to maintain consistent destination state#}
{% if var("previous_run_status") != "success" and var("destination_sync_mode") == "mirror" and stg_snapshot_present %}

    with old_snapshot as (
        select * from {{ source('scratch', var('stg_snapshot')) }}  
    )
    select * from old_snapshot

{% else %}
    with init as (
        select * from {{ ref( var("init")) }}
    )

    {% if var("destination_sync_mode") in ("upsert" ,'append','create','update') %}

        select '{{  var("destination_sync_mode") }}'  AS _valmi_sync_op, ADDED.* from (
            select {{ ",".join(var("columns")) }}
            from {{ source("aliased_source", var("source_table")) }}

            except

            select {{ ",".join(var("columns")) }}
            from {{ source("scratch", var("finalized_snapshot")) }}
            ) ADDED


    {% elif var("destination_sync_mode") == "mirror" %}

        select 'upsert'  AS _valmi_sync_op, ADDED.* from (
            select {{ ",".join(var("columns")) }}
            from {{ source("aliased_source", var("source_table")) }}

            except

            select {{ ",".join(var("columns")) }}
            from {{ source("scratch", var("finalized_snapshot")) }}
            ) ADDED

        UNION ALL
         
        select 'delete'  AS _valmi_sync_op, DELETED.* from (
            select {{ ",".join(var("columns")) }}
            from {{ source("scratch", var("finalized_snapshot")) }}

            except

            select {{ ",".join(var("columns")) }}
            from {{ source("aliased_source", var("source_table")) }}
            
            ) DELETED

    {% endif %}

{% endif %}