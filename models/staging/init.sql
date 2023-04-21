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

{% if var("full_refresh") == "true" %}
    {% set do_full_refresh = true %}
{% else %}

    {%- set source_relation = adapter.get_relation(
        database=source("scratch", var("finalized_snapshot")).database,
        schema=source("scratch", var("finalized_snapshot")).schema,
        identifier=source("scratch", var("finalized_snapshot")).name,
    ) -%}

    {% set do_full_refresh = source_relation is none %}
{% endif %}


{% if do_full_refresh %}

    {# Cleanup entries from destination #}
    {# CRITICAL - make sure that Force full_sync is disabled in mirror mode until the last run is a success.
                - Enable Force Full_sync in mirror mode only if last run is a success else Disable Force full_sync.#}
    {% if var("destination_sync_mode") == "mirror" %}
        {%- set source_relation = adapter.get_relation(
            database=source("scratch", var("finalized_snapshot")).database,
            schema=source("scratch", var("finalized_snapshot")).schema,
            identifier=source("scratch", var("finalized_snapshot")).name,
        ) -%}
        
        {% set delete_records_required = source_relation is not none %}
        {% if delete_records_required %}

            {{ finalize_transit_snapshot() }}


            {# ID key cannot be changed by editing a Sync :: other columns can be changed#}
            {% set query %}
                DROP TABLE IF EXISTS {{ source('scratch', var('cleanup_snapshot')) }} 
            {% endset %}
            {% do run_query(query) %}

            {% set query %}
                CREATE TABLE {{ source('scratch', var('cleanup_snapshot')) }}  
                AS  SELECT {{ var("id_key") }} 
                FROM {{ source("scratch", var("finalized_snapshot")) }} 
            {% endset %}
            {% do run_query(query) %}
        {% endif %}

    {% endif %}


    {# drop the finalized snapshot and create again#}
    {% set query %}
        DROP TABLE IF EXISTS {{ source('scratch', var('finalized_snapshot')) }} CASCADE
    {% endset %}
    {% do run_query(query) %}
 
    {% set query %}
            CREATE TABLE {{ source('scratch', var('finalized_snapshot')) }}  
            AS SELECT {{ ",".join(var("columns")) }} 
            FROM {{ source("aliased_source", var("source_table")) }} 
            LIMIT 0
    {% endset %}
    {% do run_query(query) %}

{% else %}

    {% if var("previous_run_status") == "success" %}
    
        {{ finalize_transit_snapshot() }}

        {% set query %}
            DROP TABLE IF EXISTS {{ source('scratch', var('cleanup_snapshot')) }} 
        {% endset %}
        {% do run_query(query) %}

    {% endif %}
 
{% endif %}

SELECT 0 AS "init_column"