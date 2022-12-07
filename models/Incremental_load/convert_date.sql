{{ config(post_hook="truncate table {{source('DEVELOPER_DB','ACCIDENT_TABLE_INCREMENTAL')}}",
materialized='table') }}
select to_number(to_char(START_TIME, 'yyyymmdd')) as date_id,* from {{source('DEVELOPER_DB','ACCIDENT_TABLE_INCREMENTAL')}}