{{config(materialized='table')}}
select to_number(to_char(START_TIME, 'yyyymmdd')) as date_id,* from {{source('DEVELOPER_DB','INCREMENTAL_US_ACCIDENT_STAGE_TABLE')}}