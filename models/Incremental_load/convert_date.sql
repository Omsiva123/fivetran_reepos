{{ config(materialized='table') }}

select to_number(to_char(START_TIME, 'yyyymmdd')) as DATE_ID,* from {{source('DEVELOPER_DB','US_ACCIDENT_TABLE') }}