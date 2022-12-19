{{config(materialized='incremental')}}


with source_1 as(

select distinct SUNRISE_SUNSET,CIVIL_TWILIGHT,NAUTICAL_TWILIGHT,ASTRONOMICAL_TWILIGHT,_MODIFIED from {{source('DEVELOPER_DB','INCREMENTAL_US_ACCIDENT_STAGE_TABLE') }}),

source_data_dimention_1 as (

select CONCAT('D-',cast(dron_seq.nextval as varchar)) as dron_id,
SUNRISE_SUNSET,CIVIL_TWILIGHT,NAUTICAL_TWILIGHT,ASTRONOMICAL_TWILIGHT,_MODIFIED as dimention_Load_time from source_1)

select dron_id,SUNRISE_SUNSET,CIVIL_TWILIGHT,NAUTICAL_TWILIGHT,ASTRONOMICAL_TWILIGHT,dimention_Load_time from source_data_dimention_1