{{ config(materialized='table') }}
with source_1 as(

select distinct SUNRISE_SUNSET,CIVIL_TWILIGHT,NAUTICAL_TWILIGHT,ASTRONOMICAL_TWILIGHT from {{source('DEVELOPER_DB','US_ACCIDENT_STAGE_TABLE') }}),

source_data_dimention_1 as (

select row_number() over(order by CIVIL_TWILIGHT desc) as dimention_id,CONCAT('D-',cast(dimention_id as varchar)) as dron_id,
SUNRISE_SUNSET,CIVIL_TWILIGHT,NAUTICAL_TWILIGHT,ASTRONOMICAL_TWILIGHT,current_timestamp() as dron_Load_time from source_1)

select dron_id,SUNRISE_SUNSET,CIVIL_TWILIGHT,NAUTICAL_TWILIGHT,ASTRONOMICAL_TWILIGHT,dron_Load_time from source_data_dimention_1