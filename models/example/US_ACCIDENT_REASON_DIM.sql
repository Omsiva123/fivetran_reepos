{{ config(materialized='table') }}

with source_2 as(

select distinct AMENITY,BUMP,CROSSING,GIVE_WAY,JUNCTION,NO_EXIT,RAILWAY,ROUNDABOUT,STATION,
STOP,TRAFFIC_CALMING,TRAFFIC_SIGNAL,TURNING_LOOP from {{source('DEVELOPER_DB','US_ACCIDENT_STAGE_TABLE') }}),

source_data_dimention_2 as (

select row_number() over(order by AMENITY desc) as dimention_id_1,CONCAT('R-',cast(dimention_id_1 as varchar)) as reason_id,
AMENITY,BUMP,CROSSING,GIVE_WAY,JUNCTION,NO_EXIT,RAILWAY,ROUNDABOUT,STATION,STOP,TRAFFIC_CALMING,TRAFFIC_SIGNAL,TURNING_LOOP,
current_timestamp() as reason_Load_time from source_2)

select reason_id,AMENITY,BUMP,CROSSING,GIVE_WAY,JUNCTION,NO_EXIT,RAILWAY,ROUNDABOUT,STATION,STOP,TRAFFIC_CALMING,TRAFFIC_SIGNAL,TURNING_LOOP,
reason_Load_time from source_data_dimention_2