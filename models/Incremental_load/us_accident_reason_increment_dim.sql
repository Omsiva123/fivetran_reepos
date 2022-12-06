{{
    config(
        materialized='incremental'
    )
}}

with source_2 as(

select distinct AMENITY,BUMP,CROSSING,GIVE_WAY,JUNCTION,NO_EXIT,RAILWAY,ROUNDABOUT,STATION,
STOP,TRAFFIC_CALMING,TRAFFIC_SIGNAL,TURNING_LOOP,_MODIFIED  from {{source('DEVELOPER_DB','CONVERT_DATE') }}),

source_data_dimention_2 as (

select CONCAT('R-',cast(reason_seq.nextval as varchar)) as reason_id,
AMENITY,BUMP,CROSSING,GIVE_WAY,JUNCTION,NO_EXIT,RAILWAY,ROUNDABOUT,STATION,STOP,TRAFFIC_CALMING,TRAFFIC_SIGNAL,TURNING_LOOP,
_MODIFIED  as dimention_Load_time from source_2)

select reason_id,AMENITY,BUMP,CROSSING,GIVE_WAY,JUNCTION,NO_EXIT,RAILWAY,ROUNDABOUT,STATION,STOP,TRAFFIC_CALMING,TRAFFIC_SIGNAL,TURNING_LOOP,
dimention_Load_time from source_data_dimention_2



