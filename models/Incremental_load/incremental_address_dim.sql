{{config(materialized='incremental')}}
with source as(

select distinct CITY,COUNTRY,STATE,ZIPCODE,COUNTY,TIMEZONE,AIRPORT_CODE,STREET,_MODIFIED from {{source('DEVELOPER_DB','INCREMENTAL_US_ACCIDENT_STAGE_TABLE') }}),

source_data_dimention as (

select CONCAT('A-',cast(addres_seq.nextval as varchar)) as ADD_ID,CITY,COUNTRY,STATE,ZIPCODE,COUNTY,TIMEZONE,AIRPORT_CODE,STREET,
_MODIFIED as dimention_Load_time from source)

select ADD_ID,CITY,COUNTRY,STATE,ZIPCODE,COUNTY,TIMEZONE,AIRPORT_CODE,STREET,dimention_Load_time from source_data_dimention

