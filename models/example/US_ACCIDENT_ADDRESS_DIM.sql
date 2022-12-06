with source as(

select distinct CITY,COUNTRY,STATE,ZIPCODE,COUNTY,TIMEZONE,AIRPORT_CODE,STREET from {{source('DEVELOPER_DB','US_ACCIDENT_STAGE_TABLE') }}),

source_data_dimention as (

select row_number() over(order by CITY) as Row_ID,CONCAT('A-',cast(Row_ID as varchar)) as ADD_ID,CITY,COUNTRY,STATE,ZIPCODE,COUNTY,TIMEZONE,AIRPORT_CODE,STREET,
current_timestamp() as Load_time from source)

select ADD_ID,CITY,COUNTRY,STATE,ZIPCODE,COUNTY,TIMEZONE,AIRPORT_CODE,STREET,Load_time from source_data_dimention
