{{
    config(
        materialized='table'
    )
}}

with source as(

select distinct ID,NAME,TYPE,STATUS,current_timestamp() as campaign_load_time from {{source('DEVELOPER_DB','CAMPAIGN') }})

select ID,NAME,TYPE,STATUS,campaign_load_time from source