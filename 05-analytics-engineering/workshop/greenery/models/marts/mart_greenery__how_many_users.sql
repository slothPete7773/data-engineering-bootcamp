with source as (
    select * 
    from {{ ref('stg_greenery__users') }}
)
, summarized as (
    select 
        count(user_guid) as count_users
    from source
)
select * from summarized