with source as (
    select 
        *
    from {{ ref("fact_movements") }}
)
, final as (
    SELECT 
        train_id as TID
        , variation_status as status
        , count(variation_status) as countOffRoute
    FROM source
    group by train_id, variation_status
    having variation_status="OFF ROUTE"
    order by countOffRoute desc

)
select *
from final