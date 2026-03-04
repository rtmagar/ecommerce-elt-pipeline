
    
    

with child as (
    select seller_id as from_field
    from "analytics_warehouse"."public"."fact_sales"
    where seller_id is not null
),

parent as (
    select seller_id as to_field
    from "analytics_warehouse"."public"."dim_sellers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null


