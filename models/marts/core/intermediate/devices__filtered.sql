-- configured as ephemeral, see dbt_project.yml

with 

base as (

    select * from {{ ref('stg_interview__devices') }}
),

final as (

    select distinct
        type_id as order_id,

        first_value(device) over (
            partition by type_id
            order by created_at
        ) as device,

        device_type

    from base
    where type = 'order'

)

select * from final