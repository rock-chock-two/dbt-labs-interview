with 

source as (

    select * from {{ source('interview', 'addresses') }}

),

base as (

    select
        *,
        case
            when country_code = 'US' then 'US'
            when country_code != 'US' then 'International'
            else 'unknown'
        end as country_type

    from source

)

select * from base