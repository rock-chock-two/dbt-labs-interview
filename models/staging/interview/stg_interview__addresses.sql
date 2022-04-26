with 

base as (

  select
    *,
    case
      when country_code = 'US' then 'US'
      when country_code != 'US' then 'International'
      else null
    end as country_type

  from `dbt-public.interview_task.addresses`

)

select * from base