with

base as (

    select
        device,
        created_at,
        updated_at,
        type,
        safe_cast(type_id as int64) as type_id,
        
        case
            when device = 'web' then 'desktop'
            when device in ('ios-app', 'android') then 'mobile-app'
            when device in ('mobile', 'tablet') then 'mobile-web'
            else 'unknown'
        end as device_type

    from `dbt-public.interview_task.devices`

)

select * from base