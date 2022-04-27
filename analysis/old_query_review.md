# Query review


## Helpful resources
To get started with dbt, I'd recommend reading the following resources and try following the described best practices:
1. https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md
2. https://discourse.getdbt.com/t/how-we-structure-our-dbt-projects/355
3. https://docs.getdbt.com/docs/guides/best-practices


## Comments on query logic/calculations 

1. devices

1.1
There are no records that have `android-app` device, instead I found records with device = `android`.

Suggestion:
```
WHEN d.device IN (
  'ios-app',
  'android-app'
) THEN 'mobile-app'
```
to
```
when device in (
  'ios-app', 
  'android'
) then 'mobile-app'
```

1.2
Do we really need both 'unknown' and 'ERROR' variants?
To me they look redundant, and currently there are no records in this dataset that satisfies these cases.

Suggestion:
```
CASE
  WHEN d.device = 'web' THEN 'desktop'
  WHEN d.device IN (
    'ios-app',
    'android-app'
  ) THEN 'mobile-app'
  WHEN d.device IN (
    'mobile',
    'tablet'
  ) THEN 'mobile-web'
  WHEN NULLIF(
    d.device,
    ''
  ) IS NULL THEN 'unknown'
  ELSE 'ERROR'
END AS purchase_device_type
```
to
```
case
    when device = 'web' then 'desktop'
    when device in ('ios-app', 'android') then 'mobile-app'
    when device in ('mobile', 'tablet') then 'mobile-web'
    else 'unknown'
end as device_type
```

1.3
There are 2 records in raw "devices" table that have nulls for
created_at, updated_at and type_id.

I'd check if these records exist in app's production database, and then check with engineers
if this is expected state.

If it's not expected and the records don't exist in app's production database or will be removed soon, I'd filter out such records and set not null schema tests on some of these columns (created_at, updated_at).

I'd also document the expected scenarios in .yml file: "type_id is null for devices.type='cart'"


1.4
When testing casting a nullable value, I encountered a database error in accessing the resulting value.

I prefer using safe_cast() to cast() to ensure that there are no
unexpected errors.
Also, I'd add not_null schema tests for all such casted columns

Suggestion:
```
CAST(d.type_id AS int64) AS order_id,
```
to
```
safe_cast(type_id as int64) as order_id
```

1.5
I'd provide documentation about type_id column in .yml file.
"type_id is a foreign key of specified type. E.g. if type = 'order' and type_id ='' "


2. Addresses

2.1 country_type
Use else to catch not obvious answers.
Also, there is no need to transform NULL to 'Null country', just leave the null

Suggestion:
```
CASE
  WHEN oa.country_code IS NULL THEN 'Null country'
  WHEN oa.country_code = 'US' THEN 'US'
  WHEN oa.country_code != 'US' THEN 'International'
END AS country_type,
```
to
```
case
    when country_code = 'US' then 'US'
    when country_code != 'US' then 'International'
    else null
end as country_type
```


2. Payments

2.1 coupons
Some orders were partially paid by coupons (payment_type = 'coupon').
I'd check with finance team / with engineers on how to properly use coupon to
calculate amounts.

E.g.
I'd expect coupon to lower total gross payment amount before taxes.
In this case I'd create a separate column 'coupon_discount_amount' and adjust
calculations of amounts for aggregated order.

2.2 order_status_category
Since we're interested in payment amounts, I'd classify successful category as 'paid' rather than 'completed'


3. Order amounts

Not clear and not correct logic for amounts.

I'd recommend documenting the logic and thinking about more self-descriptive column names.

I don't fully understand the initial logic of the `total_amount_cents`
without documentation and I don't think it's correct.

I also don't think that `pa.gross_total_amount_cents` is properly calculated as it doesn't consider coupon discount.

Suggestion:
```
o.amount_total_cents,
pa.gross_total_amount_cents,
CASE
    WHEN o.currency = 'USD' THEN o.amount_total_cents
    ELSE pa.gross_total_amount_cents
END AS total_amount_cents,
pa.gross_tax_amount_cents,
pa.gross_amount_cents,
pa.gross_shipping_amount_cents
```
to
```
stg_interview__orders.order_total_amount_cents,
payments__grouped.paid_amount_cents,
payments__grouped.paid_tax_amount_cents,
payments__grouped.paid_shipping_amount_cents,
payments__grouped.discount_amount_cents,
payments__grouped.paid_total_amount_cents, 
payments__grouped.paid_total_amount_plus_discount_cents
```



## SQL Style
1. 4 spaces
Per dbt Labs style guide (https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md), the indentation is 4 spaces.

2. Lowercase
dbt Labs style guide (https://github.com/dbt-labs/corp/blob/main/dbt_style_guide.md) mentions that using lowercase for sql queries makes it easier to read and type code.

3. Table aliases
When using aliases, don't omit as (see dbt Labs)
Don't use table aliases that are not self-explanatory as it's hard to read.
Use lengthy self-descriptive names.

e.g.
```
  select
    a.id,
    o.id
  from
    addresses a
  left join
    orders o
    on a.customer_id = o.customer_id
```
should become
```
select
    addresses.id,
    orders.id

from addresses
left join orders
    on addresses.customer_id = orders.customer_id
```

No need to use table aliases before column names when there is no join.

e.g.
``` select devices.id from devices
```
should become
``` select id from devices
```

4. case when

If there are only 2 options in conditional statement, the less verbose conditional
is BigQuery's if() function.

See BigQuery documentation:
https://cloud.google.com/bigquery/docs/reference/standard-sql/conditional_expressions#if

e.g.
```
  case
    when status = 'completed' then amount_cents
    else 0
  end
```
becomes
```
  if(status = 'completed', amount_cents, 0)
```

5. join

Use consistent order when joining tables together. Always join new table
to the same left table.

Suggestion:
```
from orders
left join filtered_orders
    on orders.user_id = filtered_orders.user_id
left join addresses
    on addresses.order_id = orders.order_id
```
to
```
from orders
left join filtered_orders
    on orders.user_id = filtered_orders.user_id
left join addresses
    on orders.order_id = addresses.order_id
```

6. Column order

Try to order column names so that they are visually added to the same semantic group

e.g.
```
select
    order_id,
    user_id,
    currency,
    amount_total_cents,
    user_type,
    amount_cents
```
to
```
select
    order_id,

    user_id,
    user_type,

    currency,
    amount_total_cents,
    amount_cents
```


## dbt project best practices

1. Staging models

Add source and staging models to have ability to change paths only once for all sources.
Easily reuse these models for dependent models for any reports.

Move basic transformation (casting, defining type based on device, status, etc.) that can be reused later to staging model.

e.g.
```
distinct cast(
  devices.type_id as int64
) as order_id,
```

Staging models should select from a single raw source.

All the other models should not select from raw data and instead should refer to
staging models via {{ ref('') }} macro.
This approach allows to see a clear DAGs that shows where is the data coming from.


2. CTEs, not subqueries

CTEs allow to easily check results of each logical unit and make the code easier
to read.

CTEs help the code to be like lego, it's much easier to combine independent
parts of the huge query and test them, and yo don't have to memorize all the details
once you've checked that the data is valid.

Read more about CTEs:
https://discourse.getdbt.com/t/why-the-fishtown-sql-style-guide-uses-so-many-ctes/1091

3. Documents and tests

Test models to prevent unexpected database errors and not consistent results.

Document complex logic in models to help others understand the model and to enhance data self-service within the company.

