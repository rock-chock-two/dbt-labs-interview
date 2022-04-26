SELECT
  *,
  amount_total_cents / 100 AS amount_total,
  gross_total_amount_cents / 100 AS gross_total_amount,
  total_amount_cents / 100 AS total_amount,
  gross_tax_amount_cents / 100 AS gross_tax_amount,
  gross_amount_cents / 100 AS gross_amount,
  gross_shipping_amount_cents / 100 AS gross_shipping_amount
FROM
  (
    SELECT
      o.order_id,
      o.user_id,
      o.created_at,
      o.updated_at,
      o.shipped_at,
      o.currency,
      o.status AS order_status,
      CASE
        WHEN o.status IN (
          'paid',
          'completed',
          'shipped'
        ) THEN 'completed'
        ELSE o.status
      END AS order_status_category,
      CASE
        WHEN oa.country_code IS NULL THEN 'Null country'
        WHEN oa.country_code = 'US' THEN 'US'
        WHEN oa.country_code != 'US' THEN 'International'
      END AS country_type,
      o.shipping_method,
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
      END AS purchase_device_type,
      d.device AS purchase_device,
      CASE
        WHEN fo.first_order_id = o.order_id THEN 'new'
        ELSE 'repeat'
      END AS user_type,
      o.amount_total_cents,
      pa.gross_total_amount_cents,
      CASE
        WHEN o.currency = 'USD' THEN o.amount_total_cents
        ELSE pa.gross_total_amount_cents
      END AS total_amount_cents,
      pa.gross_tax_amount_cents,
      pa.gross_amount_cents,
      pa.gross_shipping_amount_cents
    FROM
      `dbt-public.interview_task.orders` o
      LEFT JOIN (
        SELECT
          DISTINCT CAST(
            d.type_id AS int64
          ) AS order_id,
          FIRST_VALUE(
            d.device
          ) OVER (
            PARTITION BY d.type_id
            ORDER BY
              d.created_at ROWS BETWEEN UNBOUNDED PRECEDING
              AND UNBOUNDED FOLLOWING
          ) AS device
        FROM
          `dbt-public.interview_task.devices` d
        WHERE
          d.type = 'order'
      ) d
      ON d.order_id = o.order_id
      LEFT JOIN (
        SELECT
          fo.user_id,
          MIN(
            fo.order_id
          ) AS first_order_id
        FROM
          `dbt-public.interview_task.orders` AS fo
        WHERE
          fo.status != 'cancelled'
        GROUP BY
          fo.user_id
      ) fo
      ON o.user_id = fo.user_id
      LEFT JOIN `dbt-public.interview_task.addresses` oa
      ON oa.order_id = o.order_id
      LEFT JOIN (
        SELECT
          order_id,
          SUM(
            CASE
              WHEN status = 'completed' THEN tax_amount_cents
              ELSE 0
            END
          ) AS gross_tax_amount_cents,
          SUM(
            CASE
              WHEN status = 'completed' THEN amount_cents
              ELSE 0
            END
          ) AS gross_amount_cents,
          SUM(
            CASE
              WHEN status = 'completed' THEN amount_shipping_cents
              ELSE 0
            END
          ) AS gross_shipping_amount_cents,
          SUM(
            CASE
              WHEN status = 'completed' THEN tax_amount_cents + amount_cents + amount_shipping_cents
              ELSE 0
            END
          ) AS gross_total_amount_cents
        FROM
          `dbt-public.interview_task.payments`
        GROUP BY
          order_id
      ) pa
      ON pa.order_id = o.order_id
  )
