{% docs dim_orders %}

This model joins information from multiple sources to provide order details report.

Each order can have multiple payments, including coupons.

Coupons are essentially discounts on total amount.

There are multiple `amount` columns:

- order_total_amount_cents
This is total order's amount tha is taken from `orders` table
- paid_amount_cents
Base amount paid by user before taxes and shipping info. If payment is not successful, then this amount has value 0.

- paid_tax_amount_cents
Taxes paid for sucessful payment.

- paid_shipping_amount_cents
Shipping costs paid for sucessful payment.

- discount_amount_cents
Discount decreases the amount that user pays for the order.

- paid_total_amount_cents
User's real payments: paid_amount_cents + paid_tax_amount_cents + paid_shipping_amount_cents

- paid_total_amount_plus_discount_cents
Total amount paid per order plus discount amount for order with successful payments. 
If there were no successful payment and there is only discount amount, then this amount is 0.

{% enddocs %}