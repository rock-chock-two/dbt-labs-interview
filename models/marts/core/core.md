{% docs dim_orders %}

This model joins information from multiple sources to provide order details report.

Each order can have multiple payments, including coupons.

Coupons are essentially discounts on total amount.

Definitions of `amount` columns:

- order_total_amount
This is total order's amount that is taken from `orders` table, regardless of if there were any successful payments.

- paid_amount
Base amount paid by user before taxes and shipping info. If a payment is not successful, then this amount has value 0.

- paid_tax_amount
Taxes paid via successful payment.

- paid_shipping_amount
Shipping costs paid via successful payment.

- discount_amount
Discount decreases the amount that user pays for the order. 

- paid_total_amount
User's real payments: paid_amount + paid_tax_amount + paid_shipping_amount

- paid_total_amount_plus_discount
Total amount paid per order plus discount amount for order with successful payments. 
If there were no successful payment and there is only discount amount, then this amount is 0 since coupon discount is not income.

{% enddocs %}
