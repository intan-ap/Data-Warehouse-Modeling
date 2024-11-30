-- fact_salesorder
{{
  config(
    materialized='table'
  )
}}

SELECT 
  `Order ID` AS order_id, 
  Date AS date,
  Status AS status,
  ASIN as asin,
  B2B as b2b,
  currency,
  {{ dbt_utils.generate_surrogate_key([
				'SKU'
			]) }} as product_id,
  {{ dbt_utils.generate_surrogate_key([
				'Fulfilment', 
				'`fulfilled-by`'
			])}} AS fulfillment_id,
  {{ dbt_utils.generate_surrogate_key([
				'`promotion-ids`'
			]) }} as promotion_id,
  {{ dbt_utils.generate_surrogate_key([
			    '`Sales Channel `'
			])}} AS channel_id,
  {{ dbt_utils.generate_surrogate_key([
                '`Courier Status`',
                '`ship-service-level`',
                '`ship-city`',
                '`ship-state`',
                '`ship-postal-code`',
                '`ship-country`',
			]) }} as shipment_id,
  SUM(qty) AS qty,
  COALESCE(SUM(amount),0) AS amount,
FROM
    {{ source('bronze', 'amazon_sale_report') }}
GROUP BY
  `Order ID`,
  Date,
  Status,
  ASIN,
  B2B,
  currency,
  product_id,
  fulfillment_id,
  promotion_id,
  channel_id,
  shipment_id
  