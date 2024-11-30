-- dim_promotion
{{
  config(
    materialized='table'
  )
}}

With t_data AS (
SELECT DISTINCT 
  `promotion-ids` as promotion_ids
FROM
    {{ source('bronze', 'amazon_sale_report') }}
)

SELECT 
{{ dbt_utils.generate_surrogate_key([
				'promotion_ids'
			])}} AS promotion_id, *
FROM t_data
