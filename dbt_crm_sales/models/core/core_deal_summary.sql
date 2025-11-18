{{ config(materialized='table') }}

select
  product,
  account,
  date_trunc('month', deal_date) as deal_month,
  count(distinct opportunity_id) as deals_won,
  sum(close_value_usd) as total_close_value_usd
from {{ ref('int_deals_won') }}
group by 1, 2, 3