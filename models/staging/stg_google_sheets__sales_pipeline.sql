{{ config(materialized='view') }}


with src as (
  select * from {{ source('google_sheets','sales_pipeline') }}
)

select
  cast(opportunity_id as varchar)          as opportunity_id,
  nullif(trim(sales_agent), '')            as sales_agent,
  nullif(trim(product), '')                as product,
  nullif(trim(account), '')                as account,
  upper(nullif(trim(deal_stage), ''))      as deal_stage,
  try_to_date(engage_date)                 as engage_date,
  try_to_date(close_date)                  as close_date,
  try_to_number(close_value, 18, 2)        as close_value_usd,
  -- carry through Airbyte’s change-detection column
  "_AIRBYTE_EXTRACTED_AT"                  as _AIRBYTE_EXTRACTED_AT
from src