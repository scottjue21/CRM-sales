{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = 'opportunity_id'
  )
}}

with latest as (
  -- Deduplicate to newest record per opportunity
  select *
  from (
    select
      s.*,
      row_number() over (
        partition by opportunity_id
        order by _AIRBYTE_EXTRACTED_AT desc nulls last
      ) as rn
    from {{ ref('stg_google_sheets__sales_pipeline') }} s
    where upper(deal_stage) = 'WON'
      and close_date is not null

      {% if is_incremental() %}
        and _AIRBYTE_EXTRACTED_AT >
            coalesce(
              (select max(last_extracted_at) from {{ this }}),
              to_timestamp_ntz('1900-01-01')
            )
      {% endif %}
  )
  where rn = 1
),

agg as (
  select
    opportunity_id,
    product,
    account,
    cast(close_date as date) as deal_date,
    close_value_usd,
    _AIRBYTE_EXTRACTED_AT as last_extracted_at
  from latest
)

select * from agg