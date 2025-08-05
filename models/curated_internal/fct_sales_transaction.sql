{{
config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_fct_sales_transaction",
    tags=["SC_FCT"]
)
}}

with source as (
    select
        transaction_id,
        transaction_date,
        customer_id,
        product_id,
        sales_amount,
        quantity_sold,
        discount_amount,
        sales_channel,
        is_returned_flag,
        audit_timestamp as source_audit_timestamp
    from {{ source("sales_system","sales_transaction_hist") }}

    {% if is_incremental() %}
    changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz) where
        metadata$action = 'INSERT'
        and source_audit_timestamp > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
        and sales_channel in ('ONLINE', 'RETAIL')
        qualify 1 row_number() over(
            partition by transaction_id order by "header_timestamp" desc
        )
    {% else %}
    where
        is_current_flag = true and sales_channel in ('ONLINE', 'RETAIL')
    {% endif %}
),

final as (
    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.transaction_id'
                ])
            }}, '-1'
        ) as sk_fct_sales_transaction,
        src.transaction_id,
        src.transaction_date,
        src.customer_id,
        src.product_id,
        src.sales_amount,
        src.quantity_sold,
        src.discount_amount,
        src.sales_channel,
        src.is_returned_flag,
        current_timestamp() as current_audit_timestamp,
        src.source_audit_timestamp
    from source as src
)

select
    sk_fct_sales_transaction,
    transaction_id,
    transaction_date,
    customer_id,
    product_id,
    sales_amount,
    quantity_sold,
    discount_amount,
    sales_channel,
    is_returned_flag,
    current_audit_timestamp,
    source_audit_timestamp
from final
