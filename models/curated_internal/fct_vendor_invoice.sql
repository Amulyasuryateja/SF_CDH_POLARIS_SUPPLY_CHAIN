{{
config(
    unique_key="sk_fct_vendor_invoice",
    tags=["SC_FCT"]
)
}}

with source as (
    select
        invoice_id,
        invoice_date,
        vendor_id,
        region_code,
        invoice_amount,
        tax_amount,
        payment_status,
        payment_date,
        is_credit_note_flag,
        audit_event_time as source_audit_event_time
    from {{ source("finance_system","vendor_invoice") }}

    {% if is_incremental() %}
    changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz) where
        metadata$action = 'INSERT'
        and source_audit_event_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
        and payment_status in ('PAID', 'UNPAID')
        qualify 1 row_number() over(
            partition by invoice_id order by "header_timestamp" desc
        )
    {% else %}
    where
        is_current_flag = true and payment_status in ('PAID', 'UNPAID')
    {% endif %}
),

final as (
    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.invoice_id'
                ])
            }}, '-1'
        ) as sk_fct_vendor_invoice,
        src.invoice_id,
        src.invoice_date,
        src.vendor_id,
        src.region_code,
        src.invoice_amount,
        src.tax_amount,
        src.payment_status,
        src.payment_date,
        src.is_credit_note_flag,
        current_timestamp() as current_audit_event_time,
        src.source_audit_event_time
    from source as src
)

select
    sk_fct_vendor_invoice,
    invoice_id,
    invoice_date,
    vendor_id,
    region_code,
    invoice_amount,
    tax_amount,
    payment_status,
    payment_date,
    is_credit_note_flag,
    current_audit_event_time,
    source_audit_event_time
from final
