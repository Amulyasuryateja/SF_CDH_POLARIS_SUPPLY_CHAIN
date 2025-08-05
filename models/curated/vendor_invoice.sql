{{
config(
    tags=["SC_VW","sit1"]
)
}}

with fct_invoice as (
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
        current_audit_event_time as fct_audit_event_time
    from {{ ref('fct_vendor_invoice') }}
),

dim_vendor as (
    select
        sk_dim_vendor_master,
        vendor_master,
        region,
        group_name,
        type_id
    from {{ ref('dim_vendor_master') }}
),

joined as (
    select
	greatest(
           coalesce(fct_invoice.current_audit_datetime, cast('1900-01-01' as datetime)),
           coalesce(dim_vendor.current_audit_datetime, cast('1900-01-01' as datetime))
) as dp_timestamp,
        fct_invoice.sk_fct_vendor_invoice,
        fct_invoice.invoice_id,
        fct_invoice.invoice_date,
        fct_invoice.vendor_id,
        fct_invoice.region_code,
        fct_invoice.invoice_amount,
        fct_invoice.tax_amount,
        fct_invoice.payment_status,
        fct_invoice.payment_date,
        fct_invoice.is_credit_note_flag,
        dim_vendor.sk_dim_vendor_master,
        dim_vendor.vendor_master,
        dim_vendor.region as vendor_region,
        dim_vendor.group_name as vendor_group,
        dim_vendor.type_id as vendor_type_id,
        fct_invoice.fct_audit_event_time
    from fct_invoice
    left join dim_vendor
        on fct_invoice.vendor_id = dim_vendor.sk_dim_vendor_master
)

select *
from joined
