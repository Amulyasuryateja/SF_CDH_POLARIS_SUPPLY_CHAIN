{{
config(
    tags=["SC_VW"]
)
}}

with fct_sales as (
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
        current_audit_timestamp as fct_audit_timestamp
    from {{ ref('fct_sales_transaction') }}
),

dim_customer as (
    select
        sk_dim_customer_profile,
        customer_profile,
        client,
        category,
        type
    from {{ ref('dim_customer_profile') }}
),

dim_product as (
    select
        sk_dim_product_catalog,
        product_catalog,
        company,
        type_id,
        class
    from {{ ref('dim_product_catalog') }}
),

joined as (
    select
	greatest(
            coalesce(fct_sales.current_audit_datetime, cast('1900-01-01' as datetime)),
            coalesce(dim_customer.current_audit_datetime, cast('1900-01-01' as datetime)),
            coalesce(dim_product.current_audit_datetime, cast('1900-01-01' as datetime))
) as dp_timestamp,
        fct_sales.sk_fct_sales_transaction,
        fct_sales.transaction_id,
        fct_sales.transaction_date,
        fct_sales.sales_amount,
        fct_sales.quantity_sold,
        fct_sales.discount_amount,
        fct_sales.sales_channel,
        fct_sales.is_returned_flag,
        dim_customer.sk_dim_customer_profile,
        dim_customer.customer_profile,
        dim_customer.client as customer_client,
        dim_customer.category as customer_category,
        dim_customer.type as customer_type,
        dim_product.sk_dim_product_catalog,
        dim_product.product_catalog,
        dim_product.company as product_company,
        dim_product.type_id as product_type_id,
        dim_product.class as product_class,
        fct_sales.fct_audit_timestamp
    from fct_sales
    left join dim_customer
        on fct_sales.customer_id = dim_customer.sk_dim_customer_profile
    left join dim_product
        on fct_sales.product_id = dim_product.sk_dim_product_catalog
)

select *
from joined
