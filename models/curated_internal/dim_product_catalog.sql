{{
config(
    unique_key="sk_dim_product_catalog",
    tags=["SC_DIM"]
)
}}

with source as (
    select
        product_source,
        company_code,
        product_name,
        product_class,
        product_type_id,
        is_discontinued_flag,
        product_id,
        audit_datetime as source_audit_datetime
    from {{ source("erp_system","product_catalog_hist") }}

    {% if is_incremental() %}
    changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz) where
        metadata$action = 'INSERT'
        and source_audit_datetime > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
        and product_source in ('P4')
        qualify 1 row_number() over(
            partition by product_source, company_code, product_name order by "header_timestamp" desc
        )
    {% else %}
    where
        is_current_flag = true and product_source in ('P4')
    {% endif %}
),

final as (
    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.product_source', 'src.company_code', 'src.product_name'
                ])
            }}, '-1'
        ) as sk_dim_product_catalog,
        src.product_name as product_catalog,
        src.is_discontinued_flag as discontinued_flag,
        src.product_source as source_system,
        src.company_code as company,
        src.product_type_id as type_id,
        current_timestamp() as current_audit_datetime,
        src.product_class as class,
        src.product_id as catalog_id,
        'PRODUCT_CATALOG_HIST' as stage_source_table,
        src.source_audit_datetime as source_audit_datetime
    from source as src
)

select
    sk_dim_product_catalog,
    discontinued_flag,
    source_system,
    company,
    product_catalog,
    class,
    type_id,
    current_audit_datetime,
    catalog_id,
    stage_source_table,
    source_audit_datetime
from final
