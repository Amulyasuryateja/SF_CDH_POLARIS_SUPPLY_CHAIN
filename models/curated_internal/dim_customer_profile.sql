{{
config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_dim_customer_profile",
    tags=["SC_DIM"]
)
}}

with source as (
    select
        customer_source,
        client_id,
        customer_name,
        customer_type,
        customer_category,
        is_deleted_flag,
        customer_profile_id,
        audit_timestamp as source_audit_timestamp
    from {{ source("erp_system","customer_profile_hist") }}

    {% if is_incremental() %}
    changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz) where
        metadata$action = 'INSERT'
        and source_audit_timestamp > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
        and customer_source in ('P4')
        qualify 1 row_number() over(
            partition by customer_source, client_id, customer_name order by "header_timestamp" desc
        )
    {% else %}
    where
        is_current_flag = true and customer_source in ('P4')
    {% endif %}
),

final as (
    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.customer_source', 'src.client_id', 'src.customer_name'
                ])
            }}, '-1'
        ) as sk_dim_customer_profile,
        src.customer_name as customer_profile,
        src.is_deleted_flag as delete_flag,
        src.customer_source as source_system,
        src.client_id as client,
        src.customer_category as category,
        current_timestamp() as current_audit_timestamp,
        src.customer_type as type,
        src.customer_profile_id as profile_id,
        'CUSTOMER_PROFILE_HIST' as stage_source_table,
        src.source_audit_timestamp as source_audit_timestamp
    from source as src
)

select
    sk_dim_customer_profile,
    delete_flag,
    source_system,
    client,
    customer_profile,
    type,
    category,
    current_audit_timestamp,
    profile_id,
    stage_source_table,
    source_audit_timestamp
from final
