{{
config(
    unique_key="sk_dim_vendor_master",
    tags=["SC_DIM"]
)
}}

with source as (
    select
        vendor_source,
        region_code,
        vendor_name,
        vendor_group,
        vendor_type_id,
        is_inactive_flag,
        vendor_master_id,
        audit_event_time as source_audit_event_time
    from {{ source("erp_system","vendor_master") }}

    {% if is_incremental() %}
    changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz) where
        metadata$action = 'INSERT'
        and source_audit_event_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
        and vendor_source in ('P4')
        qualify 1 row_number() over(
            partition by vendor_source, region_code, vendor_name order by "header_timestamp" desc
        )
    {% else %}
    where
        is_current_flag = true and vendor_source in ('P4')
    {% endif %}
),

final as (
    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.vendor_source', 'src.region_code', 'src.vendor_name'
                ])
            }}, '-1'
        ) as sk_dim_vendor_master,
        src.vendor_name as vendor_master,
        src.is_inactive_flag as inactive_flag,
        src.vendor_source as source_system,
        src.region_code as region,
        src.vendor_type_id as type_id,
        current_timestamp() as current_audit_event_time,
        src.vendor_group as group_name,
        src.vendor_master_id as master_id,
        'VENDOR_MASTER_HIST' as stage_source_table,
        src.source_audit_event_time as source_audit_event_time
    from source as src
)

select
    sk_dim_vendor_master,
    inactive_flag,
    source_system,
    region,
    vendor_master,
    group_name,
    type_id,
    current_audit_event_time,
    master_id,
    stage_source_table,
    source_audit_event_time
from final
