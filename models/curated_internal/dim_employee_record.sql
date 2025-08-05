{{
config(
    unique_key="sk_dim_employee_record",
    tags=["SC_DIM"]
)
}}

with source as (
    select
        employee_source,
        department_id,
        employee_full_name,
        employee_role,
        role_type_id,
        is_terminated_flag,
        employee_id,
        audit_log_time as source_audit_log_time
    from {{ source("hr_system","employee_record") }}

    {% if is_incremental() %}
    changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz) where
        metadata$action = 'INSERT'
        and source_audit_log_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
        and employee_source in ('P4')
        qualify 1 row_number() over(
            partition by employee_source, department_id, employee_full_name order by "header_timestamp" desc
        )
    {% else %}
    where
        is_current_flag = true and employee_source in ('P4')
    {% endif %}
),

final as (
    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.employee_source', 'src.department_id', 'src.employee_full_name'
                ])
            }}, '-1'
        ) as sk_dim_employee_record,
        src.employee_full_name as employee_record,
        src.is_terminated_flag as terminated_flag,
        src.employee_source as source_system,
        src.department_id as department,
        src.role_type_id as role_type,
        current_timestamp() as current_audit_log_time,
        src.employee_role as role,
        src.employee_id as record_id,
        'EMPLOYEE_RECORD_HIST' as stage_source_table,
        src.source_audit_log_time as source_audit_log_time
    from source as src
)

select
    sk_dim_employee_record,
    terminated_flag,
    source_system,
    department,
    employee_record,
    role,
    role_type,
    current_audit_log_time,
    record_id,
    stage_source_table,
    source_audit_log_time
from final
