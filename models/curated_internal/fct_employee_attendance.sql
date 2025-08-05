{{
config(
    unique_key="sk_fct_employee_attendance",
    tags=["SC_FCT"]
)
}}

with source as (
    select
        attendance_id,
        attendance_date,
        employee_id,
        department_id,
        shift_code,
        hours_worked,
        is_overtime_flag,
        absence_reason,
        supervisor_id,
        audit_log_time as source_audit_log_time
    from {{ source("hr_system","employee_attendance") }}

    {% if is_incremental() %}
    changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz) where
        metadata$action = 'INSERT'
        and source_audit_log_time > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
        and shift_code in ('DAY', 'NIGHT')
        qualify 1 row_number() over(
            partition by attendance_id order by "header_timestamp" desc
        )
    {% else %}
    where
        is_current_flag = true and shift_code in ('DAY', 'NIGHT')
    {% endif %}
),

final as (
    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.attendance_id'
                ])
            }}, '-1'
        ) as sk_fct_employee_attendance,
        src.attendance_id,
        src.attendance_date,
        src.employee_id,
        src.department_id,
        src.shift_code,
        src.hours_worked,
        src.is_overtime_flag,
        src.absence_reason,
        src.supervisor_id,
        current_timestamp() as current_audit_log_time,
        src.source_audit_log_time
    from source as src
)

select
    sk_fct_employee_attendance,
    attendance_id,
    attendance_date,
    employee_id,
    department_id,
    shift_code,
    hours_worked,
    is_overtime_flag,
    absence_reason,
    supervisor_id,
    current_audit_log_time,
    source_audit_log_time
from final
