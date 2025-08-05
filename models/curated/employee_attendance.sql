{{
config(
    tags=["SC_VW","sit1"]
)
}}

with fct_attendance as (
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
        current_audit_log_time as fct_audit_log_time
    from {{ ref('fct_employee_attendance') }}
),

dim_employee as (
    select
        sk_dim_employee_record,
        employee_record,
        department,
        role,
        role_type
    from {{ ref('dim_employee_record') }}
),

joined as (
    select
        fct_attendance.sk_fct_employee_attendance,
        fct_attendance.attendance_id,
        fct_attendance.attendance_date,
        fct_attendance.employee_id,
        fct_attendance.department_id,
        fct_attendance.shift_code,
        fct_attendance.hours_worked,
        fct_attendance.is_overtime_flag,
        fct_attendance.absence_reason,
        fct_attendance.supervisor_id,
        dim_employee.sk_dim_employee_record,
        dim_employee.employee_record,
        dim_employee.department as emp_department,
        dim_employee.role as emp_role,
        dim_employee.role_type as emp_role_type,
        fct_attendance.fct_audit_log_time
    from fct_attendance
    left join dim_employee
        on fct_attendance.employee_id = dim_employee.sk_dim_employee_record
)

select *
from joined
