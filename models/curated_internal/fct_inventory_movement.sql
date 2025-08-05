{{
config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key="sk_fct_inventory_movement",
    tags=["SC_FCT"]
)
}}

with source as (
    select
        movement_id,
        movement_date,
        warehouse_id,
        product_id,
        movement_type,
        quantity_moved,
        unit_cost,
        reference_doc,
        is_adjustment_flag,
        audit_datetime as source_audit_datetime
    from {{ source("inventory_system","inventory_movement_hist") }}

    {% if is_incremental() %}
    changes (information => append_only) at(timestamp => '{{ dbt_xom_package.get_incremental_ts(this) }}'::timestamp_tz) where
        metadata$action = 'INSERT'
        and source_audit_datetime > '{{ dbt_xom_package.get_incremental_ts(this)}}'::timestamp_tz
        and movement_type in ('IN', 'OUT')
        qualify 1 row_number() over(
            partition by movement_id order by "header_timestamp" desc
        )
    {% else %}
    where
        is_current_flag = true and movement_type in ('IN', 'OUT')
    {% endif %}
),

final as (
    select
        ifnull(
            {{
                dbt_utils.generate_surrogate_key([
                    'src.movement_id'
                ])
            }}, '-1'
        ) as sk_fct_inventory_movement,
        src.movement_id,
        src.movement_date,
        src.warehouse_id,
        src.product_id,
        src.movement_type,
        src.quantity_moved,
        src.unit_cost,
        src.reference_doc,
        src.is_adjustment_flag,
        current_timestamp() as current_audit_datetime,
        src.source_audit_datetime
    from source as src
)

select
    sk_fct_inventory_movement,
    movement_id,
    movement_date,
    warehouse_id,
    product_id,
    movement_type,
    quantity_moved,
    unit_cost,
    reference_doc,
    is_adjustment_flag,
    current_audit_datetime,
    source_audit_datetime
from final
