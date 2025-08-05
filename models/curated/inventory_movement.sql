{{
config(
    tags=["SC_VW"]
)
}}

with fct_inventory as (
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
        current_audit_datetime as dp_timestamp,
        source_audit_datetime
    from {{ ref('fct_inventory_movement') }}
)

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
    dp_timestamp,
    source_audit_datetime
from fct_inventory
