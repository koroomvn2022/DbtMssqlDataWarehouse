{{
    config(
        materialized='incremental'
        , unique_key='SalesOrderDetailKey'
        , incremental_strategy='merge'
    )
}}

select
    *
from
    {{ ref('init_mssql__SalesOrderDetail') }}

{% if is_incremental() %}
where
    {{ dbt_utils.generate_surrogate_key(
        dbt_utils.get_filtered_columns_in_relation(
            from=ref('init_mssql__SalesOrderDetail')
        )
    )}} not in (
        select
            {{ dbt_utils.generate_surrogate_key(
                dbt_utils.get_filtered_columns_in_relation(
                    from=this
                )
            )}}
        from
            {{this}}
    )

{% endif %}

