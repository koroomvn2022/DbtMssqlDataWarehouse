{{
    config(
        materialized='incremental'
        , unique_key='cast(CustomerID as varchar) + cast(AddressID as varchar)'
        , incremental_strategy='delete+insert'
    )
}}


select
    *
from
    {{ref('init_mssql__CustomerAddress')}}

{% if is_incremental() %}

    where
        {{dbt_utils.generate_surrogate_key(
            dbt_utils.get_filtered_columns_in_relation(
                from=ref('init_mssql__CustomerAddress')
            )
        )}} not in (
            select
                {{dbt_utils.generate_surrogate_key(
                    dbt_utils.get_filtered_columns_in_relation(
                        from=this
                    )
                )}} 
            from
                {{this}}
        )

{% endif %}