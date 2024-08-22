{{
    config(
        materialized = 'incremental'
        , unique_key = ['DateKey']
        , incremental_strategy = 'delete+insert'
    )
}}

select
    *
from
    {{ ref('init_mssql__Date') }}