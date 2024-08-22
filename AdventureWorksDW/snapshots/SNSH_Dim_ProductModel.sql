{% snapshot SNSH_Dim_ProductModel %}
    {{
        config(
            target_schema='snapshot'
            , strategy='check'
            , unique_key='ProductModelID'
            , check_cols='all'
        )
    }}

    select
        *
    from
        {{ ref('init_mssql__ProductModel')}}
        
{% endsnapshot%}