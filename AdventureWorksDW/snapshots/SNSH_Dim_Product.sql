{% snapshot SNSH_Dim_Product %}
    {{
        config(
            target_schema='snapshot'
            , strategy='check'
            , unique_key='ProductID'
            , check_cols='all'
        )
    }}

    select
        *
    from
        {{ ref('init_mssql__Product') }}
        
{% endsnapshot %}