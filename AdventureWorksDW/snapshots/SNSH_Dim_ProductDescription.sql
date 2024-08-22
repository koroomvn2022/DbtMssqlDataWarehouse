{% snapshot SNSH_Dim_ProductDescription%}
    {{
        config(
            target_schema='snapshot'
            , strategy='check'
            , unique_key='ProductDescriptionID'
            , check_cols='all'
        )
    }}

    select 
        *
    from
        {{ ref('init_mssql__ProductDescription') }}
{% endsnapshot%}