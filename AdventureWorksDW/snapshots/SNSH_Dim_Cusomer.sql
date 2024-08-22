{% snapshot SNSH_Dim_Customer%}
    {{
        config(
            target_schema='snapshot'
            , strategy='check'
            , unique_key='CustomerID'
            , check_cols='all'
        )
    }}

    select 
        * 
    from 
        {{ ref('init_mssql__Customer')}}
    
{% endsnapshot %%}