{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_clase as (
    select *
    from {{ ref('raw_clase') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_clase.CLAS_CCDGO']) }} as clase_key,
        CLAS_CCDGO as clase_codigo,
        CLAS_NOMBRE as clase_nombre
    from stg_raw_clase
    order by CLAS_CCDGO
)

select *
from transformed