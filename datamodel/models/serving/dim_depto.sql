{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_depto as (
    select *
    from {{ ref('stg_raw_depto') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_depto.DPTO_CCDGO']) }} as depto_key, -- auto-incremental surrogate key
        dpto_ccdgo as depto_codigo,
        dpto_cnmbr as depto_nombre, 
        area as depto_area, 
        latitud as depto_latitud, 
        longitud as depto_longitud, 
        geometry as depto_geometry, 
        version as depto_version
    from stg_raw_depto
    order by dpto_ccdgo
)

select *
from transformed