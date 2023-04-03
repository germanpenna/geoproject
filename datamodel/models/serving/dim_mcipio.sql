{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_mcipio as (
    select *
    from {{ ref('stg_raw_mcipio') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_mcipio.mpio_cdpmp']) }} as mcipio_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_mcipio.DPTO_CCDGO']) }} as depto_key,
        mpio_cdpmp as mcipio_codigo_comp,
        mpio_ccdgo as mcipio_codigo,
        mpio_cnmbr as mcipio_nombre,
        area as mcipio_area, 
        latitud as mcipio_latitud, 
        longitud as mcipio_longitud, 
        geometry as mcipio_geometry, 
        version as mcipio_version
    from stg_raw_mcipio
    order by mpio_cdpmp
)

select *
from transformed