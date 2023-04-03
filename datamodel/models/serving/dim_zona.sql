{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_zonaurbana as (
    select *
    from {{ ref('stg_raw_zonaurbana') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_zonaurbana.ZU_CDIVI']) }} as zona_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_zonaurbana.mpio_cdpmp']) }} as mcipio_key,
        ZU_CDIVI as zona_codigo_comp,
        ZU_CCDGO as zona_codigo,
        NOM_CPOB as zona_nombre,
        area as zona_area, 
        latitud as zona_latitud, 
        longitud as zona_longitud, 
        geometry as zona_geometry, 
        version as zona_version
    from stg_raw_zonaurbana
    order by ZU_CDIVI
)

select *
from transformed