{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_sectorurbano as (
    select *
    from {{ ref('stg_raw_sectorurbano') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_sectorurbano.SETU_CCNCT']) }} as sector_urbano_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_sectorurbano.MPIO_CLASE']) }} as mcipio_clase_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_sectorurbano.ZU_CDIVI']) }} as zona_key,
        SETU_CCNCT as sector_urbano_codigo_comp,
        SETU_CCDGO as sector_urbano_codigo,
        area as sector_urbano_area, 
        latitud as sector_urbano_latitud, 
        longitud as sector_urbano_longitud, 
        geometry as sector_urbano_geometry, 
        version as sector_urbano_version
    from stg_raw_sectorurbano
    order by SETU_CCNCT
)

select *
from transformed