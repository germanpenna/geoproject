{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_sectorrural as (
    select *
    from {{ ref('stg_raw_sectorrural') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_sectorrural.SETR_CCNCT']) }} as sector_rural_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_sectorrural.MPIO_CLASE']) }} as mcipio_clase_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_sectorrural.ZU_CDIVI']) }} as zona_key,
        SETR_CCNCT as sector_rural_codigo_comp,
        SETR_CCDGO as sector_rural_codigo,
        area as sector_rural_area, 
        latitud as sector_rural_latitud, 
        longitud as sector_rural_longitud, 
        geometry as sector_rural_geometry, 
        version as sector_rural_version
    from stg_raw_sectorrural
    order by SETR_CCNCT
)

select *
from transformed