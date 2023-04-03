{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_seccionrural as (
    select *
    from {{ ref('stg_raw_seccionrural') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionrural.SECR_CCNCT']) }} as seccion_rural_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionrural.SETR_CCNCT']) }} as sector_rural_key,
        SECR_CCNCT as seccion_rural_codigo_comp,
        SECR_CCDGO as seccion_rural_codigo,
        AG_CCDGO as seccion_rural_area_geo,
        DATOS_ANM as seccion_rural_anonimizado,
        area as seccion_rural_area, 
        latitud as seccion_rural_latitud, 
        longitud as seccion_rural_longitud, 
        geometry as seccion_rural_geometry, 
        version as seccion_rural_version
    from stg_raw_seccionrural
    order by SECR_CCNCT
)

select *
from transformed