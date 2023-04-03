{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_seccionurbano as (
    select *
    from {{ ref('stg_raw_seccionurbano') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionurbano.SECU_CCNCT']) }} as seccion_urbano_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionurbano.SETU_CCNCT']) }} as sector_urbano_key,
        SECU_CCNCT as seccion_urbano_codigo_comp,
        SECU_CCDGO as seccion_urbano_codigo,
        DATOS_ANM as seccion_urbano_anonimizado,
        area as seccion_urbano_area, 
        latitud as seccion_urbano_latitud, 
        longitud as seccion_urbano_longitud, 
        geometry as seccion_urbano_geometry, 
        version as seccion_urbano_version
    from stg_raw_seccionurbano
    order by SECU_CCNCT
)

select *
from transformed