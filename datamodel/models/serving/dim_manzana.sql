{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_manzana as (
    select *
    from {{ ref('stg_raw_manzana') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_manzana.COD_DANE_A']) }} as manzana_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_manzana.SECU_CCNCT']) }} as seccion_urbano_key,
        COD_DANE_A as manzana_codigo_comp,
        MANZ_CCDGO as manzana_codigo,
        cd_lc_cm as manzana_codigo_localidad,
        tp_lc_cm as manzana_nombre_localidad,
        nmb_lc_cm as manzana_tipo_localidad,
        densidad as manzana_densidad,
        area as manzana_area, 
        latitud as manzana_latitud, 
        longitud as manzana_longitud, 
        geometry as manzana_geometry, 
        version as manzana_version
    from stg_raw_manzana
    order by COD_DANE_A
)

select *
from transformed