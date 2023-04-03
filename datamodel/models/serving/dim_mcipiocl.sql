{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with stg_raw_mcipiocl as (
    select *
    from {{ ref('stg_raw_mcipiocl') }}
),

transformed as (
    select 
        {{ dbt_utils.generate_surrogate_key(['stg_raw_mcipiocl.mpio_clase']) }} as municipio_clase_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_mcipiocl.mpio_cdpmp']) }} as mcipio_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_mcipiocl.CLAS_CCDGO']) }} as clase_key,
        mpio_clase as municipio_clase_codigo_comp,
        CLAS_CCDGO as municipio_clase_codigo,
        area as municipio_clase_area, 
        latitud as municipio_clase_latitud, 
        longitud as municipio_clase_longitud, 
        geometry as municipio_clase_geometry, 
        version as municipio_clase_version
    from stg_raw_mcipiocl
    order by mpio_clase
)

select *
from transformed