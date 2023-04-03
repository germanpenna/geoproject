{{
    config(
        materialized='table',
        database='GEODATA',
        schema='MODEL'
    )
}}

with obt_rural_mod as (
    select sector_rural_key as sector_key, seccion_rural_key as seccion_key,
        {{ dbt_utils.star(from=ref('obt_rural'), relation_alias='obt_rural', except=["sector_rural_key", "seccion_rural_key"]) }},
         'RURAL' as source
    from model.obt_rural
),
obt_urbano_mod as (
    select sector_urbano_key as sector_key, seccion_urbano_key as seccion_key,
        {{ dbt_utils.star(from=ref('obt_urbano'), relation_alias='obt_urbano', except=["sector_urbano_key", "seccion_urbano_key"]) }},
         'URBANO' as source
    from model.obt_urbano
),
obt as (
    select * from obt_rural_mod
    union all
    select * from obt_urbano_mod
)

select * 
from obt
order by depto_nombre, mcipio_nombre, municipio_clase_codigo, zona_nombre