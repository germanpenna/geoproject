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
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionrural.DPTO_CCDGO']) }} as depto_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionrural.MPIO_CDPMP']) }} as mcipio_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionrural.mpio_clase']) }} as municipio_clase_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionrural.ZU_CDIVI']) }} as zona_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionrural.SETR_CCNCT']) }} as sector_rural_key,
        {{ dbt_utils.generate_surrogate_key(['stg_raw_seccionrural.SECR_CCNCT']) }} as seccion_rural_key,
        stg_raw_depto.dpto_cnmbr as depto_nombre,
        stg_raw_mcipio.mpio_cnmbr as mcipio_nombre,
        stg_raw_zonaurbana.NOM_CPOB as zona_nombre,
        stg_raw_seccionrural.CLAS_CCDGO as municipio_clase_codigo,
        stg_raw_seccionrural.LATITUD as latitud,
        stg_raw_seccionrural.LONGITUD as longitud,
        stg_raw_seccionrural.STCTNENCUE as encuestas_censo,
        stg_raw_seccionrural.STP3_1_SI as encuestas_terr_etnico,
        stg_raw_seccionrural.STP3_2_NO as encuestas_terr_no_etnico,
        stg_raw_seccionrural.STP3A_RI as encuestas_terr_etnico_resg_ind,
        stg_raw_seccionrural.STP3B_TCN as encuestas_terr_etnico_com_negra,
        stg_raw_seccionrural.STP4_1_SI as encuestas_area_protegida,
        stg_raw_seccionrural.STP4_2_NO as encuestas_area_no_protegida,
        stg_raw_seccionrural.STP9_1_USO as unidades_vivienda,
        stg_raw_seccionrural.STP9_2_USO as unidades_uso_mixto,
        stg_raw_seccionrural.STP9_3_USO as unidades_no_residencial,
        stg_raw_seccionrural.STP9_4_USO as unidades_especial_alojamiento,
        stg_raw_seccionrural.STP9_2_1_M as unidades_mixtas_industria,
        stg_raw_seccionrural.STP9_2_2_M as unidades_mixtas_comercio,
        stg_raw_seccionrural.STP9_2_3_M as unidades_mixtas_servicio,
        stg_raw_seccionrural.STP9_2_4_M as unidades_mixtas_agro,
        stg_raw_seccionrural.STP9_2_9_M as unidades_mixtas_sin_info,
        stg_raw_seccionrural.STP9_3_1_N as unidades_no_residencial_industria,
        stg_raw_seccionrural.STP9_3_2_N as unidades_no_residencial_comercio,
        stg_raw_seccionrural.STP9_3_3_N as unidades_no_residencial_servicio,
        stg_raw_seccionrural.STP9_3_4_N as unidades_no_residencial_agro,
        stg_raw_seccionrural.STP9_3_5_N as unidades_no_residencial_institucional,
        stg_raw_seccionrural.STP9_3_6_N as unidades_no_residencial_lote,
        stg_raw_seccionrural.STP9_3_7_N as unidades_no_residencial_parque,
        stg_raw_seccionrural.STP9_3_8_N as unidades_no_residencial_minas,
        stg_raw_seccionrural.STP9_3_9_N as unidades_no_residencial_proteccion,
        stg_raw_seccionrural.STP9_3_10 as unidades_no_residencial_construccion,
        stg_raw_seccionrural.STP9_3_99 as unidades_no_residencial_sin_info,
        stg_raw_seccionrural.STVIVIENDA as viviendas,
        stg_raw_seccionrural.STP14_1_TI as viviendas_casa,
        stg_raw_seccionrural.STP14_2_TI as viviendas_apto,
        stg_raw_seccionrural.STP14_3_TI as viviendas_cuarto,
        stg_raw_seccionrural.STP14_4_TI as viviendas_indigena,
        stg_raw_seccionrural.STP14_5_TI as viviendas_trad_etnica,
        stg_raw_seccionrural.STP14_6_TI as viviendas_otro,
        stg_raw_seccionrural.STP15_1_OC as viviendas_ocupadas_presente,
        stg_raw_seccionrural.STP15_2_OC as viviendas_ocupadas_ausente,
        stg_raw_seccionrural.STP15_3_OC as viviendas_temporal,
        stg_raw_seccionrural.STP15_4_OC as viviendas_desocupada,
        stg_raw_seccionrural.TSP16_HOG as hogares,
        stg_raw_seccionrural.STP19_EC_1 as viviendas_energia_electrica,
        stg_raw_seccionrural.STP19_ES_2 as viviendas_no_energia_electrica,
        stg_raw_seccionrural.STP19_EE_1 as viviendas_energia_estrato1,
        stg_raw_seccionrural.STP19_EE_2 as viviendas_energia_estrato2,
        stg_raw_seccionrural.STP19_EE_3 as viviendas_energia_estrato3,
        stg_raw_seccionrural.STP19_EE_4 as viviendas_energia_estrato4,
        stg_raw_seccionrural.STP19_EE_5 as viviendas_energia_estrato5,
        stg_raw_seccionrural.STP19_EE_6 as viviendas_energia_estrato6,
        stg_raw_seccionrural.STP19_EE_9 as viviendas_energia_no_info,
        stg_raw_seccionrural.STP19_ACU1 as viviendas_acueducto,
        stg_raw_seccionrural.STP19_ACU2 as viviendas_no_acueducto,
        stg_raw_seccionrural.STP19_ALC1 as viviendas_alcantarillado,
        stg_raw_seccionrural.STP19_ALC2 as viviendas_no_alcantarillado,
        stg_raw_seccionrural.STP19_GAS1 as viviendas_gas,
        stg_raw_seccionrural.STP19_GAS2 as viviendas_no_gas,
        stg_raw_seccionrural.STP19_GAS9 as viviendas_no_info_gas,
        stg_raw_seccionrural.STP19_REC1 as viviendas_rec_basuras,
        stg_raw_seccionrural.STP19_REC2 as viviendas_no_rec_basuras,
        stg_raw_seccionrural.STP19_INT1 as viviendas_internet,
        stg_raw_seccionrural.STP19_INT2 as viviendas_no_internet,
        stg_raw_seccionrural.STP19_INT9 as viviendas_no_info_internet,
        stg_raw_seccionrural.STP27_PERS as personas,
        stg_raw_seccionrural.STPERSON_L as personas_especial_alojamiento,
        stg_raw_seccionrural.STPERSON_S as personas_hogar_particular,
        stg_raw_seccionrural.STP32_1_SE as hombres,
        stg_raw_seccionrural.STP32_2_SE as mujeres,
        stg_raw_seccionrural.STP34_1_ED as personas_0_9_annos,
        stg_raw_seccionrural.STP34_2_ED as personas_10_19_annos,
        stg_raw_seccionrural.STP34_3_ED as personas_20_29_annos,
        stg_raw_seccionrural.STP34_4_ED as personas_30_39_annos,
        stg_raw_seccionrural.STP34_5_ED as personas_40_49_annos,
        stg_raw_seccionrural.STP34_6_ED as personas_50_59_annos,
        stg_raw_seccionrural.STP34_7_ED as personas_60_69_annos,
        stg_raw_seccionrural.STP34_8_ED as personas_70_79_annos,
        stg_raw_seccionrural.STP34_9_ED as personas_80_mas_annos,
        stg_raw_seccionrural.STP51_PRIM as personas_estudio_primaria,
        stg_raw_seccionrural.STP51_SECU as personas_estudio_secundaria,
        stg_raw_seccionrural.STP51_SUPE as personas_estudio_tecprof,
        stg_raw_seccionrural.STP51_POST as personas_estudio_post,
        stg_raw_seccionrural.STP51_13_E as personas_no_estudio,
        stg_raw_seccionrural.STP51_99_E as personas_no_info
    from stg_raw_seccionrural
    left join stg_raw_depto
        on stg_raw_seccionrural.DPTO_CCDGO = stg_raw_depto.DPTO_CCDGO
    left join stg_raw_mcipio
        on stg_raw_seccionrural.MPIO_CDPMP = stg_raw_mcipio.MPIO_CDPMP
    left join stg_raw_zonaurbana
        on stg_raw_seccionrural.ZU_CDIVI = stg_raw_zonaurbana.ZU_CDIVI
    order by stg_raw_seccionrural.SECR_CCNCT
)

select *
from transformed