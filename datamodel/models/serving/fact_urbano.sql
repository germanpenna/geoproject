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
        STCTNENCUE as encuestas_censo,
        STP3_1_SI as encuestas_terr_etnico,
        STP3_2_NO as encuestas_terr_no_etnico,
        STP3A_RI as encuestas_terr_etnico_resg_ind,
        STP3B_TCN as encuestas_terr_etnico_com_negra,
        STP4_1_SI as encuestas_area_protegida,
        STP4_2_NO as encuestas_area_no_protegida,
        STP9_1_USO as unidades_vivienda,
        STP9_2_USO as unidades_uso_mixto,
        STP9_3_USO as unidades_no_residencial,
        STP9_4_USO as unidades_especial_alojamiento,
        STP9_2_1_M as unidades_mixtas_industria,
        STP9_2_2_M as unidades_mixtas_comercio,
        STP9_2_3_M as unidades_mixtas_servicio,
        STP9_2_4_M as unidades_mixtas_agro,
        STP9_2_9_M as unidades_mixtas_sin_info,
        STP9_3_1_N as unidades_no_residencial_industria,
        STP9_3_2_N as unidades_no_residencial_comercio,
        STP9_3_3_N as unidades_no_residencial_servicio,
        STP9_3_4_N as unidades_no_residencial_agro,
        STP9_3_5_N as unidades_no_residencial_institucional,
        STP9_3_6_N as unidades_no_residencial_lote,
        STP9_3_7_N as unidades_no_residencial_parque,
        STP9_3_8_N as unidades_no_residencial_minas,
        STP9_3_9_N as unidades_no_residencial_proteccion,
        STP9_3_10 as unidades_no_residencial_construccion,
        STP9_3_99 as unidades_no_residencial_sin_info,
        STVIVIENDA as viviendas,
        STP14_1_TI as viviendas_casa,
        STP14_2_TI as viviendas_apto,
        STP14_3_TI as viviendas_cuarto,
        STP14_4_TI as viviendas_indigena,
        STP14_5_TI as viviendas_trad_etnica,
        STP14_6_TI as viviendas_otro,
        STP15_1_OC as viviendas_ocupadas_presente,
        STP15_2_OC as viviendas_ocupadas_ausente,
        STP15_3_OC as viviendas_temporal,
        STP15_4_OC as viviendas_desocupada,
        TSP16_HOG as hogares,
        STP19_EC_1 as viviendas_energia_electrica,
        STP19_ES_2 as viviendas_no_energia_electrica,
        STP19_EE_1 as viviendas_energia_estrato1,
        STP19_EE_2 as viviendas_energia_estrato2,
        STP19_EE_3 as viviendas_energia_estrato3,
        STP19_EE_4 as viviendas_energia_estrato4,
        STP19_EE_5 as viviendas_energia_estrato5,
        STP19_EE_6 as viviendas_energia_estrato6,
        STP19_EE_9 as viviendas_energia_no_info,
        STP19_ACU1 as viviendas_acueducto,
        STP19_ACU2 as viviendas_no_acueducto,
        STP19_ALC1 as viviendas_alcantarillado,
        STP19_ALC2 as viviendas_no_alcantarillado,
        STP19_GAS1 as viviendas_gas,
        STP19_GAS2 as viviendas_no_gas,
        STP19_GAS9 as viviendas_no_info_gas,
        STP19_REC1 as viviendas_rec_basuras,
        STP19_REC2 as viviendas_no_rec_basuras,
        STP19_INT1 as viviendas_internet,
        STP19_INT2 as viviendas_no_internet,
        STP19_INT9 as viviendas_no_info_internet,
        STP27_PERS as personas,
        STPERSON_L as personas_especial_alojamiento,
        STPERSON_S as personas_hogar_particular,
        STP32_1_SE as hombres,
        STP32_2_SE as mujeres,
        STP34_1_ED as personas_0_9_annos,
        STP34_2_ED as personas_10_19_annos,
        STP34_3_ED as personas_20_29_annos,
        STP34_4_ED as personas_30_39_annos,
        STP34_5_ED as personas_40_49_annos,
        STP34_6_ED as personas_50_59_annos,
        STP34_7_ED as personas_60_69_annos,
        STP34_8_ED as personas_70_79_annos,
        STP34_9_ED as personas_80_mas_annos,
        STP51_PRIM as personas_estudio_primaria,
        STP51_SECU as personas_estudio_secundaria,
        STP51_SUPE as personas_estudio_tecprof,
        STP51_POST as personas_estudio_post,
        STP51_13_E as personas_no_estudio,
        STP51_99_E as personas_no_info
    from stg_raw_seccionurbano
    order by SECU_CCNCT
)

select *
from transformed