# Pacotes
library(here)
library(fs)
library(beepr)
library(sf)
library(tidyverse)
library(tidylog)
library(skimr)
library(sfarrow)

# Funções
source(here("scripts", "functions.R"))

# Opções
options(scipen = 99999,
        error = beep)

# 1. Importa -------------------------------------------------------------------

#
get_shapefiles_names()

#
get_shapefiles(
  files = c(
    # Linhas
    #"LinhaMetroProjeto", "LinhaMetro", "LinhaTrem", "Corredor", "CorredorOnibus",
    # Estações
    "EstacaoMetro", "EstacaoMetroProjeto", "EstacaoTrem"
  )
)

# Output gerado em linhas_tratamento.R
# Requer a execução desse script
corredor_onibus_rmsp_ajust <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte", "corredor_onibus.parquet")
)

# 2. Estrutura -----------------------------------------------------------------

# Metrô
# Unifica estações de metrô existentes e projetadas
# Aqui temos mais de uma observação por eixo/infra de transporte em razão de diferentes geometrias
estacao_metro_ajust <- estacao_metro %>%
  bind_rows(
    estacao_metro_projeto %>%
      filter(emtp_linha %in% c("PRATA", "LARANJA")) %>%
      transmute(
        emt_situac = emtp_situa,
        emt_linha = emtp_linha,
        emt_nome = emtp_nome,
        emt_empres = emtp_empre
      )
  )

# Trem
estacao_trem_ajust <- estacao_trem %>%
  transmute(
    etr_situac,
    etr_linha,
    etr_nome = if_else(str_starts(etr_nome, "ESTAÇÃO "), 
                       str_remove(etr_nome, "ESTAÇÃO "),
                       etr_nome),
    etr_empres
  )

# Ônibus
# Unifica corredores municipais e metropolitanos
# corredor_onibus_rmsp_ajust <- corredor_onibus %>%
#   # Eclui corredor itaquera-líder para atualizar com nova geometria
#   filter(!co_id == 13) %>%
#   mutate(co_operacao = "SPTRANS",
#          municipio = "SAO PAULO") %>%
#   bind_rows(
#     # Inclui corredor itaquera-líder com geometria ajustada manualmente via QGIS
#     corredor_itaquera_lider,
#     # Inclui corredores metropolitanos
#     corredor_onibus_rmsp %>%
#       transmute(
#         co_km = EXT_KM,
#         co_ano = INICIO,
#         co_nome = N_CORR_C,
#         co_operacao = OPERACAO,
#         municipio = MUNIC
#       ) %>%
#       filter(co_operacao != "SPTRANS")
#   ) %>%
#   mutate(co_nome = str_remove_all(co_nome, "CORREDOR "))

# 3. Integra dados -------------------------------------------------------------

# 
eixos_por_ponto_acesso <- bind_rows(
  estacao_metro_ajust %>%
    # group_by(emt_nome) %>%
    group_by(eixo = emt_nome) %>%
    summarize(
      # nome = emt_nome,
      tipo_transp = "METRÔ",
      tipo_infra = "ESTAÇÃO",
      situacao_transp = if_else(any(emt_situac == "OPERANDO"), "OPERANDO", "PROJETADA"),
      ind_area_expansao = if_else(any(emt_linha %in% c("AZUL", "VERMELHA", "PRATA")), 
                                  TRUE, FALSE),
      # Unifica gemometria de corredores com mesmo nome
      geometry = st_combine(geometry)
    ) %>%
    ungroup(),
  estacao_trem_ajust %>%
    group_by(eixo = etr_nome) %>%
    summarize(
      tipo_transp = "TREM",
      tipo_infra = "ESTAÇÃO",
      situacao_transp = if_else(any(etr_situac == "OPERANDO"), "OPERANDO", "PROJETADA"),
      ind_area_expansao = FALSE,
      geometry = st_combine(geometry)
    ) %>%
    ungroup()) %>%
  st_cast(to = "POINT") %>% # De multiponto para ponto
  distinct() %>% # Duplicatas
  bind_rows(
    corredor_onibus_rmsp_ajust %>%
      group_by(eixo = co_nome) %>%
      summarize(
        tipo_transp = "ÔNIBUS",
        tipo_infra = "CORREDOR",
        situacao_transp = "OPERANDO",
        ind_area_expansao = FALSE,
        geometry = st_combine(geometry)) %>%
      ungroup()
  ) 
  
#
eixos <- eixos_por_ponto_acesso %>%
  group_by(eixo, tipo_infra) %>%
  mutate(
    ind_metro = if_else(tipo_transp == "METRÔ", TRUE, FALSE),
    ind_trem = if_else(tipo_transp == "TREM", TRUE, FALSE),
    ind_onibus = if_else(tipo_transp == "ÔNIBUS", TRUE, FALSE),
  ) %>%
  # ungroup() %>%
  # group_by(eixo, tipo_infra) %>%
  summarize(# Alguns eixos são multimodal de transporte, necessário assinalar e ajustar
    n_pontos_acesso = n(),
    tipo_transp = case_when(
      any(tipo_transp == "METRÔ") ~ "METRÔ",
      any(tipo_transp == "TREM") ~ "TREM",
      any(tipo_transp == "ÔNIBUS") ~ "ÔNIBUS",
      TRUE ~ NA_character_),
    tipo_transp_secundario = case_when(
      any(ind_metro) == TRUE & any(ind_trem) == TRUE ~ "TREM",
      any(ind_metro == TRUE) & any(ind_onibus == TRUE) ~ "ÔNIBUS",
      any(ind_trem) == TRUE & any(ind_onibus == TRUE) ~ "ÔNIBUS",
      TRUE ~ NA_character_),
    situacao_transp = case_when(
      any(situacao_transp == "OPERANDO") ~ "OPERANDO",
      TRUE ~ first(situacao_transp)),
    ind_area_expansao = if_else(any(ind_area_expansao == TRUE), TRUE, FALSE)
    ) %>%
  ungroup()


# 4. Exporta para datalake -----------------------------------------------------

#
fs::dir_create(here("inputs", "2_trusted", "Transporte"))

#
sfarrow::st_write_parquet(
  eixos_por_ponto_acesso,
  here("inputs", "2_trusted", "Transporte",
       "eixos_por_ponto_acesso.parquet")
)

#
sfarrow::st_write_parquet(
  eixos,
  here("inputs", "2_trusted", "Transporte",
       "eixos.parquet")
)

#
sfarrow::st_write_parquet(
  estacao_metro,
  here("inputs", "2_trusted", "Transporte",
       "estacao_metro.parquet")
)

#
sfarrow::st_write_parquet(
  estacao_metro_projeto,
  here("inputs", "2_trusted", "Transporte",
       "estacao_metro_projeto.parquet")
)

#
sfarrow::st_write_parquet(
  estacao_trem,
  here("inputs", "2_trusted", "Transporte",
       "estacao_trem.parquet")
)




