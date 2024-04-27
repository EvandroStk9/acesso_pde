# Pacotes
library(here)
library(fs)
library(beepr)
library(sf)
library(tidyverse)
library(tidylog)

# Funções
source(here("scripts", "functions.R"))

# Opções
options(scipen = 99999,
        error = beep)

# 1. Importa -------------------------------------------------------------------

# Sistema de coordenadas Geográficas de referência SIRGAS 
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
get_shapefiles(
  files = c(
    # Transporte 
    "Linha", "CorredorOnibus", "CorredorItaquera", "RedeRuas"
  )
)

# Unifica corredores municipáis e metropolitanos
corredor_onibus <- corredor_onibus %>%
  # Eclui corredor itaquera-líder para atualizar com nova geometria
  filter(!co_id == 13) %>%
  mutate(co_operacao = "SPTRANS",
         municipio = "SAO PAULO") %>%
  bind_rows(
    # Inclui corredor itaquera-líder com geometria ajustada manualmente via QGIS
    corredor_itaquera_lider,
    # Inclui corredores metropolitanos
    corredor_onibus_rmsp %>%
      transmute(
        co_km = EXT_KM,
        co_ano = INICIO,
        co_nome = N_CORR_C,
        co_operacao = OPERACAO,
        municipio = MUNIC
      ) %>%
      filter(co_operacao != "SPTRANS")
  ) %>%
  mutate(co_nome = str_remove_all(co_nome, "CORREDOR "))

# 2. Exporta para datalake -----------------------------------------------------

outputs <- c(
  "linha_metro", "linha_metro_projeto", 
  "linha_trem",
  "corredor_onibus",
  "rede_ruas")

#
list_outputs <- mget(outputs) %>%
  set_names(outputs)

# Cria diretório
fs::dir_create(here("inputs", "2_trusted", "Transporte"))

#
list_outputs %>%
  keep(~is(.x, "sf")) %>%
  map2(names(.), 
       ~sfarrow::st_write_parquet(.x,
                                  here("inputs", "2_trusted", "Transporte",
                                       paste0(.y, ".parquet")))
  )
