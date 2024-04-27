# Pacotes
library(here)
library(fs)
library(beepr)
library(sf)
library(tidyverse)
library(tidylog)
library(skimr)
library(sfarrow)


# 1. Importa --------------------------------------------------------------

# Lotes
lotes <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao",
       "lotes.parquet"))
#
zonas_eetu <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu.parquet")
) %>%
  select(id_zoneamento, decreto_eixo, eixo, tipo_infra, n_pontos_acesso,
         tipo_transp, tipo_transp_secundario, situacao_transp, tipo_eixo,
         isocrona, area_eixo, distancia_cbd)


# 2. Integra --------------------------------------------------------------

#
zonas_eetu_lotes <- lotes %>%
  filter(id_zoneamento %in% zonas_eetu$id_zoneamento) %>%
  st_join(zonas_eetu %>% select(-id_zoneamento), largest = TRUE)


# 3. Exporta para o datalake ----------------------------------------------

#
sfarrow::st_write_parquet(zonas_eetu_lotes,
                          here("inputs", "2_trusted", "Eixos",
                               "zonas_eetu_lotes.parquet"))

#
beepr::beep(2)

