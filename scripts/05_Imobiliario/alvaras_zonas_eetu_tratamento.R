# Pacotes
library(here)
library(fs)
library(sf)
library(sfarrow)
library(tidyverse)
library(tidylog)
library(beepr)

# Opções
options(scipen = 99999,
        error = beep)

# 1. Importa --------------------------------------------------------------

# Dados tratados em formato .parquet
alvaras_por_lote <- sfarrow::st_read_parquet(here("inputs", "2_trusted", "Licenciamentos",
                                                  "alvaras_por_lote.parquet"))
#
zonas_eetu <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu.parquet")
)

#
zonas_eetu_potencial_mem <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu_potencial_mem.parquet")
)

# 2. Integra --------------------------------------------------------------

zonas_eetu_potencial <- zonas_eetu %>%
  st_drop_geometry() %>%
  bind_rows(
    zonas_eetu_potencial_mem %>%
      select(-id) %>%
      st_drop_geometry()) %>%
  select(-distancia_cbd)
                                              
#
alvaras_zonas_eetu_por_lote <- alvaras_por_lote %>%
  inner_join(zonas_eetu_potencial)

#
alvaras_zonas_eetu_por_lote %>%
  st_drop_geometry() %>%
  count(tipo_eixo)

alvaras_zonas_eetu_por_lote %>%
  st_drop_geometry() %>%
  count(tipo_transp)

# 3. Exporta para datalake ------------------------------------------------

# Dados tratados em formato .parquet
sfarrow::st_write_parquet(alvaras_zonas_eetu_por_lote, 
                          here("inputs", "2_trusted", "Licenciamentos",
                               "alvaras_zonas_eetu_por_lote.parquet"))



