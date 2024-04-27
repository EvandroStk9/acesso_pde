# Pacotes
library(here)
library(fs)
library(beepr)
library(tidyverse)
library(sf)
library(sfarrow)
library(furrr)

# Opções
options(error = beep)

# 1. Importa --------------------------------------------------------------

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

# Lotes por subprefeitura
# Os lotes são disponibilizados na plataforma Geosampa para cada subprefeituras
# Necessário primeiramente unificar todos os dados em uma base comum
list_lotes <- list.dirs(here("inputs", "Complementares", "Lotes")) %>%
  set_names(map_chr(., ~ str_extract(.x, "[0-9].*"))) %>%
  map(~ list.files(.x, pattern = ".shp$", full.names = TRUE)) %>%
  compact() %>%
  map(st_read) %>%
  map_if(~ is.na(st_crs(.x)), ~ st_set_crs(.x, value = CRS)) %>%
  map(~ st_transform(.x, crs = CRS) %>%
        st_make_valid())

# Lotes
lotes <- bind_rows(list_lotes) %>%
  mutate(area_lote = unclass(st_area(.)))

# Zoneamento
zoneamento <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao",
       "zoneamento.parquet"))


# 2. Integra dados  ------------------------------------------------------------

# Estimativa: 2264.651 segundos/ 37.3 minutos
lotes_ajust <- lotes %>%
  st_join(zoneamento %>% select(id_zoneamento), largest = TRUE) %>%
  left_join(zoneamento %>% st_drop_geometry(), by = "id_zoneamento")

# 3. Exporta para datalake -----------------------------------------------------

#
fs::dir_create(here("inputs", "2_trusted", "Legislacao"))

# Exporta input
sfarrow::st_write_parquet(lotes_ajust, 
                          here("inputs", "2_trusted", "Legislacao", 
                               "lotes.parquet"))
