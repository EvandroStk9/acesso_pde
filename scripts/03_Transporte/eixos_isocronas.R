library(here)
library(fs)
library(sfarrow)
library(sf)
library(tidyverse)

# 1. Importa --------------------------------------------------------------

#
eixos <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos.parquet"))

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
metro_isocronas <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos_metro_por_isocrona.parquet"))
#
trem_isocronas <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos_trem_por_isocrona.parquet"))

#
onibus_isocronas <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos_onibus_por_isocrona.parquet"))


# 2. Intregra ------------------------------------------------------------

#
eixos_por_isocrona <- metro_isocronas %>%
  bind_rows(trem_isocronas) %>%
  bind_rows(onibus_isocronas) %>%
  # left_join(st_drop_geometry(eixos_ajust), by = "id") %>%
  st_transform(crs = CRS) %>%
  st_make_valid()

# 3. Dissolve -------------------------------------------------------------

# Demanda para dataviz: isócronas dissolvidas

#
isocronas_eixos_15min_dissolved <- st_union(st_union(st_make_valid(metro_isocronas %>% 
                                                                     filter(isocrona == 15))),
                                            st_union(st_make_valid(trem_isocronas %>% 
                                                                     filter(isocrona == 15)))) %>%
  st_make_valid() %>%
  st_union(st_union(st_make_valid(onibus_isocronas %>% 
                                    filter(isocrona == 15))) %>%
             st_make_valid()) %>%
  st_cast("POLYGON") %>%
  st_as_sf()

#
isocronas_eixos_20min_dissolved <- st_union(st_union(st_make_valid(metro_isocronas %>% 
                                                                     filter(isocrona == 20))),
                                            st_union(st_make_valid(trem_isocronas %>% 
                                                                     filter(isocrona == 20)))) %>%
  st_make_valid() %>%
  st_union(st_union(st_make_valid(onibus_isocronas %>% 
                                    filter(isocrona == 20))) %>%
             st_make_valid()) %>%
  st_cast("POLYGON") %>%
  st_as_sf()

#
isocronas_eixos_25min_dissolved <- st_union(st_union(st_make_valid(metro_isocronas %>% 
                                                                     filter(isocrona == 25))),
                                            st_union(st_make_valid(trem_isocronas %>% 
                                                                     filter(isocrona == 25)))) %>%
  st_make_valid() %>%
  st_union(st_union(st_make_valid(onibus_isocronas %>% 
                                    filter(isocrona == 25))) %>%
             st_make_valid()) %>%
  st_cast("POLYGON") %>%
  st_as_sf()

#
isocronas_eixos_30min_dissolved <- st_union(st_union(st_make_valid(metro_isocronas %>% 
                                                                     filter(isocrona == 30))),
                                            st_union(st_make_valid(trem_isocronas %>% 
                                                                     filter(isocrona == 30)))) %>%
  st_make_valid() %>%
  st_union(st_union(st_make_valid(onibus_isocronas %>% 
                                    filter(isocrona == 30))) %>%
             st_make_valid()) %>%
  st_cast("POLYGON") %>%
  st_as_sf()


# 4. Exporta --------------------------------------------------------------

#
sfarrow::st_write_parquet(eixos_por_isocrona,
                          here("inputs", "2_trusted", "Transporte", 
                               "eixos_por_isocrona.parquet"))

#
sfarrow::st_write_parquet(isocronas_eixos_15min_dissolved,
                          here("inputs", "2_trusted", "Transporte", 
                               "eixos_por_isocrona_15min_dissolved.parquet"))

sfarrow::st_write_parquet(isocronas_eixos_20min_dissolved,
                          here("inputs", "2_trusted", "Transporte", 
                               "eixos_por_isocrona_20min_dissolved.parquet"))

sfarrow::st_write_parquet(isocronas_eixos_25min_dissolved,
                          here("inputs", "2_trusted", "Transporte", 
                               "eixos_por_isocrona_25min_dissolved.parquet"))

sfarrow::st_write_parquet(isocronas_eixos_30min_dissolved,
                          here("inputs", "2_trusted", "Transporte", 
                               "eixos_por_isocrona_30min_dissolved.parquet"))