library(here)
library(tidyverse)
library(sf)
library(sfarrow)

# Functions
source(here("scripts", "functions.R"))

# 1. Importa --------------------------------------------------------------

#
CRS = "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
get_shapefiles(
  files = c("Amenities", "Orbis")
)

#
casos_por_isocrona <- sfarrow::st_read_parquet(here("inputs", "2_trusted", "Casos",
                                                 "isocronas_casos.parquet")) %>%
  group_by(id, caso, mode) %>%
  # Cria hierarquia das isocronas por tempo de deslocamento
  mutate(hierarquia = rank(isochrone, ties.method = "first")) %>%
  ungroup() %>%
  select(isochrone, hierarquia, everything())

#
alvaras_por_caso <- sfarrow::st_read_parquet(here("inputs", "2_trusted", "Casos",
                                                  "casos_alvaras.parquet"))

# 2. Contabiliza oportunidades -------------------------------------------------

#
acesso_amenidades <- amenities_spl_geocoded_clean_class_shp %>%
  st_transform(crs = CRS) %>%
  # Atribui o identificador do caso/isócrona
  st_join(casos_por_isocrona %>% 
            st_transform(crs = CRS), join = st_within, 
          left = FALSE) %>%
  st_drop_geometry() %>%
  # Contabiliza o total de oportunidades do caso/isócrona
  group_by(caso, isochrone, mode) %>%
  summarize(n_amenidades = n())

#  
acesso_empresas <- orbis_spl_geocoded_clean_class_shp %>%
  st_transform(crs = CRS) %>%
  # Atribui o identificador do caso/isócrona
  st_join(casos_por_isocrona %>% 
            st_transform(crs = CRS), join = st_intersects, 
          left = FALSE) %>%
  st_drop_geometry() %>%
  # Contabiliza o total de oportunidades do caso/isócrona
  group_by(caso, isochrone, mode) %>%
  summarize(n_empresas = n())

#
# acesso_imoveis <- alvaras_por_lote %>%
#   st_transform(crs = CRS) %>%
#   # Atribui o identificador do caso/isócrona
#   st_join(isocronas %>% 
#             st_transform(crs = CRS), join = st_intersects, 
#           left = FALSE) %>%
#   st_drop_geometry() %>%
#   # Contabiliza o total de oportunidades do caso/isócrona
#   group_by(caso, isochrone, mode) %>%
#   summarize(n_imoveis = n())

#
acesso_por_isocrona <- casos_por_isocrona %>%
  left_join(acesso_amenidades) %>%
  left_join(acesso_empresas) %>%
  # Contabiliza oportunidades 
  rowwise() %>%
  mutate(
    n_oportunidades_acumuladas = sum(across(starts_with("n")))) %>%
  ungroup() %>%
  group_by(caso, mode) %>%
  mutate(
    # n_empresas = n_empresas - lag(n_empresas, default = 0),
    n_oportunidades = n_oportunidades_acumuladas - lag(n_oportunidades_acumuladas,
                                                       default = 0))

# 2. Estima acessibilidade ------------------------------------------------
#
acessibilidade_por_isocronas <- acesso_por_isocrona %>%
  # Estima acessibilidade
  group_by(caso, mode, isochrone) %>%
  mutate(
    # acessibilidade_empresas = 0.5^(hierarquia - 1)*n_empresas,
    acessibilidade = 0.5^(hierarquia - 1)*n_oportunidades
  ) %>%
  ungroup() %>%
  relocate(polygons, .after = everything())

#
acessibilidade_por_caso <- acessibilidade_por_isocronas %>%
  st_drop_geometry() %>%
  group_by(caso) %>%
  summarize(
    # acessibilidade_empresas_r5r = sum(acessibilidade_empresas),
    acessibilidade_r5r = sum(acessibilidade))


# 3. Integra dados -------------------------------------------------
#
casos <- alvaras_por_caso %>%
  left_join(acessibilidade_por_caso) %>%
  select(id, sql_incra, caso, acessibilidade_r5r, #acessibilidade_empresas_r5r, 
         acessibilidade_empregos, fx_acessibilidade_empregos, everything()) %>%
  relocate(c(geom_tipo, lat, lon), .before = "geometry")

#
amenidades_casos <- amenities_spl_geocoded_clean_class_shp %>%
  st_transform(crs = CRS) %>%
  st_filter(casos_por_isocrona %>%
              st_transform(crs = CRS)) %>%
  st_transform(crs = 4326)

#
empresas_casos <- orbis_spl_geocoded_clean_class_shp %>%
  st_transform(crs = CRS) %>%
  st_filter(casos_por_isocrona %>%
              st_transform(crs = CRS)) %>%
  st_transform(crs = 4326)

# 4. Exporta --------------------------------------------------------------

#
sfarrow::st_write_parquet(casos,
                          here("inputs", "2_trusted", "Casos",
                               "casos.parquet"))

#
sfarrow::st_write_parquet(amenidades_casos,
                          here("inputs", "2_trusted", "Casos",
                               "casos_amenidades.parquet"))

#
sfarrow::st_write_parquet(empresas_casos,
                          here("inputs", "2_trusted", "Casos",
                               "casos_empresas.parquet"))
