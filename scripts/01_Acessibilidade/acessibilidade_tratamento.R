# Pacotes
library(here)
library(fs)
library(sf)
library(tidyverse)
library(tidylog)

# Funções
source(here("scripts", "functions.R"))

# Opções
options(scipen = 99999)

# 1. Importa --------------------------------------------------------------------

#
get_shapefiles(
  files = c("Acessibilidade", "Bordas")
)

CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"


# 2. Estrutura ------------------------------------------------------------


#
acessibilidade_empregos <- acessibilidade_empregos2019_shp %>%
  transmute(
    acessibilidade_empregos = accssbl,
    # accssbl = log(accssbl)
    # accssbl = rescale(accssbl, to = c(0, 1)),
    fx_acessibilidade_empregos = case_when(
      accssbl <= quantile(accssbl, 0.3) ~ "Muito baixa",
      between(accssbl, quantile(accssbl, 0.301),
              quantile(accssbl, 0.5)) ~ "Baixa",
      between(accssbl, quantile(accssbl, 0.501),
              quantile(accssbl, 0.7)) ~ "Média",
      between(accssbl, quantile(accssbl, 0.701),
              quantile(accssbl, 0.9)) ~ "Alta",
      accssbl > quantile(accssbl, 0.9) ~ "Muito alta") %>%
      fct_reorder(accssbl))

#
acessibilidade_empregos_por_grade <- bordas_cidade %>%
  st_make_grid(cellsize = c(1500,1500), crs = CRS, 
                     what = 'polygons') %>%
  st_sf('geometry' = ., data.frame(id_grade = seq_along(.))) %>%
  st_intersection(bordas_cidade) %>%
  select(id_grade) %>%
  st_join(acessibilidade_empregos, join = st_nearest_feature)



# 3. Exporta para datalake -----------------------------------------------------

#
fs::dir_create(here("inputs", "2_trusted", "Acessibilidade"))

#
sfarrow::st_write_parquet(
  acessibilidade_empregos,
  here("inputs", "2_trusted", "Acessibilidade",
       "acessibilidade_empregos.parquet")
)

#
sfarrow::st_write_parquet(
  acessibilidade_empregos_por_grade,
  here("inputs", "2_trusted", "Acessibilidade",
       "acessibilidade_empregos_por_grade.parquet")
)

