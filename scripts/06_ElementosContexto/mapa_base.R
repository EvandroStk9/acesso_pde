# Pacotes
library(here)
library(fs)
library(beepr)
library(sf)
library(tidyverse)
library(tidylog)
library(janitor)
library(lubridate)
library(readxl)
library(ggtext)
library(patchwork)
library(ggrepel)
library(ggthemes)
library(scales)
library(gt)

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
    # Administrativo
    "BordasCidade", "Distritos", "Subprefeitura",
    # Meio Físico
    "AreasVerdes", "Rios", "CorposDagua"
  )
)

# 2. Exporta para datalake -----------------------------------------------------

#
list_outputs <- mget(ls(.GlobalEnv))

# Cria diretório
fs::dir_create(here("inputs", "2_trusted", "ElementosContexto"))

#
list_outputs %>%
  keep(~is(.x, "sf")) %>%
  map2(names(.), 
       ~sfarrow::st_write_parquet(.x,
                                  here("inputs", "2_trusted", "ElementosContexto",
                                       paste0(.y, ".parquet")))
)

# 3 -----------------------------------------------------------------------

#
# subprefeituras_ajust <- subprefeituras %>%
#   mutate(X = map_dbl(geometry, ~st_centroid(.x)[[1]]),
#          Y = map_dbl(geometry, ~st_centroid(.x)[[2]]))

# # Zoom na mancha urbana de São Paulo
# bbox_sp <- subprefeituras %>%
#   st_transform(crs = CRS) %>%
#   filter(sp_id %in% c(1:24)) %>%
#   st_bbox()
# 
# # Criando mapa base de São Paulo
# geo_sp <- 
#   ggplot() +
#   geom_sf(data = subprefeituras, size = 0.25, color = "black", fill = "white") +
#   geom_sf(data = areas_verdes, fill = "#00800C", alpha = 0.7, color = NA) +
#   geom_sf(data = corpos_dagua, fill = "#191970", alpha = 0.7, color = NA) +
#   geom_sf(data = rios_principais, color = "#191970") +
#   # geom_sf(data = rede_ruas, fill = "black", size = 0.15, alpha = 0.3) +
#   geom_sf(data = bordas_cidade, alpha = 0.5, color = "black", fill = NA) +
#   labs(x = "", y ="") +
#   theme_bw(base_family = "lato", base_size = 14) +
#   theme(axis.text = element_blank(),
#         axis.ticks = element_blank(),
#         panel.grid = element_blank(),
#         plot.caption = element_markdown()) +
#   # coord_sf(xlim = st_bbox(bbox_sp)[c(1, 3)],
#   #          ylim = st_bbox(bbox_sp)[c(2, 4)]) 
#   coord_sf(xlim = bbox_sp[c(1, 3)],
#            ylim = bbox_sp[c(2, 4)])
# 
# 
# # Mapa simplificado - sem marcação de cores adicionais (área verde e corpos d'agua)
# geo_sp_simple <- 
#   ggplot() +
#   geom_sf(data = subprefeituras, size = 0.25, color = "black", fill = "white") +
#   # geom_sf(data = rede_ruas, fill = "black", size = 0.15, alpha = 0.3) +
#   geom_sf(data = bordas_cidade, alpha = 0.5, color = "black", fill = NA) +
#   labs(x = "", y ="") +
#   theme_bw(base_family = "lato", base_size = 14) +
#   theme(axis.text = element_blank(),
#         axis.ticks = element_blank(),
#         panel.grid = element_blank(),
#         plot.caption = element_markdown()) +
#   coord_sf(xlim = bbox_sp[c(1, 3)],
#            ylim = bbox_sp[c(2, 4)])

#
# rm(RedeRuas, RiosPrincipais, BordasCidade, CorposDagua, AreasVerdes, CorposDagua) %>%
#   gc()

