# Pacotes
library(here)
library(fs)
library(sf)
library(tidyverse)
library(beepr)
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
    # Zoneamento
    "Macroareas", 
    # Intervenção urbana
    "OperacaoUrbana", "PIU", "AIU"
    )
)


# 2. Estrutura ------------------------------------------------------------

pius <- pius %>%
  ## Problema com double encoding!!!
  mutate(across(where(is.character), 
                ~iconv(.x, to="latin1", from="utf-8") %>%
                  iconv(to="ASCII//TRANSLIT", from="utf-8") %>%
                  str_to_upper(locale = "br"))) %>%
  st_make_valid() %>%
  transmute(piu = Layer %>%
              str_remove("CONCESSAO DOS TERMINAIS - ")) %>%
  st_cast("MULTIPOLYGON") %>%
  st_make_valid() 

# aiu_perimetro <- st_transform(aiu_perimetro, crs = "EPSG:4326")

# 3. Exporta para datalake -----------------------------------------------------

#
list_outputs <- mget(ls(.GlobalEnv))

# Cria diretório
fs::dir_create(here("inputs", "2_trusted", "Legislacao"))

#
list_outputs %>%
  keep(~is(.x, "sf")) %>%
  map2(names(.), 
       ~sfarrow::st_write_parquet(.x,
                                  here("inputs", "2_trusted", "Legislacao",
                                       paste0(.y, ".parquet")))
  )

