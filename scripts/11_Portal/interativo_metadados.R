# Objetivo
# Reprojetar dados geoespaciais para EPSG:4326 e deploy no Geoserver
# Dados requisitados pelas sessões do Terria

library(here)
library(fs)
library(tidyverse)
library(sfarrow)
library(sf)
library(arrow)
library(writexl)



# 1. Estrutura  --------------------------------------------------------------

#
geoparquet_dirs <- list.dirs(here("inputs", "3_portal_dados"), recursive = TRUE)


# Lista todos os arquivos .shp e nomeia de acordo com nome do respectivo diretório
geoparquet_files <- geoparquet_dirs %>%
  map(~ list.files(here("inputs", "3_portal_dados", 
                        paste0(word(.x, -2, sep = "/")), paste0(word(.x, -1, sep = "/"))), 
                   pattern = ".parquet$",
                   full.names = TRUE)) %>%
  set_names(paste0(word(geoparquet_dirs, -1, sep = "/"))) %>%
  compact() %>%
  unlist() %>%
  set_names(paste0(#word(., -2, sep = "/"), "/", 
                   word(., -1, sep = "/")))

#
metadados <- tibble(
  #"Conjunto de dados" = NA_character_,
  "Nome do arquivo" = sort(names(geoparquet_files))
)


# 2. Exporta --------------------------------------------------------------

#
fs::dir_create(
  here("inputs", "metadados", "0_raw")
)

#
writexl::write_xlsx(metadados, here("inputs", "metadados", "0_raw", "metadados_acesso_pde.xlsx"))

