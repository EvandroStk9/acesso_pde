# Objetivo
# Reprojetar dados geoespaciais para EPSG:4326 e deploy no Geoserver
# Dados requisitados pelas sessões do Terria
# Dados gerais (exceto lotes) são tratados no script 1

library(here)
library(fs)
library(tidyverse)
library(sfarrow)
library(sf)
library(arrow)

#
unix::rlimit_as(13e9, 13e9)

# 1. Reprojeta dados espaciais -------------------------------------------------

#
reproject_geoparquet <- function(){
  # Lista nomes de cada diretório contido na pasta "inputs"
  geoparquet_names <- list.dirs(here("inputs", "2_trusted")) %>%
    map_chr(~ word(.x, -1, sep = "/")) %>%
    str_subset("Pre", negate = TRUE)
  
  
  # Lista todos os arquivos .shp e nomeia de acordo com nome do respectivo diretório
  geoparquet_files <- geoparquet_names %>%
    map(~ list.files(here("inputs", "2_trusted", paste0(.x)), 
                     pattern = ".parquet$", full.names = TRUE)) %>%
    set_names(geoparquet_names) %>%
    compact() %>%
    unlist() %>%
    set_names(paste0(word(., -2, sep = "/"), "/", word(., -1, sep = "/"))) %>%
    discard(~str_detect(.x, ".no_geo")) %>%
    keep(~str_detect(.x, "_lotes|/lotes"))
  
  # Padrão regex para os objetos que queremos encontrar na lista de arquivos
  # files <- files
  
  # Sistema geográfico de referência
  CRS <- "EPSG:4326" # Exigido pelo portal
  
  # Lê shapefiles, aplica CRS e reprojeta para SIRGAS 2000
  geoparquet_list <- geoparquet_files %>%
    map(sfarrow::st_read_parquet) %>%
    map(~ st_transform(.x, crs = CRS)) %>%
    map(~ st_make_valid(.x)) %>%
    map(~ .x %>% mutate(across(where(is.factor), as.character)))
  
  # Importa shapefiles tratadas para o Global Environment
  # list2env(geoparquet_list, envir = .GlobalEnv)
  
  return(geoparquet_list)
}

#
geoparquet_list <- reproject_geoparquet()

#
# map(geoparquet_list, ~any(st_is_valid(.x) == FALSE))


# 2. Cria dummy espacial --------------------------------------------------

dummy_geoparquet <- function(){
  # Lista nomes de cada diretório contido na pasta "inputs"
  parquet_names <- list.dirs(here("inputs", "2_trusted")) %>%
    map_chr(~ word(.x, -1, sep = "/")) %>%
    str_subset("Pre", negate = TRUE)
  
  
  # Lista todos os arquivos .shp e nomeia de acordo com nome do respectivo diretório
  parquet_files <- parquet_names %>%
    map(~ list.files(here("inputs", "2_trusted", paste0(.x)), 
                     pattern = ".parquet$", full.names = TRUE)) %>%
    set_names(parquet_names) %>%
    compact() %>%
    unlist() %>%
    set_names(paste0(word(., -2, sep = "/"), "/", word(., -1, sep = "/"))) %>%
    keep(~str_detect(.x, ".no_geo")) %>%
    keep(~str_detect(.x, "_lotes|/lotes"))
  
  # Padrão regex para os objetos que queremos encontrar na lista de arquivos
  # files <- files
  
  # Sistema geográfico de referência
  # CRS <- "EPSG:4326" # Exigido pelo portal
  
  # Lê shapefiles, aplica CRS e reprojeta para SIRGAS 2000
  dummy_geoparquet_list <- parquet_files %>%
    map(arrow::read_parquet) %>%
    map(~ .x %>%
          # mutate(across(everything(), as.character)) %>%
          mutate(X = 0, Y = 0) %>%
          st_as_sf(coords = c("X", "Y"))) %>%
    map(~ .x %>% mutate(across(where(is.factor), as.character)))
  
  # Importa shapefiles tratadas para o Global Environment
  # list2env(geoparquet_list, envir = .GlobalEnv)
  
  return(dummy_geoparquet_list)
}

#
dummy_geoparquet_list <- dummy_geoparquet()

# 3. Exporta para datalake -----------------------------------------------------

#
fs::dir_create(here("inputs", "3_portal_dados", "Dados"))

#
list.dirs(here("inputs", "2_trusted")) %>%
  map_chr(~ word(.x, -1, sep = "/")) %>%
  str_subset("Pre|2_trusted", negate = TRUE) %>%
  map(~fs::dir_create(here("inputs", "3_portal_dados", "Dados", .x)))

# Geospacial
geoparquet_list %>%
  map2(names(.), 
       ~sfarrow::st_write_parquet(.x,
                                  here("inputs", "3_portal_dados", "Dados",
                                       word(.y, -2, sep = "/"),
                                       paste0(word(.y, -1, sep = "/"))))
  )

# Dummy geospacial
dummy_geoparquet_list %>%
  map2(names(.), 
       ~sfarrow::st_write_parquet(.x,
                                  here("inputs", "3_portal_dados", "Dados",
                                       word(.y, -2, sep = "/"),
                                       paste0(word(.y, -1, sep = "/"))))
  )
