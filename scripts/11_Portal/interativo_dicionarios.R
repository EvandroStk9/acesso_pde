library(here)
library(tidyverse)
library(sfarrow)
library(sf)


# 1. Dados ----------------------------------------------------------------

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
    word(., -1, sep = "/"))) %>%
  discard(~str_detect(.x, "plot_"))


# Lê arquivos
geoparquet_list <- geoparquet_files %>%
  map_if(
    ~is(.x, "sf"),
    ~sfarrow::st_read_parquet(.x, query = "SELECT * FROM table LIMIT 2") %>%
      sf::st_drop_geometry) %>%
  map_if(
    ~!is(.x, "sf"),
    ~arrow::read_parquet(.x, query = "SELECT * FROM table LIMIT 2"))

#
geoparquet_dictionaries <- geoparquet_list %>%
  map2(
    geoparquet_files,
    ~ tibble::tibble(
      atributo = names(.x),
      descricao = NA_character_,
      tipo = map_chr(.x, typeof),
      path = .y
    )
  ) %>%
  map(
    ~ .x %>%
      mutate(
        tipo = case_when(
          tipo %in% c("double", "integer") ~ "Numérico",
          tipo == "character" ~ "Texto",
          tipo == "logical" ~ "Lógico",
          atributo %in% c("geometry", "polygons", "x") ~ "Geográfico",
          TRUE ~ tipo),
        posicao = row_number()
      ) %>%
      select(posicao, everything())
  ) 
  

# 2. Dados de histórias ---------------------------------------------------

#
geoparquet_files_historias <- geoparquet_dirs %>%
  map(~ list.files(here("inputs", "3_portal_dados", 
                        paste0(word(.x, -2, sep = "/")), paste0(word(.x, -1, sep = "/"))), 
                   pattern = ".parquet$",
                   full.names = TRUE)) %>%
  set_names(paste0(word(geoparquet_dirs, -1, sep = "/"))) %>%
  compact() %>%
  unlist() %>%
  set_names(paste0(#word(., -2, sep = "/"), "/", 
    word(., -1, sep = "/"))) %>%
  keep(~str_detect(.x, "plot_"))

#
geoparquet_list_historias <- geoparquet_files_historias %>%
  map_if(
    ~is(.x, "sf"),
    ~sfarrow::st_read_parquet(.x, query = "SELECT * FROM table LIMIT 2") %>%
      sf::st_drop_geometry) %>%
  map_if(
    ~!is(.x, "sf"),
    ~arrow::read_parquet(.x, query = "SELECT * FROM table LIMIT 2"))


#
geoparquet_dictionaries_historias <- geoparquet_list_historias %>%
  map2(
    geoparquet_files_historias,
    ~ tibble::tibble(
      atributo = names(.x),
      descricao = NA_character_,
      tipo = map_chr(.x, typeof),
      path = .y
    )
  ) %>%
  map(
    ~ .x %>%
      mutate(tipo = case_when(
        tipo %in% c("double", "integer") ~ "Numérico",
        tipo == "character" ~ "Texto",
        tipo == "logical" ~ "Lógico",
        atributo %in% c("geometry", "polygons", "x") ~ "Geográfico",
        TRUE ~ tipo
      ))
  )


# 3. Exporta --------------------------------------------------------------


#
map(
  word(geoparquet_dictionaries %>%
         map_df(~distinct(.x, path)) %>%
         pull(path), -2, sep = "/"),
  ~fs::dir_create(
    here("inputs", "metadados", "0_raw", .x)
  )
)

#
map2(
  geoparquet_dictionaries,
  geoparquet_dictionaries %>%
    map_df(~distinct(.x, path)) %>%
    pull(path),
  ~ writexl::write_xlsx(.x %>%
                          select(-path), 
                        here("inputs", "metadados", "0_raw", #"3_portal_dados", "Dados",
                             word(.y, -2, sep = "/"),
                             paste0(word(.y, -1, sep = "/") %>%
                                      str_replace_all(".parquet", "_dicionario.xlsx")))
  )
)

# #
# map2(
#   geoparquet_dictionaries_historias,
#   geoparquet_dictionaries_historias %>%
#     map_df(~distinct(.x, path)) %>%
#     pull(path),
#   ~ writexl::write_xlsx(.x %>%
#                           select(-path), 
#                         here("inputs", "3_portal_dados", "Historias", 
#                              word(.y, -2, sep = "/"),
#                              paste0(word(.y, -1, sep = "/") %>%
#                                       str_replace_all(".parquet", "_dicionario.xlsx")))
#   )
# )
