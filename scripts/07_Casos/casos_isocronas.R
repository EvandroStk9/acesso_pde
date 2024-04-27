# Reprodução da metodologia IPEA
# Disponível em: https://ipeagit.github.io/intro_access_book/


library(here)
library(fs)
library(tidyverse)
library(sf)
library(sfarrow)
library(r5r)
library(future)

# Adiciona função de isochrone()
# Compartilhada via repositório oficial do github r5r, após issue
# Modificação do original: acrescentado parâmetro para ajustar tamanho da amostra
source(here("scripts", "isochrone.R"))

# Parâmetros java estabelecido conforme metodologia IPEA
options(scipen = 999,
        java.parameters = "-Xmx4G")

# 1. Importa --------------------------------------------------------------

#
conexao_r5r <- setup_r5(here("inputs", "1_inbound", "r5"), 
                        verbose = FALSE)

# Dados requeridos (cf. metodologia IPEA)
dir_tree(here("inputs", "1_inbound", "r5"))

#
alvaras_por_caso <- sfarrow::st_read_parquet(here("inputs", "2_trusted", "Casos",
                                                  "alvaras_por_caso.parquet"))

# 2. Gera isócronas -------------------------------------------------------

#
list_poi <- alvaras_por_caso %>%
  st_drop_geometry() %>%
  group_split(caso) %>%
  set_names(map_chr(., ~.x$caso[1] %>% as.character()))

# debug(isochrone)

#
isocronas_transit <- map(list_poi,
                         ~isochrone(
                           conexao_r5r,
                           origin = .x,
                           sample_prop = 1,
                           mode = c("WALK", "TRANSIT"),
                           departure_datetime = as.POSIXct(
                             "22-03-2023 09:00:00",
                             format = "%d-%m-%Y %H:%M:%S"
                           ),
                           cutoffs = c(0, 5, 10, 15, 20, 25, 30),
                           n_threads = as.numeric(future::availableCores() - 1)
                         )
)

#
isocronas_bike <- map(list_poi,
                      ~isochrone(
                        conexao_r5r,
                        origin = .x,
                        sample_prop = 1,
                        mode = c("BICYCLE"),
                        departure_datetime = as.POSIXct(
                          "22-03-2023 09:00:00",
                          format = "%d-%m-%Y %H:%M:%S"
                        ),
                        cutoffs = c(0, 5, 10, 15, 20, 25, 30),
                        n_threads = as.numeric(future::availableCores() - 1)
                      )
)

#
isocronas_walk <- map(list_poi,
                      ~isochrone(
                        conexao_r5r,
                        origin = .x,
                        sample_prop = 1,
                        mode = c("WALK"),
                        departure_datetime = as.POSIXct(
                          "22-03-2023 09:00:00",
                          format = "%d-%m-%Y %H:%M:%S"
                        ),
                        cutoffs = c(0, 5, 10, 15, 20, 25, 30),
                        n_threads = as.numeric(future::availableCores() - 1)
                      )
)

#
modes <- c("WALK", "BICYCLE", "TRANSIT")

#
casos_por_isocrona <- list(isocronas_walk, isocronas_bike, isocronas_transit) %>%
  set_names(modes) %>%
  map(~map2_df(.x, names(.x),
               ~ .x %>%
                 mutate(caso = .y))) %>%
  map(bind_rows) %>%
  map2_df(names(.),
          ~ .x %>%
            mutate(mode = .y)) %>%
  arrange(caso, isochrone) %>%
  left_join(st_drop_geometry(alvaras_por_caso))


# 3. Exporta para datalake -----------------------------------------------------

#
sfarrow::st_write_parquet(casos_por_isocrona,
                          here("inputs", "2_trusted", "Casos",
                               "isocronas_casos.parquet"))
