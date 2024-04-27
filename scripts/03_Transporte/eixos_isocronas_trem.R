# Reprodução da metodologia IPEA
# Disponível em: https://ipeagit.github.io/intro_access_book/

# Pacotes
library(here)
library(fs)
library(beepr)
library(tidyverse)
library(sf)
library(sfarrow)
library(r5r)
library(future)

# Funções
source(here("scripts", "functions.R"))
source(here("scripts", "isochrone.R")) # Adiciona função de isochrone()
# Compartilhada via repositório oficial do github r5r, após issue
# Modificação do original: acrescentado parâmetro para ajustar tamanho da amostra

# Opções
options(scipen = 99999,
        error = beep,
        java.parameters = "-Xmx8G") # Parâmetros java conforme metodologia IPEA


# 1. Importa --------------------------------------------------------------

#
conexao_r5r <- setup_r5(here("inputs", "1_inbound", "r5"), 
                        verbose = FALSE)

# Dados requeridos (cf. metodologia IPEA)
dir_tree(here("inputs", "1_inbound", "r5"))

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
get_shapefiles(
  files = c("Bordas", "Estacao", "Corredor")
)

#
eixos <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos.parquet"))

# 2. Estrutura --------------------------------------------------------------

# Divide geometria dos corredores (linha) em pontos com intervalo de 100m entre si
# E atende requisitos para aplicar pacote r5r
eixos_ajust <- eixos %>% 
  nest(data = -tipo_infra) %>%
  mutate(
    data = if_else(
      tipo_infra == "CORREDOR", 
      map(data, 
          ~ .x %>% st_cast("MULTIPOINT") %>% st_cast("LINESTRING") %>%
            mutate(
              geometry = st_line_sample(geometry, density = 1/100)
            ) %>%
            st_cast("POINT")
      ),
      data)
  )  %>%
  unnest(data) %>%
  st_as_sf() %>%
  # st_cast("MULTIPOINT") %>%
  st_intersection(bordas_cidade) %>%
  select(-names(bordas_cidade)) %>%
  st_transform(crs = st_crs(4326)) %>%
  mutate(id = row.names(.),
         lon = map_dbl(geometry, ~st_centroid(.x)[[1]]),
         lat = map_dbl(geometry, ~st_centroid(.x)[[2]]))

# 3. Gera isócronas ------------------------------------------------------------

#
trem_list_poi <- eixos_ajust %>%
  filter(tipo_transp == "TREM") %>%
  st_drop_geometry() %>%
  mutate(nome = paste0(id, "_", eixo)) %>%
  group_split(nome) %>%
  set_names(map_chr(., ~.x$nome[1] %>% as.character()))

#
trem_isocronas_walk <- map(
  trem_list_poi,
  ~isochrone(
    conexao_r5r,
    origin = .x,
    sample_prop = 1,
    mode = c("WALK"),
    departure_datetime = as.POSIXct(
      "22-03-2023 09:00:00",
      format = "%d-%m-%Y %H:%M:%S"
    ),
    cutoffs = c(15, 20, 25, 30),
    n_threads = as.numeric(future::availableCores() - 1)
  )
)

#
trem_isocronas <- list(trem_isocronas_walk) %>%
  set_names("WALK") %>%
  map(~map2_df(.x, names(.x),
               ~ .x %>%
                 mutate(nome = .y))) %>%
  map(bind_rows) %>%
  map2_df(names(.),
          ~ .x %>%
            mutate(mode = .y)) %>%
  arrange(nome, isochrone) %>%
  transmute(
    id = str_extract_all(nome, "[:number:]+_") %>%
      str_remove("_"),
    isocrona = isochrone
  ) %>%
  left_join(st_drop_geometry(eixos_ajust), by = "id")



# 4. Exporta --------------------------------------------------------------

#
sfarrow::st_write_parquet(trem_isocronas,
                          here("inputs", "2_trusted", "Transporte", 
                               "eixos_trem_por_isocrona.parquet"))