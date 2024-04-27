library(here)
library(tidyverse)
library(sf)
library(sfarrow)

# 1. Seleciona casos potenciais ------------------------------------------------

#
alvaras_por_lote <- sfarrow::st_read_parquet(here("inputs", "2_trusted", "Licenciamentos",
                                                  "alvaras_por_lote.parquet"))
# Casos potenciais selecionados
# Cf. Exploratória feita em alvaras_casos_selecao para detalhes do processo de seleção e critérios
alvaras_por_caso_potencial <- alvaras_por_lote %>%
  filter(between(ano_execucao, 2010, 2020)) %>%
  filter(between(area_do_terreno, 10000, 20000)) 

# Arquivo KML com georreferenciamento revisado pela pesquisadora Joyce Ferreira Reis
casos_kml <- st_read(here("inputs", "1_inbound", "Insper", "EstudoCasos", "casos.kml"))

# 2. Seleciona casos ------------------------------------------------------

# Após rodadas de análise qualitativa, foram selecionados entre os casos potenciais
sql_casos <- c("074.378.0003-9", "099.054.0005-2")

#
alvaras_por_caso <- casos_kml %>%
  separate(col = Name, into = c("categoria_de_uso", "bairro", "geom_tipo"), sep = "_") %>%
  transmute(sql_incra = case_when(
    bairro == "FREGUESIA" ~ "074.378.0003-9",
    bairro == "LAPA" ~ "099.054.0005-2"
  ),
  caso = str_c(categoria_de_uso, bairro, sep = "_"),  
  geom_tipo,
  lon = map_dbl(geometry, ~st_centroid(.x)[[1]]),
  lat = map_dbl(geometry, ~st_centroid(.x)[[2]])) %>%
  st_transform(crs = st_crs(4326)) %>%
  left_join(alvaras_por_lote %>%
              st_drop_geometry(), by = "sql_incra") %>%
  # Selecionada a entrada dos empreendimentos
  filter(geom_tipo == "ENTRADA") %>%
  select(id, sql_incra, everything()) %>%
  relocate(c(geom_tipo, lat, lon), .before = "geometry")

# 3. Exporta --------------------------------------------------------------

#
sfarrow::st_write_parquet(alvaras_por_caso_potencial,
                          here("inputs", "2_trusted", "Casos",
                               "casos_potenciais_alvaras.parquet"))

# 
sfarrow::st_write_parquet(alvaras_por_caso,
                          here("inputs", "2_trusted", "Casos",
                               "casos_alvaras.parquet"))

# Casos potenciais
# openxlsx::write.xlsx(here("dados", "3_refined", 
#                           "alvaras_casos_potenciais.xlsx"), 
#                      asTable = FALSE)


