library(here)
library(fs)
library(tidyverse)
library(sfarrow)
library(sf)
library(gt)
library(scales)
library(ggrepel)
library(ggtext)
library(patchwork)
library(ggthemes)


# 1.  ---------------------------------------------------------------------


# Sistema de coordenadas Geográficas de referência SIRGAS 
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"



# Setores da MEM
mem_por_setor <- 
  # st_read(here("inputs", 
  #                                  "Complementares","Macroareas",
  #                                  "MEMSetores",
  #                                  "sirgas_PDE_2A-Setores-MEM.shp"),
  #                             crs = CRS) %>%
  sfarrow::st_read_parquet(here("inputs", "2_trusted",
                                "Legislacao",
                                "mem_por_setor.parquet")) %>%
  transmute(
    mem_setor,
    mem_setor_grupo,
    area_edificavel = unclass(st_area(.))
  ) %>%
  group_by(mem_setor) %>%
  summarize(
    mem_setor_grupo = first(mem_setor_grupo),
    area_edificavel = sum(area_edificavel)) %>%
  st_make_valid() %>%
  # st_transform(crs = 4674) %>%
  mutate(X = map_dbl(geometry, ~st_centroid(.x)[[1]]),
         Y = map_dbl(geometry, ~st_centroid(.x)[[2]]))

#
alvaras_por_lote <- 
  sfarrow::st_read_parquet(here("inputs", "2_trusted",
                                "Licenciamentos",
                                "alvaras_por_lote.parquet")) %>%
  # left_join(st_drop_geometry(geo_sp_macroarea), by = "macroarea") %>%
  left_join(st_drop_geometry(mem_por_setor), by = c("mem_setor", "mem_setor_grupo")) %>%
  filter(ano_execucao >=2013) %>%
  mutate(
    periodo = case_when(
      between(ano_execucao, 2013, 2015) ~ "2013-2015",
      between(ano_execucao, 2016, 2018) & legislacao == "PDE2002eLPUOS2004" ~ "2016-2018 (Pré-PDE)",
      between(ano_execucao, 2016, 2018) & legislacao == "PDE2014eLPUOS2016" ~ "2016-2018 (Pós-PDE)",
      between(ano_execucao, 2019, 2021) ~ "2019-2021",
      TRUE ~ NA_character_) %>%
      factor(levels = c("2013-2015", "2016-2018 (Pré-PDE)", 
                        "2016-2018 (Pós-PDE)", "2019-2021")),
    legislacao = factor(legislacao, levels =  c("PDE2002eLPUOS2004", "PDE2014eLPUOS2004", "PDE2014eLPUOS2016")),
    porte = case_when(
      n_unidades < 50 ~ "Pequeno porte",
      between(n_unidades, 50, 199) ~ "Médio porte",
      n_unidades >= 200 ~ "Grande porte",
      TRUE ~ NA_character_),
    mem_setor_grupo_id = case_when(mem_setor_grupo == "CENTRO" ~ 2,
                                   mem_setor_grupo == "EIXO" ~ 1,
                                   mem_setor_grupo == "ORLA" ~ 3),
    mem_setor = fct_reorder(mem_setor, as.numeric(mem_setor_grupo)),
    zoneamento_grupo = case_when(
      zoneamento %in% c("ZDE-1", "ZDE-2", "ZPI") ~ "ZDE&ZPI",
      TRUE ~ as.character(zoneamento_grupo)
    ),
    fx_area_terreno = fct_reorder(
      case_when(between(area_do_terreno, 0, 4999.999) ~ "0 a 5000 m²",
                between(area_do_terreno, 5000, 9999.999) ~ "5000 a 10000 m²",
                between(area_do_terreno, 10000,  19999.999) ~ "10000 a 20000m²",
                # between(area_do_terreno, 20000, 39999.999) ~ "20000 a 40000m²",
                area_do_terreno >= 20000 ~ "Mais que 20000 m2",
                TRUE ~ "Sem informação"), 
      replace_na(area_do_terreno, 0))
  ) 

#
alvaras_por_unidade <- alvaras_por_lote %>%
  # st_drop_geometry() %>%
  filter(!is.na(n_unidades)) %>%
  uncount(n_unidades, .remove = FALSE)

#
# mem_eixos_por_quadra <- sfarrow::st_read_parquet(here("outputs", "MEM",
#                                                       "mem_eixos_por_quadra.parquet")) %>%
#   st_transform(4674)

#
zonas_eetu <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu.parquet")) %>%
  select(-isocrona, -distancia_cbd) %>%
  mutate(
    across(
      c(eixo, tipo_infra, #tipo_transp
      ),
      str_to_title
    )
  )
#
zonas_eetu_potencial <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu_potencial_mem.parquet")) %>%
  bind_rows(zonas_eetu)


# 2.  ---------------------------------------------------------------------

# n_empreendimentos
plot_serie_macroareas_lotes <- alvaras_por_lote %>% 
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao, macroarea) %>%
  summarize(n = n())

# Unidades
plot_serie_macroareas_unidades <- alvaras_por_lote %>% 
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao, macroarea) %>%
  summarize(n = sum(n_unidades, na.rm = TRUE))


# Área de terreno
plot_serie_macroareas_area_terreno <- alvaras_por_lote %>% 
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao, macroarea) %>%
  summarize(n = sum(area_do_terreno, na.rm = TRUE)) 

# Area construída 
plot_serie_macroareas_area_construcao <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao, macroarea) %>%
  summarize(n = sum(area_da_construcao, na.rm = TRUE))


# 3. ----------------------------------------------------------------------

#
plot_mem_media_unidades <- 
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  filter(macroarea == "MEM") %>%
  group_by(ano_execucao) %>%
  summarize(n_unidades = mean(n_unidades, na.rm = TRUE)) 

#
plot_mem_unidades_area_media <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  group_by(ano_execucao) %>%
  filter(ano_execucao >=2013) %>%
  filter(macroarea == "MEM") %>%
  summarize(area_por_unidade = mean(area_da_construcao/n_unidades, na.rm = TRUE))

#
plot_mem_cota_parte <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  filter(macroarea == "MEM") %>%
  group_by(ano_execucao) %>%
  summarize(cota_parte = mean(cota_parte, na.rm = TRUE))

#
plot_mem_pavimentos <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  filter(macroarea == "MEM") %>%
  group_by(ano_execucao) %>%
  summarize(n_pavimentos_por_bloco = mean(n_pavimentos_por_bloco, na.rm = TRUE))


# 4.  ---------------------------------------------------------------------

# Setores da MEM por número de unidades e percentual de unidades ERP - 2

plot_mem_setores_pavimentos <- alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(legislacao == "PDE2014eLPUOS2016") %>%
  filter(!is.na(mem_setor)) %>%
  group_by(mem_setor) %>% 
  summarize(n_unidades = sum(n_unidades, na.rm = TRUE),
            n_pavimentos_por_bloco = mean(n_pavimentos_por_bloco, na.rm = TRUE)) 

# xx.  --------------------------------------------------------------------

#
fs::dir_create(here("inputs", "3_portal_dados", 
                    "Historias", "04_EnquantoOsPIUSnaoSaemDoPapel"))

list_outputs <- mget(ls(.GlobalEnv, pattern = "plot_"), envir = .GlobalEnv)

names(list_outputs)

list_outputs %>%
  map2(names(.), 
       ~writexl::write_xlsx(.x,
                            here("inputs", "3_portal_dados", "Historias", 
                                 "04_EnquantoOsPIUSnaoSaemDoPapel", 
                                 paste0(.y, ".xlsx")))
  )

