# PREPARACAO DE DADOS PARA MODELAGEM - DETERMINANTES DO MERCADO IMOBILIÁRIO SP
# FORMATO 1: ÁREA DE PONDERAÇÃO E REGRESSÃO LINEAR MÚLTIPLA

library(here)
library(beepr)
library(sf)
library(arrow)
library(sfarrow)
library(raster)
library(tidyverse)
library(tidylog)
library(geobr) #devtools::install_github("ipeaGIT/geobr", subdir = "r-package")

# here::i_am("inputs/Alvaras/ArquivosTratados/Alvaras.shp")

options(error = beep)

# 0. Importação ----------------------------------------------------------------
#
# parquet_list <- map(here("inputs", "Fatores"),
#                     ~ list.files(.x, pattern = ".parquet$", full.names = TRUE)) %>%
#   flatten() %>%
#   set_names(~ map_chr(.x, ~ word(.x, -1, sep = "/") %>%
#                         str_remove(".parquet"))) %>%
#   map(sfarrow::st_read_parquet)
# 
# #
# list2env(parquet_list, envir = .GlobalEnv)


# 1. Unidade observacional ------------------------------------------------

# Unidade observacional = área de ponderação de São Paulo
unidade <- geobr::read_weighting_area(code_weighting = 3550308) %>% 
  transmute(id_unidade = code_weighting,
            distancia = round(as.numeric(
              st_distance(., data.frame(long = -46.633959, lat= -23.550382) %>% # Praça da Sé
                            st_as_sf(coords = c("long", "lat"),
                                     crs = 4674))) / 1000, 1),
            area = unclass(st_area(.))/1000000)

# 2. Licenciamentos ----------------------------------------------

# Y = {alvaras} 

# Licenciamentos imobiliários
alvaras_por_lote <- sfarrow::st_read_parquet(here("inputs", "2_trusted",
                                                  "Licenciamentos",
                                                  "alvaras_por_lote.parquet"))

## soma unidades e soma de empreendimentos no setor no período (flexível)
#
alvaras_por_unidade_pos_pde <- alvaras_por_lote %>%
  filter(between(ano_execucao, 2013, 2021)) %>% # t = {2013, ..., 2021}
  filter(legislacao == "PDE2014eLPUOS2016") %>%
  st_join(unidade, join = st_within) %>%
  st_drop_geometry() %>%
  group_by(id_unidade) %>%
  summarize(
    n_empreendimentos_por_km2 = n()/ first(area),
    n_empreendimentos_erm_por_km2 = length(categoria_de_uso_grupo[which(categoria_de_uso_grupo == "ERM")])/ first(area),
    n_empreendimentos_erp_por_km2 = length(categoria_de_uso_grupo[which(categoria_de_uso_grupo  == "ERP")])/ first(area),
    n_unidades_por_km2 = sum(n_unidades, na.rm = TRUE)/ first(area),
    n_unidades_erm_por_km2 = sum(n_unidades[which(categoria_de_uso_grupo == "ERM")],na.rm = T)/ first(area),
    n_unidades_erp_por_km2 = sum(n_unidades[which(categoria_de_uso_grupo == "ERP")],na.rm = T)/ first(area),
    zoneamento_grupo = fct_count(zoneamento_grupo, sort = TRUE) %>% 
      mutate(f = first(f)) %>%
      filter(n == first(n)) %>% 
      pull(f),
    # zoneamento_grupo = fct_relevel(zoneamento_grupo, ref = "Outros"),
    ca_total = mean(as.numeric(ca_total), na.rm = TRUE)) %>%
  ungroup() 

#
alvaras_por_unidade_pre_pde <- alvaras_por_lote %>%
  filter(between(ano_execucao, 2013, 2021)) %>% # t = {2013, ..., 2021}
  filter(legislacao %in% c("PDE2014eLPUOS2004", "PDE2002eLPUOS2004")) %>%
  st_join(unidade, join = st_within) %>%
  st_drop_geometry() %>%
  group_by(id_unidade) %>%
  summarize(
    n_empreendimentos_por_km2 = n()/ first(area),
    n_empreendimentos_erm_por_km2 = length(categoria_de_uso_grupo[which(categoria_de_uso_grupo == "ERM")])/ first(area),
    n_empreendimentos_erp_por_km2 = length(categoria_de_uso_grupo[which(categoria_de_uso_grupo  == "ERP")])/ first(area),
    n_unidades_por_km2 = sum(n_unidades, na.rm = TRUE)/ first(area),
    n_unidades_erm_por_km2 = sum(n_unidades[which(categoria_de_uso_grupo == "ERM")],na.rm = T)/ first(area),
    n_unidades_erp_por_km2 = sum(n_unidades[which(categoria_de_uso_grupo == "ERP")],na.rm = T)/ first(area),
    zoneamento_grupo = fct_count(zoneamento_grupo, sort = TRUE) %>% 
      mutate(f = first(f)) %>%
      filter(n == first(n)) %>% 
      pull(f),
    # zoneamento_grupo = fct_relevel(zoneamento_grupo, ref = "Outros"),
    ca_total = mean(as.numeric(ca_total), na.rm = TRUE)) %>%
  ungroup() 


# 3. Escassez do Solo -----------------------------------------------------

# β7 -> Escassez do Solo
## Arquivo grande!
## soma do solo disponível e do solo bruto por área de ponderação em 2020
## agregação espacial com parâmetro flexível
escassez_solo_por_unidade <- st_read(here("inputs", "1_inbound", "Abrainc", "EscassezSolo", 
                                          "abrainc_insper_analise_lotes_sirgas.shp"), 
                                     query = "SELECT solo_disp FROM abrainc_insper_analise_lotes_sirgas") %>%
  st_transform(crs = 4674) %>%
  filter(st_is_valid(.) == TRUE) %>%
  st_join(unidade %>%
            dplyr::select(id_unidade, area), join = st_within) %>%
  st_drop_geometry() %>%
  group_by(id_unidade) %>%
  summarize(
    percent_solo_disponivel = (sum(solo_disp, na.rm = TRUE) / 1000000) / first(area),
    lotes_disponiveis_por_km2 = sum(if_else(solo_disp > 0, 1, 0), na.rm = TRUE) / first(area),
    area_lote_disponivel = sum(solo_disp, na.rm = TRUE)/sum(if_else(solo_disp > 0, 1, 0), na.rm = TRUE))


# 4. Empregos -------------------------------------------------------------

# β3 -> Empregos
empregos_por_unidade <- st_read(here("inputs", "1_inbound", "Insper", "Orbis", 
                                     "CleanedData", "OrbisSPL_Geocoded_Clean_Class_shp",
                                     "OrbisSPL_Geocoded_Clean_Class.shp"),
                                query = "SELECT NEmplys FROM OrbisSPL_Geocoded_Clean_Class") %>%
  st_transform(crs = 4674) %>% # SIRGAS 2000
  st_join(unidade, join = st_within) %>%
  st_drop_geometry() %>%
  group_by(id_unidade) %>%
  summarize(n_empregos_por_km2 = sum(NEmplys, na.rm = TRUE) / first(area))


# 5. Densidade ------------------------------------------------------------

# β1 -> DensidadePop
## Média área/setor em 2020
# densidade_pop <- raster(here("inputs", "1_inbound", "Insper", "DensidadePop",
#                              "DensidadePopRaster2020.tif"))

#
# densidade_pop_por_unidade <- unidade %>%
#   transmute(id_unidade,
#             densidade_pop = raster::extract(densidade_pop %>%
#                                               projectRaster(crs = 4674),
#                                             unidade, fun = mean, na.rm = TRUE) %>% 
#               as.numeric(.)) %>%
#   st_drop_geometry()


#
censo <- st_read(here("inputs", "1_inbound", "Insper", 
                      "Censo", "DadosTratados", "Censo2010",
                      "SetorCenso2010.shp")) %>%
  st_transform(crs = st_crs(unidade)) %>%
  transmute(
    Pop,
    densidade_pop = Pop_Dns,
    perc_com_esgoto = DPPEsgR/DPP,
    pop_domicilio = Pop_Dmc,
    renda_media = RndMdRs,
    renda_per_capita = renda_media/pop_domicilio) %>%
  st_join(unidade,
          join = st_within)


#
densidade_pop_por_unidade <- unidade %>%
  transmute(id_unidade,
            area) %>%
  left_join(st_drop_geometry(censo), by = "id_unidade") %>%
  st_drop_geometry() %>%
  group_by(id_unidade) %>%
  summarize(
    densidade_pop = mean(densidade_pop),
  )


## Projection NA --> install.packages("rgdal")

# 6. Comercios -----------------------------------------------------------

# β2 -> Amenidades
## Soma na aŕea/setor em 2020
comercios_por_unidade <- st_read(here("inputs", "1_inbound", "Insper", "FourSquare", 
                                      "CleanedData", "AmenitiesSPL_Geocoded_Clean_Class_shp",
                                      "AmenitiesSPL_Geocoded_Clean_Class.shp")) %>%
  st_transform(crs = 4674) %>% # SIRGAS 2000
  transmute(n_comercios = 1) %>%
  st_join(unidade, join = st_within) %>%
  st_drop_geometry() %>%
  group_by(id_unidade) %>%
  summarize(n_comercios_por_km2 = sum(n_comercios, na.rm = TRUE) / first(area))

# 7. Zoneamento -----------------------------------------------------------

# β4 -> ClassesZoneamento
# Já inclusa no conjunto de dados alvaras
## classe predominante na área de ponderação (moda)

# 8. Acessibilidade -------------------------------------------------------

# β5 -> AcessibilidadeEmpregos
# Centroide mais próximo em 2019
acessibilidade_empregos <-  st_read(here("inputs", "1_inbound", "Insper",
                                         "AcessibilidadeEmpregos", "CleanedData", "AcessibilidadeEmpregos2019_shp",
                                         "AcessibilidadeEmpregos2019.shp")) %>%
  st_transform(crs = st_crs(unidade)) %>%
  transmute(acessibilidade_empregos = accssbl)

##
acessibilidade_empregos_por_unidade <- st_join(unidade, acessibilidade_empregos, 
                                               join = st_nearest_feature) %>%
  st_drop_geometry() %>%
  dplyr::select(id_unidade, acessibilidade_empregos)

# 9. Distância CBD --------------------------------------------------------

# β6 -> DistCBD
# Já inclusa no conjunto de dados alvaras
## distância do centroide da área de ponderação ao CBD

# 10. Vulnerabilidade ------------------------------------------------------------

# β8 -> Vulnerabilidade
## Renda + Percentual com Esgoto
## Censo/IPVS/Eletricidade/Facebook index/ Assentamentos precários

#
censo_por_unidade <- unidade %>%
  transmute(id_unidade,
            area) %>%
  left_join(st_drop_geometry(censo), by = "id_unidade") %>%
  st_drop_geometry() %>%
  group_by(id_unidade) %>%
  summarize(renda_per_capita = mean(renda_per_capita, na.rm = TRUE),
            perc_com_esgoto = mean(perc_com_esgoto, na.rm = TRUE))

# 11. Equipamentos ---------------------------------------------------------

# β9 -> Equipamentos
## Base do GeoSampa dá várias shapefiles 
# Necessário tratamento para serem integradas em um só conjunto
## Uso do repositório IPEA como paliativo
equipamentos <- bind_rows(
  # Educação
  geobr::read_schools() %>%
    filter(name_muni == "São Paulo") %>%
    transmute(n_equipamentos = 1), 
  # Saúde
  geobr::read_health_facilities() %>%
    filter(code_muni == 3550308) %>%
    transmute(n_equipamentos = 1)
)

# Soma os equipamentos na área de ponderação
equipamentos_por_unidade <- st_join(equipamentos, unidade, join = st_within) %>%
  st_drop_geometry() %>%
  group_by(id_unidade) %>%
  summarize(n_equipamentos_por_km2 = sum(n_equipamentos, na.rm = TRUE) / first(area))

# 12. Estrutura conjunto de dados -----------------------------------------


# Informações socioeconomicas por área de ponderação em São Paulo que serão agregadas
## Objeto socioeconomico é construído ao longo da seção 2.
## poderia ser output de outro script, que trata em separado informações complementares sobre SP
socioeconomico <- unidade %>%
  st_drop_geometry() %>%
  left_join(densidade_pop_por_unidade, by = "id_unidade") %>%
  left_join(comercios_por_unidade, by = "id_unidade") %>%
  left_join(empregos_por_unidade, by = "id_unidade") %>%
  left_join(acessibilidade_empregos_por_unidade, by = "id_unidade") %>%
  left_join(escassez_solo_por_unidade, by = "id_unidade") %>%
  left_join(equipamentos_por_unidade, by = "id_unidade") %>%
  left_join(censo_por_unidade, by = "id_unidade")


#
fatores_por_unidade_pre_pde <- alvaras_por_unidade_pre_pde %>%
  st_drop_geometry() %>%
  left_join(socioeconomico, by = "id_unidade") %>%
  filter(!duplicated(.))

#
fatores_por_unidade_pos_pde <- alvaras_por_unidade_pos_pde %>%
  st_drop_geometry() %>%
  left_join(socioeconomico, by = "id_unidade") %>%
  filter(!duplicated(.))

# 13. Exportação ---------------------------------------------------------

#
arrow::write_parquet(fatores_por_unidade_pre_pde,
                          here("inputs", "2_trusted", "Fatores", 
                               "fatores_por_area_ponderacao_pre_pde.parquet"))

#
arrow::write_parquet(fatores_por_unidade_pos_pde,
                          here("inputs", "2_trusted", "Fatores", 
                               "fatores_por_area_ponderacao_pos_pde.parquet"))
#
beep(8)
