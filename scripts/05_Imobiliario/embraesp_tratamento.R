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
library(deflateBR)
library(sfarrow)
library(ggthemes)
library(Hmisc)

# Funções
source(here("scripts", "functions.R"))

# Opções
options(scipen = 99999,
        error = beep)

# 1. Importa -------------------------------------------------------------------

# Sistema de coordenadas Geográficas de referência SIRGAS 2000
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
embraesp_raw <- st_read(here("inputs", "1_inbound",
                             "Abrainc", "Embraesp", "Individualizados",
                             "2022_04_27_embraesp_lancamentos.shp")) %>%
  #st_transform(crs = 4674) %>%
  st_transform(crs = CRS) %>%
  mutate(key = as.character(geometry), # Chave para agrupar empreendimentos por geoloc
         endereco = str_to_upper(endereco)) %>%
  group_by(key) %>%
  mutate(geometria_duplicada = if_else(n_distinct(endereco) >= 2, TRUE, FALSE)
         # Alguns geolocs possuem mais de 1 endereço --> 20 empreendimentos e 116 lançamentos
  ) %>%
  group_by(key, endereco) %>%
  mutate(id_empreendimento = cur_group_id()) %>%
  ungroup() %>%
  select(-key) %>%
  select(id_empreendimento, id, geometria_duplicada, everything())

# Importa tipologia de zoneamento e agrega informações por empreendimento
# ZonasLeiZoneamento <- st_read(here("dados", "Complementares", "Zoneamento", 
#                                    "ZonasLeiZoneamento", "ZoningSPL_Clean_Class.shp")) %>%
#   select(Zone) %>%
#   st_transform(crs = st_crs(embraesp_raw)) %>%
#   # st_make_valid(.) %>%
#   filter(st_is_valid(.) == TRUE) %>%
#   filter(!is.na(Zone)) %>%
#   left_join(read_excel(here("dados", "Complementares", "Zoneamento", 
#                             "ZonasLeiZoneamento", "Class_Join.xls")), # tipologia nossa
#             by = "Zone") %>%
#   transmute(
#     zoneamento_ajust = as.factor(Zone),
#     zoneamento_grupo = 
#       fct_relevel(ZoneGroup, 
#                   c("ZCs&ZMs", "EETU", "EETU Futuros", 
#                     "ZEIS Aglomerado", "ZEIS Vazio",  
#                     "ZPI&DE", "ZE&P", "ZE&PR", "ZCOR")) %>%
#       fct_other(drop = c("ZPI&DE", "ZE&P", "ZE&PR", "ZCOR"), 
#                 other_level = "Outros"),
#     solo_uso = as.factor(case_when(Use == "mixed use" ~ "misto",
#                                    Use == "unique use" ~ "único",
#                                    TRUE ~ NA_character_)),
#     ca_maximo = as.factor(FARmax))

# # Importando informações sobre o perímetro de São Paulo
# subprefeituras <- st_read(
#   here("dados", "Complementares", "Subprefeituras", "SubprefectureMSP_SIRGAS.shp"),
#   crs = CRS) %>%
#   st_transform(st_crs(embraesp_raw)) %>%
#   transmute(id_regional = sp_id,
#             regional = sp_nome)
# 
# # Importando informações sobre o perímetro de São Paulo
# distritos <- st_read(
#   here("dados", "Complementares", "Distritos", "DistrictsMSP_SIRGAS.shp")) %>%
#   st_transform(st_crs(embraesp_raw)) %>%
#   transmute(id_distrito = ds_codigo,
#             distrito = ds_nome)
# 
# #
# Macroareas <- st_read(here("dados", "Complementares", "Macroareas",
#                            "SIRGAS_SHP_MACROAREAS.shp")) %>%
#   transmute(macroarea = as_factor(mc_sigla)) %>%
#   st_transform(crs = st_crs(embraesp_raw))
# 
# 
# eixos <- st_read(here("dados", "Eixos", "ArquivosTratados",
#                       "Eixos.shp")) %>%
#   set_names(as_vector(read_csv(here("dados", "Eixos", "ArquivosTratados",
#                                     "EixosLabels.csv"), show_col_types = FALSE))) %>%
#   select(id_Eixo, NomeTransp, TipoTransp, ) %>%
#   st_transform(4674)

#
get_shapefiles(
  files = "Distrito"
)

#
zoneamento <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao", 
       "zoneamento.parquet"))

#
zonas_eetu <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos", "zonas_eetu.parquet")
)

# 2. Cria dado desagregado -----------------------------------------------------

# Chave relacional com id e geometria
embraesp_key <- embraesp_raw %>%
  select(id, geometry)

# Abstrai geometria e aplica deflator
embraesp_no_geometry <- embraesp_raw %>%
  st_drop_geometry() %>%
  mutate(preco_deflacionado = deflate(nominal_values = preco_unid,
                                      nominal_dates = data_lanca,
                                      real_date = "12/2022",
                                      index = "igpm"))

# Adiciona informações de zoneamento ao dado desagregado
embraesp <- embraesp_key %>%
  inner_join(embraesp_no_geometry, by = "id") %>%
  mutate(ano = year(data_lanca),
         fx_area_util = case_when(area_util < 35 ~ "Menos que 35m²",
                                  area_util >= 35 ~ "Mais que 35m²")) %>%
  st_join(zoneamento, join = st_nearest_feature) %>%
  relocate(geometry, .after = everything()) # reposiciona


# 3. Cria dado agregado --------------------------------------------------------


# Abstrai geometria e estima variáveis por empreendimento
embraesp_por_empreendimento_no_geometry <- embraesp_no_geometry %>%
  filter(origem == "RESIDENCIAL") %>% # Tomando somente empreendimentos residenciais
  group_by(id_empreendimento) %>%
  dplyr::summarise(
    empreendimento = first(nome_empre),
    endereco = first(endereco),
    agente = first(na.omit(agente)),
    agente_ajust = fct_rev(if_else(str_detect(agente, "CEF"), "CEF", "SFH/SBPE")),
    data_lancamento = min(data_lanca),
    data_lancamento_ult = max(data_lanca),
    ano = year(data_lancamento_ult), # ano = f(ultima data de lancamento)
    n_unidades = sum(num_total_),
    n_unidades_area_comp_menos_30m2 = sum(num_total_[which(0.95*area_util < 30)]),
    n_unidades_area_util_menos_35m2 = sum(num_total_[which(area_util < 35)]),
    # n_unidades_media = mean(num_total_),
    vagas_area_comp_menos_30m2 = sum(vagas[which(0.95*area_util < 30)]*num_total_[which(0.95*area_util < 30)]),
    vagas_area_util_menos_35m2 = sum(vagas[which(area_util < 35)]*num_total_[which(area_util < 35)]),
    vagas = sum(vagas*num_total_),
    area_terreno = sum(area_terre),
    area_util_dp = sqrt(wtd.var(area_util, num_total_)),
    area_util_media = weighted.mean(area_util, num_total_),
    area_util_acima_35m2 = sum(area_util[which(area_util < 35)]*num_total_[which(area_util < 35)]),
    area_comp_dp = sqrt(wtd.var(0.95*area_util, num_total_)),
    area_comp_media = weighted.mean(0.95*area_util, num_total_),
    area_comp_acima_30m2 = sum(0.95*area_util[which(0.95*area_util < 30)]*num_total_[which(0.95*area_util < 30)]),
    area_comp = sum(0.95*area_util*num_total_),
    area_util = sum(area_util*num_total_),
    area_total = sum(area_total*num_total_),
    cota_parte = area_terreno/n_unidades,
    vgv = sum(num_total_*preco_unid), 
    preco_m2 = vgv/area_util,
    vgv_deflacionado = sum(num_total_*preco_deflacionado),
    preco_m2_deflacionado = vgv_deflacionado/area_util,
    porte = case_when(between(n_unidades, 0, 49) ~ "Até 50 UH's",
                      between(n_unidades, 50, 199) ~ "Entre 50 e 200 UH's",
                      n_unidades >= 200 ~ "Mais que 200 UH's",
                      TRUE ~ "Sem informação"),
    fx_area_terreno = fct_reorder(case_when(between(area_terreno, 0, 4999.999) ~ "0 a 5000 m²",
                                            between(area_terreno, 5000, 9999.999) ~ "5000 a 10000 m²",
                                            between(area_terreno, 10000, 19999.999) ~ "10000 a 20000m²",
                                            area_terreno >= 20000 ~ "Mais que 20000 m2",
                                            TRUE ~ "Sem informação"), area_terreno),
    fx_cota_parte = fct_reorder(case_when(between(cota_parte, 0, 49.999) ~ "0 a 50 m² por UH",
                                          between(cota_parte, 50, 99.999) ~ "50 a 100 m² por UH",
                                          between(cota_parte, 100, 999.999) ~ "100 a 1000 m² por UH",
                                          cota_parte >= 1000 ~ "Mais que 1000 m² por UH",
                                          TRUE ~ "Sem informação"), cota_parte)) %>%
  ungroup()


# Chave relacional com id e geometria por empreendimento
embraesp_por_empreendimento_key <- embraesp_raw %>%
  select(id_empreendimento, geometry) %>%
  filter(!duplicated(id_empreendimento))

#
embraesp_por_empreendimento_com_duplicatas <- embraesp_por_empreendimento_key %>%
  inner_join(embraesp_por_empreendimento_no_geometry, by = "id_empreendimento") %>%
  select(id_empreendimento, empreendimento:fx_cota_parte, everything()) %>%
  st_join(zoneamento, join = st_nearest_feature) %>%
  left_join(st_drop_geometry(zonas_eetu))


# Duplicados | Pode ser ajustado com parâmetro "largest"
# st_drop_geometry(embraesp_por_empreendimento) %>% 
#   filter(duplicated(.)) %>% 
#   unique() %>%
#   view("embraesp_duplicados")

# Exlui duplicados e reposiciona variáveis
embraesp_por_empreendimento <- embraesp_por_empreendimento_key %>%
  inner_join(st_drop_geometry(embraesp_por_empreendimento_com_duplicatas) %>% 
               filter(!duplicated(.)), by = "id_empreendimento") %>%
  relocate(geometry, .after = everything()) # reposiciona


#
embraesp_por_distrito_ano_no_geo <- embraesp_por_empreendimento %>%
  st_drop_geometry() %>%
  mutate(zoneamento_grupo = fct_other(zoneamento_grupo, keep = "EETU", other_level = "Outro")) %>%
  # filter(ano >= 2009) %>%
  group_by(distrito, ano) %>%
  summarise(
    n_empreendimentos_total = n_distinct(id_empreendimento),
    n_empreendimentos_eetu = n_distinct(id_empreendimento[which(zoneamento_grupo == "EETU")], na.rm = TRUE),
    n_unidades_total = sum(n_unidades),
    n_unidades_eetu = sum(n_unidades[which(zoneamento_grupo == "EETU")], na.rm = TRUE),
    n_vagas_total = sum(vagas),
    n_vagas_eetu = sum(vagas[which(zoneamento_grupo == "EETU")]),
    n_unidades_area_util_mais_35m2 = n_unidades_total - 
      sum(n_unidades_area_util_menos_35m2),
    n_unidades_eetu_area_util_mais_35m2 = n_unidades_eetu - 
      sum(n_unidades_area_util_menos_35m2[which(zoneamento_grupo == "EETU")], na.rm = TRUE),
    n_unidades_eetu_area_util_menos_35m2 = sum(n_unidades_area_util_menos_35m2[which(zoneamento_grupo == "EETU")], na.rm = TRUE),
    n_unidades_area_util_menos_35m2 = sum(n_unidades_area_util_menos_35m2)) %>%
  ungroup()

#
embraesp_por_distrito_ano <- distritos %>%
  transmute(distrito = ds_nome) %>%
  left_join(embraesp_por_distrito_ano_no_geo, by = c("distrito")) %>%
  # select(distrito, ano, n_empreendimentos_total, n_empreendimentos_eetu,
  #        n_unidades_total, n_unidades_eetu, n_vagas_total, n_vagas_eetu,
  #        n_unidades_area_util_mais_35m2, n_unidades_eetu_area_util_mais_35m2,
  #        n_unidades_area_util_menos_35m2, n_unidades_eetu_area_util_menos_35m2) %>%
  arrange(distrito, ano)

# 4. Exporta para datalake -----------------------------------------------------

# Por lançamento
sfarrow::st_write_parquet(
  embraesp, 
  here("inputs", "2_trusted", "Lancamentos",
       "embraesp.parquet"))

# Por empreendimento
sfarrow::st_write_parquet(
  embraesp_por_empreendimento, 
  here("inputs", "2_trusted", "Lancamentos",
       "embraesp_por_empreendimento.parquet"))

# Por distrito no ano
sfarrow::st_write_parquet(
  embraesp_por_distrito_ano, 
  here("inputs", "2_trusted", "Lancamentos",
       "embraesp_por_distrito_ano.parquet"))

# 5. Exporta para usuário -----------------------------------------------------


# # Exporta dados agregados em formato shapefile
# st_write(embraesp_por_empreendimento, here("dados", "Lancamentos", "Embraesp", "ArquivosTratados"),
#          layer="embraesp_por_empreendimento", delete_layer = TRUE, driver="ESRI Shapefile")
# 
# # Adiciona labels da Base Tratada
# write_csv(data.frame(labels = names(embraesp_por_empreendimento)), 
#           here("dados", "Lancamentos", "Embraesp", "ArquivosTratados",
#                "embraesp_por_empreendimentoLabels.csv"))
# 

# # Por lançamento
# dir_create(here("outputs", "Lancamentos", "embraesp"))
# 
# #
# st_write(embraesp, 
#          here("outputs", "Lancamentos", "embraesp"),
#          layer="embraesp", delete_layer = TRUE, driver="ESRI Shapefile")
# 
# #
# write_csv(data.frame(labels = names(embraesp)), 
#           here("outputs", "Lancamentos", "embraesp_por_empreendimento",
#                "embraesp_labels.csv"))
# 
# # Demanda disciplina jornalismo de dados
# writexl::write_xlsx(embraesp %>%
#                       st_drop_geometry() %>%
#                       select(-geometria_duplicada), 
#          here("outputs", "Lancamentos", "embraesp.xlsx"))
# 
# 
# # Por empreendimento
# dir_create(here("outputs", "Lancamentos", "embraesp_por_empreendimento"))
# 
# #
# st_write(embraesp_por_empreendimento, 
#          here("outputs", "Lancamentos", "embraesp_por_empreendimento"),
#          layer="embraesp_por_empreendimento", delete_layer = TRUE, driver="ESRI Shapefile")
# 
# #
# write_csv(data.frame(labels = names(embraesp_por_empreendimento)), 
#           here("outputs", "Lancamentos", "embraesp_por_empreendimento",
#                "embraesp_por_empreendimento_labels.csv"))
