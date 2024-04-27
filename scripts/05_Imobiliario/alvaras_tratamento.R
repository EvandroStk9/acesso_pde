# 0. Setup ----------------------------------------------------------------

# Pacotes
library(here)
library(sf)
library(sfarrow)
library(arrow)
library(tidyverse)
library(tidylog)
library(beepr)

# library(readxl)
# library(lubridate)
# library(stringr)

# Opções
options(
  error = beep,
  scipen = 9999
)

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

# 1. Importa -------------------------------------------------------------------

# A) Licenciamentos imobiliaŕios/ Novas Edificações

# Dado original, desagregado e sem georreferenciamento
# Necessário para atribuição da legislação
# Agregação: alvaras.parquet -> alvara_por_lote.parquet
# Processo está organizado em OneDrive/AlvarasPMSPSMUL e será publicizado
alvaras <- arrow::read_parquet(
  here("inputs", 
       "1_inbound", "Insper",
       "Licenciamentos", "dados",
       "alvaras.parquet"))

# Dado original, agregado e georreferenciado feito em separado pela equipe
# Tratamento dos dados públicos, construindo série histórica e qualificando
# Processo está organizado em OneDrive/AlvarasPMSPSMUL e será publicizado
alvaras_por_lote <- sfarrow::st_read_parquet(
  here("inputs",
       "1_inbound", "Insper",
       "Licenciamentos", "dados",
       "geo_alvaras_por_lote.parquet")) %>%
  st_transform(crs = 4674)

# A - Geo
## Praça da Sé como CBD
cbd <- data.frame(long = -46.633959, lat= -23.550382) %>%
  st_as_sf(coords = c("long", "lat"),
           crs = 4674)

# B - Legislação

# Classificação da legislação
## SISACOE - Classificação da prefeitura + classificação Equipe Insper
# Script alvaras_tratamento_legislacao.R
alvaras_por_lote_legislacao <- arrow::read_parquet(
  here("inputs", "2_trusted", "Licenciamentos",
       "alvaras_por_lote_legislacao_no_geo.parquet")) %>%
  select(id, legislacao, legislacao_origem)

#
zoneamento <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao",
       "zoneamento.parquet")) %>%
  st_transform(crs = 4674)

# C - Indicadores
# acessibilidade_empregos <-  st_read(here("inputs", "Complementares",
#                                          "AcessibilidadeEmpregos", "CleanedData",
#                                          "AcessibilidadeEmpregos2019",
#                                          "AcessibilidadeEmpregos2019.shp")) %>%
#   st_transform(crs = CRS) %>%
#   transmute(acessibilidade_empregos = accssbl)

# Acessibilidade
# Origem: acessibilidade_tratamento.R
acessibilidade_empregos <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Acessibilidade",
       "acessibilidade_empregos.parquet")
) %>%
  st_transform(crs = 4674)

# 2. Seleciona ------------------------------------------------------------

# Selecionando variáveis de interesse
alvaras_por_lote_pre <- alvaras_por_lote %>%
  # filter(ind_aprovacao == 1 & ind_execucao == 1) %>%
  filter(categoria_de_uso_grupo != "Outra") %>% # Somente usos residenciais!
  filter(ano_execucao < 2022) %>%
  transmute(id, 
            ano_execucao,
            #diff_dias_aprovacao,
            categoria_de_uso_grupo,
            categoria_de_uso_registro,
            area_do_terreno,
            area_da_construcao,
            n_blocos,
            n_pavimentos,
            n_unidades,
            n_pavimentos_por_bloco,
            n_unidades_por_bloco,
            sql_incra,
            endereco_ultimo,
            bairro,
            zona_de_uso_registro,
            zona_de_uso_anterior_registro = zona_de_uso_anterior,
            ind_uso_misto,
            # ind_r2v,
            # ind_r2h,
            ind_his,
            ind_hmp,
            ind_ezeis,
            # ind_zeis
  ) %>%
  left_join(alvaras_por_lote_legislacao, by = "id")

#
glimpse(alvaras_por_lote_pre)

# 3. Integra dados ----------------------------------------------------

#
alvaras_por_lote_tidy_com_outliers <- alvaras_por_lote_pre %>%
  st_join(zoneamento %>%
            st_make_valid(), 
          join = st_nearest_feature) %>%
  # Criando variáveis para análise exploratória que dependem de informações GIS
  mutate(ca_total = area_da_construcao/area_do_terreno,
         area_da_unidade = area_da_construcao/n_unidades,
         cota_parte = area_do_terreno / n_unidades,
         distancia_cbd = round(as.numeric(st_distance(., cbd)) / 1000, 1),
         ind_miolo = case_when(macroarea != "MEM" & 
                                 zoneamento_grupo != "EETU" ~ TRUE, 
                               TRUE ~ FALSE),
         # ind_eetu = if_else(row_number() %in% ind_eetu$id_row, TRUE, FALSE)
  ) %>% 
  # Agregando por geometria indicador sintético por grid de 5km
  # st_join(acessibilidade_empregos, join = st_nearest_feature) %>%
  # mutate(
  #   # acessibilidade_empregos = rescale(acessibilidade_empregos, to = c(0, 1)),
  #   fx_acessibilidade_empregos = case_when(
  #     acessibilidade_empregos <= quantile(acessibilidade_empregos, 0.3) ~ "Muito baixa",
  #     between(acessibilidade_empregos, quantile(acessibilidade_empregos, 0.301),
  #             quantile(acessibilidade_empregos, 0.5)) ~ "Baixa",
  #     between(acessibilidade_empregos, quantile(acessibilidade_empregos, 0.501),
  #             quantile(acessibilidade_empregos, 0.7)) ~ "Média",
  #     between(acessibilidade_empregos, quantile(acessibilidade_empregos, 0.701),
#             quantile(acessibilidade_empregos, 0.9)) ~ "Alta",
#     acessibilidade_empregos > quantile(acessibilidade_empregos, 0.9) ~ "Muito alta"
#   ) %>%
#     fct_reorder(acessibilidade_empregos)) %>%
select(everything(), geometry)

# 4. Trata outliers -------------------------------------------------------

# Função para ajustes de outliers com pequena correção p/ tratamento de NA's
scores_ajust <- function(x, ...) {
  not_na <- !is.na(x)
  scores <- rep(NA, length(x))
  scores[not_na] <- outliers::scores(na.omit(x), ...)
  scores
}

# Identifica outliers com teste Qui-quadrado + critério técnico
ind_outliers <- alvaras_por_lote_tidy_com_outliers %>%
  mutate(
    ind_outlier_area_unidade = scores_ajust(area_da_unidade,
                                            type = "chisq", prob = 0.9999),
    # ind_outlier_cota_parte = scores_ajust(cota_parte,
    #                                           type = "chisq", prob = 0.9999),
    ind_outlier_ca_total = scores_ajust(ca_total,
                                        type = "chisq", prob = 0.9999),
    ind_outlier_area_unidade_menos_15 = if_else(area_da_unidade < 15, 
                                                TRUE, FALSE)) %>%
  mutate(ind_outlier = case_when(
    area_da_unidade < 15 ~ TRUE, # Área precisa ser maior que 15 m²
    ind_outlier_area_unidade == TRUE | 
      # ind_outlier_cota_parte == TRUE |
      ca_total < 0.1 |
      ind_outlier_ca_total == TRUE ~ TRUE,
    TRUE ~ FALSE)) %>%
  # Atribui NA dos testes qui quadrado como FALSE/não-outlier
  mutate(across(starts_with("ind_outlier"),
                ~if_else(is.na(.x), FALSE, .x))) %>%
  relocate(starts_with("ind_"), .before = geometry)


# Outliers são 0.9% do total de empreendimentos e 18% do total de unidades
# 6.3% da área do terreno e 4.9% da área de construção
# Outliers eram 1.2% do total de empreendimentos e 18.1% do total de unidades
# 7% da área do terreno e 5% da área de construção
st_drop_geometry(ind_outliers) %>%
  mutate(ind_outlier = case_when(ind_outlier == TRUE ~ "outlier",
                                 ind_outlier == FALSE ~ "normal")) %>%
  group_by(ind_outlier) %>%
  summarize(n_empreendimentos = n(),
            n_unidades_total = sum(n_unidades, na.rm = TRUE),
            area_do_terreno_total = sum(area_do_terreno, na.rm = TRUE),
            area_da_construcao_total = sum(area_da_construcao, na.rm = TRUE)) %>%
  pivot_longer(-ind_outlier) %>%
  pivot_wider(names_from = ind_outlier, values_from = value) %>%
  mutate(prop = (outlier/(normal + outlier) * 100))

# Médias
st_drop_geometry(ind_outliers) %>%
  mutate(ind_outlier = case_when(ind_outlier == TRUE ~ "outlier",
                                 ind_outlier == FALSE ~ "normal")) %>%
  group_by(ind_outlier) %>%
  summarize(across(c(n_unidades, cota_parte, ca_total,
                     area_do_terreno, area_da_construcao,
                     n_pavimentos_por_bloco, distancia_cbd),
                   ~ mean(.x, na.rm = TRUE))) %>%
  pivot_longer(-ind_outlier) %>%
  pivot_wider(names_from = ind_outlier, values_from = value)

#

# Impacto de unidades é muito mais significativo na MEM do que na MQU e na MUC
left_join(
  ind_outliers %>%
    st_drop_geometry() %>%
    count(macroarea, wt = n_unidades),
  ind_outliers %>%
    st_drop_geometry() %>%
    filter(ind_outlier == TRUE) %>%
    count(macroarea, wt = n_unidades),
  by = "macroarea", suffix = c("_geral", "_outliers")) %>%
  mutate(prop = n_outliers/n_geral*100) 

# Anula número de unidades dos outliers
alvaras_por_lote_tidy <- ind_outliers %>%
  filter(ind_outlier == FALSE) %>%
  select(-starts_with("ind_outlier")) %>%
  # Após análise dos resultados observou-se que existem 3 grandes empreendimentos em 2016 outliers
  # A área de terreno não vem acompanhada de área de construção e contém outros erros
  # 2 empreendimentos da Caixa no Grajaú se referem a um único empreendimento com SQL's distintos
  # 1 terreno rural em São Mateus teve georreferenciamento equivocado como "EETU"
  # Solução parcial: remover empreendimento de São Mateus e 1 dos empreendimentos da Caixa
  filter(!sql_incra %in% c("9501656876427", "174.312.0016-0"))

# 5. Exporta para datalake -----------------------------------------------------

# Dados tratados em formato .parquet
sfarrow::st_write_parquet(alvaras_por_lote_tidy, 
                          here("inputs", "2_trusted", "Licenciamentos",
                               "alvaras_por_lote.parquet"))

