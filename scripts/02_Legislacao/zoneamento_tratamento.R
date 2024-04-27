# Pacotes
library(here)
library(fs)
library(beepr)
library(sf)
library(tidyverse)
library(tidylog)
library(readxl)
library(fs)

# Funções
source(here("scripts", "functions.R"))

# 1. Importa ------------------------------------------------------------


#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
get_shapefiles(
  files = c("ZonasLeiZoneamento", "Macroarea", "Operacao", "Distrito", "PIU",
            "AIU", "MEMSetores")
)

# Acessibilidade
# Origem: acessibilidade_tratamento.R
acessibilidade_empregos <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Acessibilidade",
       "acessibilidade_empregos.parquet")
)

# 2. Estrutura ------------------------------------------------------------

# Tipologia de agrupamento dos zoneamentos
# Análise qualitativa e aperfeiçoamento de tipologia pré desenvolvida 
zoneamento_tipologia <- read_excel(here("inputs", 
                          "Complementares", "Zoneamento", 
                          "ZonasLeiZoneamento", 
                          "Class_Join.xls"))

# Zoneamento 2016
zoneamento <- zonas_lei_zoneamento %>%
  select(ID, Zone) %>%
  filter(!is.na(Zone)) %>%
  st_transform(crs = st_crs(CRS)) %>%
  st_make_valid(.) %>%
  filter(st_is_valid(.) == TRUE) %>%
  left_join(zoneamento_tipologia, by = "Zone") %>%
  transmute(
    id_zoneamento = ID,
    zoneamento = as.factor(Zone),
    zoneamento_grupo = 
      fct_relevel(ZoneGroup, 
                  c("ZCs&ZMs", "EETU", "EETU Futuros", 
                    "ZEIS Aglomerado", "ZEIS Vazio",  
                    "ZPI&DE", "ZE&P", "ZE&PR", "ZCOR")) %>%
      fct_other(drop = c("ZPI&DE", "ZE&P", "ZE&PR", "ZCOR"), 
                other_level = "Outros"),
    solo_uso = as.factor(case_when(Use == "mixed use" ~ "misto",
                                   Use == "unique use" ~ "único",
                                   TRUE ~ NA_character_)),
    ca_maximo = as.factor(FARmax),
    area_zoneamento = unclass(st_area(.)))

# Distritos
distritos_ajust <- distritos %>%
  transmute(distrito = ds_nome)

# Macroareas
macroareas_ajust <- macroareas %>%
  transmute(macroarea = as_factor(mc_sigla),
            area_macroarea = mc_pde_m2)

# Setores da MEM
mem_setores_ajust <- mem_setores %>%
  transmute(
    mem_setor = as_factor(str_to_upper(nm_perimet)),
    mem_setor_grupo = case_when(
      mem_setor == "CENTRO" ~ "CENTRO",
      mem_setor %in% c("ARCO JACU-PESSEGO", "AVENIDA CUPECE", 
                       "NOROESTE", "FERNAO DIAS") ~ "EIXO",
      mem_setor %in% c("ARCO LESTE", "ARCO TIETE", "ARCO JURUBATUBA", 
                       "ARCO TAMANDUATEI", "ARCO PINHEIROS", 
                       "FARIA LIMA-AGUA ESPRAIADA-CHUCRI ZAIDAN") ~ "ORLA")
  ) %>%
  st_make_valid()

# Operações Urbanas
operacao_urbana_ajust <- operacao_urbana %>%
  transmute(operacao_urbana = ou_nome)

# PIUS
pius_ajust <- pius %>%
  ## Problema com double encoding!!!
  mutate(across(where(is.character), 
                ~iconv(.x, to="latin1", from="utf-8") %>%
                  iconv(to="ASCII//TRANSLIT", from="utf-8") %>%
                  str_to_upper(locale = "br"))) %>%
  st_make_valid() %>%
  transmute(piu = Layer %>%
              str_remove("CONCESSAO DOS TERMINAIS - ")) %>%
  st_cast("MULTIPOLYGON") %>%
  st_make_valid() 

# AIU Setor Central
aiu_perimetro_ajust <- aiu_perimetro %>%
  transmute(aiu_perimetro = per_nome)


# 3. Integra dados ------------------------------------------------------------

#
zoneamento_ajust <- zoneamento %>%
  st_join(macroareas_ajust, join = st_intersects, largest = TRUE) %>%
  st_join(operacao_urbana_ajust, join = st_intersects, largest = TRUE) %>%
  st_join(distritos_ajust, join = st_intersects, largest = TRUE) %>%
  st_join(mem_setores_ajust, largest = TRUE) %>%
  st_join(pius_ajust, largest = TRUE) %>%
  st_join(aiu_perimetro_ajust, largest = TRUE) %>%
  st_join(acessibilidade_empregos, join = st_nearest_feature)

# 4. Aplica regras de negócio -------------------------------------------------

#
zoneamento_por_zona_grupo_macroarea_no_geo <- zoneamento_ajust %>%
  st_drop_geometry() %>%
  # Calcula área de cada zoneamento em cada macroárea
  group_by(zoneamento_grupo, macroarea) %>%
  summarize(
    area_zoneamento_macroarea = sum(area_zoneamento)) %>%
  # Calcula o percentual de área do zoneamento em cada macroárea
  group_by(zoneamento_grupo) %>%
  mutate(area_percent_macroarea = area_zoneamento_macroarea/sum(area_zoneamento_macroarea)) %>%
  # Adiciona dado para Macroáreas urbanas
  left_join(
    zoneamento_ajust %>%
      st_drop_geometry() %>%
      # Calcula área de cada zoneamento em cada macroárea
      group_by(zoneamento_grupo, macroarea) %>%
      summarize(
        area_zoneamento_macroarea = sum(area_zoneamento)) %>%
      # Calcula o percentual de área do zoneamento em cada macroárea
      group_by(zoneamento_grupo) %>%
      mutate(area_percent_macroarea = area_zoneamento_macroarea/sum(area_zoneamento_macroarea)) %>%
      filter(macroarea %in% c("MEM", "MUC", "MQU", "MRVU")) %>%
      summarize(area_zoneamento_macroarea_urbana = sum(area_zoneamento_macroarea, 
                                                       na.rm = TRUE))
  )

#
zoneamento_por_zona_grupo_no_geo <- zoneamento_por_zona_grupo_macroarea_no_geo %>%
  group_by(zoneamento_grupo) %>%
  summarize(area_zoneamento_grupo = sum(area_zoneamento_macroarea)) %>%
  # Adiciona dado para agregação de Macroáreas urbanas
  left_join(zoneamento_por_zona_grupo_macroarea_no_geo %>%
              filter(macroarea %in% c("MEM", "MUC", "MQU", "MRVU")) %>%
              summarize(area_zoneamento_macroarea_urbana = sum(area_zoneamento_macroarea, 
                                                               na.rm = TRUE))) %>%
  # Adiciona dado em formato painel para Macroáreas urbanas
  left_join(zoneamento_por_zona_grupo_macroarea_no_geo %>%
              filter(macroarea %in% c("MEM", "MUC", "MQU", "MRVU")) %>%
              pivot_wider(names_from = macroarea, values_from = -c(macroarea, zoneamento_grupo))) %>%
  ungroup() %>%
  mutate(
    area_percent_zoneamento_macroarea_urbana = area_zoneamento_macroarea_urbana/area_zoneamento_grupo) %>%
  select(
    zoneamento_grupo, area_zoneamento_grupo, area_percent_zoneamento_macroarea_urbana,
    starts_with("area_percent")) %>%
  mutate(area_zoneamento_grupo = area_zoneamento_grupo/1000000)

# 5. Exporta para datalake -----------------------------------------------------

#
dir_create(here("inputs", "2_trusted", "Legislacao"))

#
sfarrow::st_write_parquet(zoneamento_ajust,
                          here("inputs", "2_trusted", "Legislacao", 
                               "zoneamento.parquet"))

#
arrow::write_parquet(zoneamento_por_zona_grupo_no_geo,
                     here("inputs", "2_trusted", "Legislacao", 
                          "zoneamento_por_zona_grupo_no_geo.parquet"))

