# Pacotes
library(here)
library(fs)
library(beepr)
library(tidyverse)
library(sf)
library(ggtext)
library(readxl)

# Funções
source(here("scripts", "functions.R"))

# Opções
options(error = beep)

# 1. Importa -------------------------------------------------------------

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
get_shapefiles(
  files = c("Macroarea", "PIU", "AIU", "MEMSetores")
)

#
zoneamento <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao", 
       "zoneamento.parquet"))

# 2. Estrutura ------------------------------------------------------------


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

# 3. Integra dados MEM  --------------------------------------------------------

#
mem <- macroareas_ajust %>%
  filter(macroarea == "MEM")

#
mem_por_zona_quadra <- zoneamento %>%
  filter(macroarea == "MEM")

#
mem_por_piu <- pius_ajust

#
mem_por_setor <- mem_setores_ajust

# 3. Exporta para datalake -----------------------------------------------------

#
fs::dir_create(here("inputs", "2_trusted", "Legislacao"))

#
sfarrow::st_write_parquet(mem,
                          here("inputs", "2_trusted", "Legislacao",
                               "mem.parquet"))
#
sfarrow::st_write_parquet(mem_por_zona_quadra,
                          here("inputs", "2_trusted", "Legislacao",
                               "mem_por_zona_quadra.parquet"))

sfarrow::st_write_parquet(mem_por_setor,
                          here("inputs", "2_trusted", "Legislacao",
                               "mem_por_setor.parquet"))

sfarrow::st_write_parquet(mem_por_piu,
                          here("inputs", "2_trusted", "Legislacao",
                               "mem_por_piu.parquet"))
