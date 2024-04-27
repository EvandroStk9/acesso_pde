# Pacotes
library(here)
library(fs)
library(beepr)
library(sf)
library(tidyverse)
library(tidylog)
library(sfarrow)

# Funções
source(here("scripts", "functions.R"))

# Opções
options(error = beep)

# 1. Importa -------------------------------------------------------------------


#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
get_shapefiles(
  files = c("Quadras", "Macroarea", "Operacao", "Distrito", "PIU",
            "AIU", "MEMSetores")
)

# Zoneamento
zoneamento <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao", 
       "zoneamento.parquet")) %>%
  select(zoneamento, zoneamento_grupo, solo_uso, ca_maximo)


# 2. Estrutura -----------------------------------------------------------------

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

# 3. Integra dados  ------------------------------------------------------------

#
quadras_ajust <- quadras %>%
  mutate(id_quadra = row_number(),
         qd_area = as.numeric(qd_area)) %>%
  st_join(macroareas_ajust, join = st_intersects, largest = TRUE) %>%
  st_join(operacao_urbana_ajust, join = st_intersects, largest = TRUE) %>%
  st_join(distritos_ajust, join = st_intersects, largest = TRUE) %>%
  st_join(mem_setores_ajust, largest = TRUE) %>%
  st_join(pius_ajust, largest = TRUE) %>%
  st_join(aiu_perimetro_ajust) %>%
  st_join(zoneamento, largest = TRUE)

# 4. Exporta para datalake  ----------------------------------------------------

#
fs::dir_create(here("inputs", "2_trusted", "Legislacao"))

#
sfarrow::st_write_parquet(quadras_ajust,
                          here("inputs", "2_trusted", "Legislacao", 
                               "quadras.parquet"))

