library(here)
library(sf)
library(sfarrow)
library(tidyverse)
library(lubridate)
library(readxl)
library(tidylog)
library(patchwork)
library(ggthemes)
library(ggtext)
library(ggrepel)

#
embraesp_por_empreendimento <- sfarrow::st_read_parquet(here(
  # "scripts", "09_NotasTecnicas", "VagasGaragemSP",
  # "dados", "Lancamentos", 
  # "Embraesp", "ArquivosTratados", 
  "inputs", "2_trusted", "Lancamentos",
  "embraesp_por_empreendimento.parquet")) %>%
  mutate(zoneamento_grupo = fct_relevel(zoneamento_grupo,
                                        c("ZCs&ZMs", "EETU", "ZEIS-Aglomerado", 
                                          "ZEIS-Vazio", "Outros")),
         fx_area_terreno = fct_reorder(fx_area_terreno, area_terreno),
         fx_cota_parte = fct_reorder(fx_cota_parte, cota_parte),
         fx_area_util = case_when(
           area_util/n_unidades < 35 ~ "Menos de 35m²",
           area_util/n_unidades >= 35 ~ "Mais de 35m²") %>%
           fct_rev(),
         fx_area_comp = case_when(
           area_comp/n_unidades < 30 ~ "Menos de 30m²",
           area_comp/n_unidades >= 30 ~ "Mais de 30m²") %>%
           fct_rev()
  )

#
embraesp <- sfarrow::st_read_parquet(
  here(
    # "scripts", "09_NotasTecnicas", "VagasGaragemSP",
    # "dados", "Lancamentos", "Embraesp", "ArquivosTratados", 
    "inputs", "2_trusted", "Lancamentos",
    "embraesp.parquet")) %>%
  filter(origem == "RESIDENCIAL") # Tomando somente empreendimentos residenciais


# 2. ----------------------------------------------------------------------

# Evolução da média de vagas por unidade lançada

# As vagas por unidade têm tendência de queda desde antes da aprovação do PDE e 
# se intensificam a partir de 2016
plot_vagas_por_unidade_sp_vs_eetu <- embraesp_por_empreendimento %>%
  st_drop_geometry() %>%
  filter(ano >= 2009) %>%
  group_by(ano) %>% 
  summarise(
    vagas_por_unidade = mean(sum(vagas)/sum(n_unidades))
  )


# 3.  ---------------------------------------------------------------------
# Evolução da média de unidades por empreendimento lançado nos eixos
# Número médio de unidades por empreendimento cresce nos eixos após 2016
plot_eetu_media_unidades <- embraesp_por_empreendimento %>% 
  st_drop_geometry() %>%
  filter(ano >= 2009 & zoneamento_grupo == "EETU") %>%
  group_by(ano) %>% 
  summarise(#n_empreendimentos = n_distinct(id_empreendimento),
            n_unidades_media = mean(n_unidades))


# Evolução da área útil média das unidades lançadas nos eixos
# Queda indica unidades de tamanho menores sendo produzidas nos eixos
plot_eetu_media_area <-embraesp_por_empreendimento %>% 
  st_drop_geometry() %>%
  filter(ano >= 2009 & zoneamento_grupo == "EETU") %>%
  group_by(ano) %>% 
  summarise(area_util_media = mean(area_util_media))


# 4.  ---------------------------------------------------------------------

# Evolução do número de unidades com menos e mais de 35m² nos eixos
# O crescimento de unidades com menos de 35m² é intenso a partir de 2018
plot_eetu_unidades_por_area <- embraesp %>% 
  st_drop_geometry() %>%
  filter(ano >= 2009 & zoneamento_grupo == "EETU") %>%
  group_by(fx_area_util, ano) %>% 
  summarise(#n_empreendimentos = n_distinct(id_empreendimento),
            n_unidades = sum(num_total_))


# Evolução do desvio-padrão da área útil das unidades por empreendimento lançado nos eixos
# Aumento indica maiores diferenças de metragem entre unidades de um mesmo empreendimento
plot_eetu_dp_area <- embraesp_por_empreendimento %>% 
  st_drop_geometry() %>%
  filter(ano >= 2009 & zoneamento_grupo == "EETU") %>%
  group_by(ano) %>% 
  summarise(area_util_dp = mean(area_util_dp))


# 5.  ---------------------------------------------------------------------

# Diferentes cenários de limites de vagas não computáveis nos eixos
# O número máximo de vagas não computáveis cai no cenário sem nenhum ajuste de mercado, 
# mas cresce 7% caso o mercado deixe de produzir estúdios com menos de 35m² de área útil
plot_eetu_cenarios_1 <-  embraesp_por_empreendimento %>% 
  st_drop_geometry() %>%
  filter(ano >= 2019 & zoneamento_grupo == "EETU") %>%
  mutate(
    # Pressuposto: vagas = limite de vagas = número de unidades
    vagas = n_unidades,
    vagas_studio = n_unidades_area_util_menos_35m2,
    
    # Cenário 2
    vagas_nova_regra_1 = round(area_comp_acima_30m2/70),
    vagas_presumidas_1 = if_else(
      # vagas nova regra >= vagas na regra atual - vagas de estúdio
      vagas_nova_regra_1 >= (vagas-vagas_studio),
      vagas_nova_regra_1, (vagas-vagas_studio)),
    
    # Cenário 3
    vagas_nova_regra_2 = round((area_comp)/70),
    vagas_presumidas_2 = if_else(
      vagas_nova_regra_2 >= vagas,
      vagas_nova_regra_2, (vagas))
  ) %>%
  select(
    id_empreendimento, ano, zoneamento_grupo, n_unidades, 
    n_unidades_area_util_menos_35m2, area_comp, area_util_acima_35m2, 
    vagas, vagas_presumidas_1, vagas_presumidas_2) %>%
  group_by(ano) %>%
  dplyr::summarise(
    "Cenário 1 (regra atual)" = sum(vagas),
    "Cenário 2 (regra proposta)" = sum(vagas_presumidas_1),
    "Cenário 3 (regra proposta com mercado adaptado)" = sum(vagas_presumidas_2)) %>%
  pivot_longer(-c(ano), values_to = "n") %>%
  group_by(name) %>%
  summarize(n = sum(n))

# xx.  --------------------------------------------------------------------

fs::dir_create(here("inputs", "3_portal_dados", 
                    "Historias", "03_VagasDeGaragemNosEixos"))


list_outputs <- mget(ls(.GlobalEnv, pattern = "plot_"), envir = .GlobalEnv)

names(list_outputs)

list_outputs %>%
  map2(names(.), 
       ~writexl::write_xlsx(.x,
                            here("inputs", "3_portal_dados", "Historias", 
                                 "03_VagasDeGaragemNosEixos", 
                                 paste0(.y, ".xlsx")))
  )

