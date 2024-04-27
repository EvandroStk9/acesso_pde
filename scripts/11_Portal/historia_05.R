# Objetivo
# Fornecer dados para deploy de dados .csv para a história referente a NT1
# Dados serão usados pelo DataWrapper e embedados no html

## ----library--------------------------------------------------
library(here)
library(sf)
library(sfarrow)
library(arrow)
library(tidyverse)
library(lubridate)
library(skimr)
library(stringr)
library(tidyverse)
library(readxl)
library(ggtext)
library(patchwork)
library(ggrepel)
library(ggthemes)
library(scales)
library(gt)


## ---- include=FALSE-------------------------------------------

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
alvaras_por_lote <- 
  sfarrow::st_read_parquet(here("inputs", "2_trusted",
                                "Licenciamentos",
                                "alvaras_por_lote.parquet")) %>%
  filter(ano_execucao >=2013) %>%
  mutate(
    periodo = case_when(
      between(ano_execucao, 2013, 2015) ~ "2013-2015",
      between(ano_execucao, 2016, 2018) & legislacao == "PDE2002eLPUOS2004" ~ "2016-2018 (Pré-PDE)",
      between(ano_execucao, 2016, 2018) & legislacao == "PDE2014eLPUOS2016" ~ "2016-2018 (Pós-PDE)",
      between(ano_execucao, 2019, 2021) ~ "2019-2021",
      # legislacao == "PDE2014eLPUOS2004" ~ "PDE2014eLPUOS2004",
      TRUE ~ NA_character_) %>%
      factor(levels = c("2013-2015", "2016-2018 (Pré-PDE)", 
                        "2016-2018 (Pós-PDE)", "2019-2021")),
    legislacao = if_else(legislacao == "Pré-PDE2014", "PDE2002eLPUOS2004", legislacao) %>%
      factor(levels = c("PDE2002eLPUOS2004", "PDE2014eLPUOS2004", "PDE2014eLPUOS2016")),
    subcategoria = case_when(
      # categoria_de_uso_grupo == "ERM" & ind_r2h == TRUE ~ "R2H",
      # categoria_de_uso_grupo == "ERM" & ind_r2v == TRUE ~ "R2V",
      categoria_de_uso_grupo == "ERM" ~ "Outra",
      ind_hmp == TRUE & ind_his == TRUE ~ "HIS & HMP",
      ind_hmp == TRUE ~ "HMP",
      ind_his == TRUE ~ "HIS",
      ind_ezeis == TRUE ~ "EZEIS"),
    porte = case_when(
      n_unidades < 50 ~ "Pequeno porte",
      between(n_unidades, 50, 199) ~ "Médio porte",
      n_unidades >= 200 ~ "Grande porte",
      TRUE ~ NA_character_),
    local = case_when(
      ind_miolo == TRUE ~ "Miolos de bairro",
      zoneamento_grupo == "EETU" ~ "EETU's",
      macroarea == "MEM" & 
        zoneamento_grupo != "EETU" ~ "MEM (Exceto EETU's)")
  )


#
alvaras_sfstats <- arrow::read_parquet(
  here("inputs", "2_trusted", "Licenciamentos", 
       "alvaras_sfstats_no_geo.parquet")
)

#
stats_zoneamento_por_macroarea <- arrow::read_parquet(
  here("inputs", "2_trusted", "Legislacao", 
       "zoneamento_por_zona_grupo_no_geo.parquet"))


# 2. ----------------------------------------------------------------------

shifts_alvaras_categoria <-
  st_drop_geometry(alvaras_por_lote) %>%
  # Reordena dataframe
  mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                             legislacao == "PDE2002eLPUOS2004" ~ "t0",
                             TRUE ~ NA_character_)) %>%
  filter(!is.na(periodo)) %>%
  # Adiciona valores por categoria
  group_by(periodo, categoria_de_uso_grupo) %>%
  summarize(n_empreendimentos = n(),
            n_unidades = sum(n_unidades, na.rm = TRUE),
            area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
            area_da_construcao = sum(area_da_construcao, na.rm = TRUE)) %>%
  ungroup() %>%
  pivot_longer(cols = -c(periodo, categoria_de_uso_grupo)) %>%
  full_join(
    # Adiciona valores totais SP como categoria SP
    st_drop_geometry(alvaras_por_lote) %>%
      mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                                 legislacao == "PDE2002eLPUOS2004" ~ "t0",
                                 TRUE ~ NA_character_)) %>%
      filter(!is.na(periodo)) %>%
      # Adiciona valores por categoria
      mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                                 legislacao == "PDE2002eLPUOS2004" ~ "t0",
                                 TRUE ~ NA_character_)) %>%
      filter(!is.na(periodo)) %>%
      group_by(periodo) %>%
      summarize(categoria_de_uso_grupo = "SP",
                n_empreendimentos = n(),
                n_unidades = sum(n_unidades, na.rm = TRUE),
                area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
                area_da_construcao = sum(area_da_construcao, na.rm = TRUE)) %>%
      ungroup() %>%
      pivot_longer(cols = -c(periodo, categoria_de_uso_grupo)) 
  ) %>%
  # Calcula taxas de variação 
  group_by(categoria_de_uso_grupo, name) %>%
  arrange(categoria_de_uso_grupo, name) %>%
  mutate(rate = (value/lag(value))-1) %>%
  arrange(name, periodo) %>%
  pivot_wider(values_from = c(value, rate), names_from = periodo) %>%
  group_by(name) %>%
  transmute(categoria_de_uso_grupo, 
            name,
            "PDE2002 e LPUOS 2004" = value_t0, "PDE 2014 e LPUOS 2016" = value_t1,
            variacao = rate_t1,
            variacao_sp = rate_t1[which(categoria_de_uso_grupo=="SP")],
            diferencial = rate_t1-rate_t1[which(categoria_de_uso_grupo=="SP")])


# Indicadores: antes e depois por macroárea
shifts_alvaras_macroarea <-
  st_drop_geometry(alvaras_por_lote) %>%
  # Reordena dataframe
  filter(!is.na(macroarea)) %>%
  mutate(#macroarea = if_else(macroarea == "MEM", "MEM", "Fora da MEM"),
    macroarea = fct_other(macroarea, keep = c("MEM", "MUC", "MQU", "MRVU"),
                          other_level = "Outra"),
    # periodo = case_when(between(ano_execucao, 2013, 2015) ~ "t0",
    #                     between(ano_execucao, 2019, 2021) ~ "t1",
    #                     TRUE ~ NA_character_)
    periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                        legislacao == "PDE2002eLPUOS2004" ~ "t0",
                        TRUE ~ NA_character_)) %>%
  filter(!is.na(periodo)) %>%
  # Adiciona valores por macroarea
  group_by(periodo, macroarea) %>%
  summarize(n_empreendimentos = n(),
            n_unidades = sum(n_unidades, na.rm = TRUE),
            area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
            area_da_construcao = sum(area_da_construcao, na.rm = TRUE)) %>%
  ungroup() %>%
  pivot_longer(cols = -c(periodo, macroarea)) %>%
  full_join(
    # Adiciona valores totais SP como macroarea SP
    st_drop_geometry(alvaras_por_lote) %>%
      filter(!is.na(macroarea)) %>%
      mutate(#macroarea = if_else(macroarea == "MEM", "MEM", "Fora da MEM"),
        macroarea = fct_other(macroarea, keep = c("MEM", "MUC", "MQU", "MRVU"),
                              other_level = "Outra"),
        periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                            legislacao == "PDE2002eLPUOS2004" ~ "t0",
                            TRUE ~ NA_character_)) %>%
      filter(!is.na(periodo)) %>%
      group_by(periodo) %>%
      summarize(macroarea = "SP",
                n_empreendimentos = n(),
                n_unidades = sum(n_unidades, na.rm = TRUE),
                area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
                area_da_construcao = sum(area_da_construcao, na.rm = TRUE)) %>%
      ungroup() %>%
      pivot_longer(cols = -c(periodo, macroarea)) 
  ) %>%
  full_join(
    # Adiciona valores totais Macroáreas Urbanas como macroarea "MEM, MUC, MQU ou MRVU"
    st_drop_geometry(alvaras_por_lote) %>%
      filter(!is.na(macroarea)) %>%
      mutate(#macroarea = if_else(macroarea == "MEM", "MEM", "Fora da MEM"),
        macroarea = fct_other(macroarea, keep = c("MEM", "MUC", "MQU", "MRVU"),
                              other_level = "Outra"),
        periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                            legislacao == "PDE2002eLPUOS2004" ~ "t0",
                            TRUE ~ NA_character_)) %>%
      filter(!is.na(periodo) & macroarea != "Outra") %>%
      mutate(macroarea = "MEM, MUC, MQU ou MRVU") %>%
      group_by(periodo, macroarea) %>%
      summarize(n_empreendimentos = n(),
                n_unidades = sum(n_unidades, na.rm = TRUE),
                area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
                area_da_construcao = sum(area_da_construcao, na.rm = TRUE)) %>%
      ungroup() %>%
      pivot_longer(cols = -c(periodo, macroarea)) 
  ) %>%
  # Calcula taxas de variação 
  group_by(macroarea, name) %>%
  arrange(macroarea, name) %>%
  mutate(rate = (value/lag(value))-1) %>%
  arrange(name, periodo) %>%
  pivot_wider(values_from = c(value, rate), names_from = periodo) %>%
  group_by(name) %>%
  transmute(macroarea, 
            name,
            "PDE2002 e LPUOS 2004" = value_t0, "PDE 2014 e LPUOS 2016" = value_t1,
            variacao = rate_t1,
            variacao_sp = rate_t1[which(macroarea=="SP")],
            diferencial = rate_t1-rate_t1[which(macroarea=="SP")])


# Indicadores: antes e depois por local
shifts_alvaras_local <-
  st_drop_geometry(alvaras_por_lote) %>%
  # Reordena dataframe
  mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                             legislacao == "PDE2002eLPUOS2004" ~ "t0",
                             TRUE ~ NA_character_)) %>%
  filter(!is.na(periodo)) %>%
  # Adiciona valores por local
  group_by(periodo, local) %>%
  summarize(n_empreendimentos = n(),
            n_unidades = sum(n_unidades, na.rm = TRUE),
            area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
            area_da_construcao = sum(area_da_construcao, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  pivot_longer(cols = -c(periodo, local)) %>%
  full_join(
    # Adiciona valores totais SP como local SP
    st_drop_geometry(alvaras_por_lote) %>%
      mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                                 legislacao == "PDE2002eLPUOS2004" ~ "t0",
                                 TRUE ~ NA_character_)) %>%
      filter(!is.na(periodo)) %>%
      group_by(periodo) %>%
      summarize(local = "SP",
                n_empreendimentos = n(),
                n_unidades = sum(n_unidades, na.rm = TRUE),
                area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
                area_da_construcao = sum(area_da_construcao, na.rm = TRUE)
      ) %>%
      ungroup() %>%
      pivot_longer(cols = -c(periodo, local)) 
  ) %>%
  # Calcula taxas de variação 
  group_by(local, name) %>%
  arrange(local, name) %>%
  mutate(rate = (value/lag(value))-1) %>%
  arrange(name, periodo) %>%
  pivot_wider(values_from = c(value, rate), names_from = periodo) %>%
  group_by(name) %>%
  transmute(local, 
            name,
            "PDE2002 e LPUOS 2004" = value_t0, "PDE 2014 e LPUOS 2016" = value_t1,
            variacao = rate_t1,
            variacao_sp = rate_t1[which(local=="SP")],
            diferencial = rate_t1-rate_t1[which(local=="SP")])


# Indicadores: ERP' antes e depois por zoneamento
shifts_alvaras_erp_zona <-
  st_drop_geometry(alvaras_por_lote) %>%
  # Reordena dataframe
  mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                             legislacao == "PDE2002eLPUOS2004" ~ "t0",
                             TRUE ~ NA_character_)) %>%
  filter(!is.na(periodo)) %>%
  filter(categoria_de_uso_grupo == "ERP") %>%
  # Adiciona valores por local
  group_by(periodo, zoneamento_grupo) %>%
  summarize(n_empreendimentos = n(),
            n_unidades = sum(n_unidades, na.rm = TRUE),
            area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
            area_da_construcao = sum(area_da_construcao, na.rm = TRUE)
  ) %>%
  ungroup() %>%
  pivot_longer(cols = -c(periodo, zoneamento_grupo)) %>%
  full_join(
    # Adiciona valores totais SP como local SP
    st_drop_geometry(alvaras_por_lote) %>%
      filter(categoria_de_uso_grupo == "ERP") %>%
      mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                                 legislacao == "PDE2002eLPUOS2004" ~ "t0",
                                 TRUE ~ NA_character_)) %>%
      filter(!is.na(periodo)) %>%
      group_by(periodo) %>%
      summarize(zoneamento_grupo = "SP",
                n_empreendimentos = n(),
                n_unidades = sum(n_unidades, na.rm = TRUE),
                area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
                area_da_construcao = sum(area_da_construcao, na.rm = TRUE)
      ) %>%
      ungroup() %>%
      pivot_longer(cols = -c(periodo, zoneamento_grupo)) 
  ) %>%
  # Calcula taxas de variação 
  group_by(zoneamento_grupo, name) %>%
  arrange(zoneamento_grupo, name) %>%
  mutate(rate = (value/lag(value))-1) %>%
  arrange(name, periodo) %>%
  pivot_wider(values_from = c(value, rate), names_from = periodo) %>%
  group_by(name) %>%
  transmute(zoneamento_grupo, 
            name,
            "PDE2002 e LPUOS 2004" = value_t0, "PDE 2014 e LPUOS 2016" = value_t1,
            variacao = rate_t1,
            variacao_sp = rate_t1[which(zoneamento_grupo=="SP")],
            diferencial = rate_t1-rate_t1[which(zoneamento_grupo=="SP")])



# 3.  ---------------------------------------------------------------------



#
plot_lotes <- 
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao) %>%
  summarize(n = n())

#
plot_unidades <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao) %>%
  summarize(n = sum(n_unidades, na.rm = TRUE))


#
plot_area_da_construcao <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao) %>%
  summarize(n = sum(area_da_construcao, na.rm = TRUE))

#
plot_area_do_terreno <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao) %>%
  summarize(n = sum(area_do_terreno, na.rm = TRUE)) 


# 4.  ---------------------------------------------------------------------

#
plot_lotes_por_categoria <- 
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao, categoria_de_uso_grupo) %>%
  summarize(n = n())

#
plot_unidades_por_categoria <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao, categoria_de_uso_grupo) %>%
  summarize(n = sum(n_unidades, na.rm = TRUE))

#
plot_area_da_construcao_por_categoria <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao, categoria_de_uso_grupo) %>%
  summarize(n = sum(area_da_construcao, na.rm = TRUE))
#
plot_area_do_terreno_por_categoria <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  group_by(ano_execucao, categoria_de_uso_grupo) %>%
  summarize(n = sum(area_do_terreno, na.rm = TRUE)) 

# 5.  ---------------------------------------------------------------------



#
table_shifts_alvaras_macroarea <- shifts_alvaras_macroarea %>%
  filter(macroarea == "Outra" | macroarea == "MEM, MUC, MQU ou MRVU") %>%
  mutate(macroarea = if_else(macroarea == "Outra", "MVRURA, MCQUA, MPEN ou MCUUS", macroarea)) %>%
  mutate(across(c(variacao, variacao_sp, diferencial), 
                ~ scales::label_percent(accuracy = 0.1, decimal.mark = ",")(.x)),
         across(where(is.numeric), ~round(.x)),
         name = fct_recode(name, 
                           "Área construída (m²)" = "area_da_construcao",
                           "Área do terreno (m²)" = "area_do_terreno",
                           "Empreendimentos" = "n_empreendimentos", 
                           "Unidades habitacionais" = "n_unidades")) %>%
  gt(groupname_col = "macroarea") %>%
  tab_spanner(
    md("**Período**"),
    3:4) %>%
  tab_spanner(
    label = md("**Taxas de variação**"),
    columns = c(5:7)
  ) %>%
  # cols_width(everything() ~ px(50)) %>%
  cols_label(name = "",
             variacao = md("&Delta;<sub>Macroarea</sub>"),
             variacao_sp = md("&Delta;<sub>SP</sub>"),
             diferencial = md("&Delta;<sub>Macroarea</sub>-&Delta;<sub>SP</sub>")) %>%
  fmt_number(columns = 3:4,
             decimals = 0,
             sep_mark = ".") %>%
  tab_footnote(
    md("*Obs: considerados apenas os licenciamentos com execução entre 2013 e 2021* <br>"),
    locations = cells_column_spanners(1)
  ) %>%
  tab_source_note(source_note = md("
  **Elaboração**: Laboratório Arq.Futuro - Insper | **Dados**: Prefeitura de São Paulo")
  ) %>%
  tab_options(#column_labels.font.weight = "bolder",
    heading.align = "left",
    table.align = "left",
    row_group.font.weight = "bold",
    # row_group.background.color = "grey",
    source_notes.font.size = 14,
    table.width = pct(100)) %>%
  cols_align("center") %>%
  cols_align("left", columns = "name") %>%
  tab_style(
    style = cell_text(color = "red"),
    locations = list(
      cells_body(
        columns = 7,
        rows = diferencial < 0
      )
    )
  ) %>%
  tab_style(
    style = cell_text(color = "darkblue"),
    locations = list(
      cells_body(
        columns = 7,
        rows = diferencial > 0
      )
    )
  )


# xx. ---------------------------------------------------------------------

# list_outputs <- list(plot_area_da_construcao_por_categoria,
#      plot_area_do_terreno_por_categoria,
#      plot_unidades_por_categoria,
#      plot_lotes_por_categoria)
#
fs::dir_create(here("inputs", "3_portal_dados", 
                    "Historias", "05_ProducaoHabitacionalFormal"))

list_outputs <- mget(ls(.GlobalEnv, pattern = "plot_"), envir = .GlobalEnv)

names(list_outputs)

list_outputs %>%
  map2(names(.), 
       ~writexl::write_xlsx(.x,
                                  here("inputs", "3_portal_dados", "Historias", 
                                       "05_ProducaoHabitacionalFormal", 
                                       paste0(.y, ".xlsx")))
  )

