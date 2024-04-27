# Objetivo
# Fornecer dados para deploy de dados .csv para a história referente a NT2
# Dados serão usados pelo DataWrapper e embedados no html

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


# Inbound --------------------------------------------------------------

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"


#
zonas_eetu <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu.parquet")) %>%
  select(-isocrona, -distancia_cbd) %>%
  mutate(
    across(
      c(eixo, tipo_infra, tipo_transp),
      str_to_title
    )
  )

#
zonas_eetu_por_estacao <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu_por_estacao.parquet"))

#
zonas_eetu_potencial <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu_potencial_mem.parquet")) %>%
  bind_rows(zonas_eetu)

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
    subcategoria = case_when(
      # categoria_de_uso_grupo == "ERM" & ind_r2h == TRUE ~ "R2H",
      # categoria_de_uso_grupo == "ERM" & ind_r2v == TRUE ~ "R2V",
      categoria_de_uso_grupo == "ERM" ~ "Outra",
      ind_hmp == TRUE & ind_his == TRUE ~ "HIS & HMP",
      ind_hmp == TRUE ~ "HMP", 
      ind_his == TRUE ~ "HIS",
      ind_ezeis == TRUE ~ "EZEIS"),
    legislacao = if_else(legislacao == "Pré-PDE2014", "PDE2002eLPUOS2004", legislacao) %>%
      factor(levels = c("PDE2002eLPUOS2004", "PDE2014eLPUOS2004", "PDE2014eLPUOS2016")),
    porte = case_when(
      n_unidades < 50 ~ "Pequeno porte",
      between(n_unidades, 50, 199) ~ "Médio porte",
      n_unidades >= 200 ~ "Grande porte",
      TRUE ~ NA_character_),
    local = case_when(
      zoneamento_grupo == "EETU Futuros" ~ "EETU's Futuros", 
      zoneamento_grupo == "EETU" ~ "EETU's",
      macroarea == "MEM" & id_zoneamento %in% zonas_eetu_potencial$id_zoneamento ~ "MEM - possíveis EETU's",
      TRUE ~ "Outro"))

#
alvaras_zonas_eetu_por_lote <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Licenciamentos",
       "alvaras_zonas_eetu_por_lote.parquet")) %>%
  mutate(
    across(
      c(eixo, tipo_infra, tipo_transp),
      str_to_title
    )
  )

#
alvaras_zonas_eetu_por_eixo <- zonas_eetu %>%
  mutate(
    X = map_dbl(geometry, ~st_centroid(.x)[[1]]),
    Y = map_dbl(geometry, ~st_centroid(.x)[[2]])) %>%
  left_join(alvaras_zonas_eetu_por_lote %>%
              filter(ano_execucao >= 2014) %>%
              select(id, id_zoneamento, n_unidades, categoria_de_uso_grupo,
                     legislacao, distancia_cbd) %>%
              st_drop_geometry(),
            by = c("id_zoneamento")) %>%
  group_by(eixo, tipo_infra) %>% # Agregando dado original em que observação = quadra-eixo
  summarize(#n_quadras = first(n_quadras),
    n_empreendimentos = n_distinct(id, na.rm = TRUE),
    area_eixo = first(area_eixo),
    n_unidades_eixo = sum(n_unidades, na.rm = TRUE),
    n_erp = sum(if_else(categoria_de_uso_grupo == "ERP", n_unidades, 0), na.rm = TRUE),
    n_erm = sum(if_else(categoria_de_uso_grupo == "ERM", n_unidades, 0), na.rm = TRUE),
    percent_erp = (n_erp / n_unidades_eixo),
    n_unidades_por_area = round(n_unidades_eixo/(area_eixo/1000000), 1),
    n_erp_por_area = round(n_erp/(area_eixo/1000000), 1),
    n_erm_por_area = round(n_erm/(area_eixo/1000000), 1),
    tipo_transp = first(tipo_transp),
    distrito = first(distrito),
    distancia_cbd = mean(distancia_cbd, na.rm = TRUE),
    acessibilidade_empregos = mean(acessibilidade_empregos, na.rm = TRUE),
    X = mean(X), Y = mean(Y)
  ) %>%
  ungroup()

#
alvaras_zonas_eetu_por_eixo_por_legislacao <- alvaras_zonas_eetu_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >= 2014) %>%
  group_by(eixo, tipo_infra, legislacao) %>% # Agregando dado original em que observação = quadra-eixo
  summarize(#n_quadras = first(n_quadras),
    n_empreendimentos = n_distinct(id, na.rm = TRUE),
    area_eixo = first(area_eixo),
    n_unidades_eixo = sum(n_unidades, na.rm = TRUE),
    n_erp = sum(if_else(categoria_de_uso_grupo == "ERP", n_unidades, 0), na.rm = TRUE),
    n_erm = sum(if_else(categoria_de_uso_grupo == "ERM", n_unidades, 0), na.rm = TRUE),
    percent_erp = (n_erp / n_unidades_eixo),
    n_unidades_por_area = round(n_unidades_eixo/(area_eixo/1000000), 1),
    n_erp_por_area = round(n_erp/(area_eixo/1000000), 1),
    n_erm_por_area = round(n_erm/(area_eixo/1000000), 1),
    tipo_transp = first(tipo_transp),
    # distrito = first(distrito),
    distancia_cbd = mean(distancia_cbd, na.rm = TRUE),
    acessibilidade_empregos = mean(acessibilidade_empregos, na.rm = TRUE)#,
    #X = mean(X), Y = mean(Y)
  ) %>%
  ungroup()

#
shifts_alvaras_zoneamento <- alvaras_por_lote %>%
  st_drop_geometry() %>%
  # Reordena dataframe
  mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                             legislacao == "PDE2002eLPUOS2004" ~ "t0",
                             TRUE ~ NA_character_)) %>%
  filter(!is.na(periodo)) %>%
  # Adiciona valores por zoneamento
  group_by(periodo, zoneamento_grupo) %>%
  summarize(n_empreendimentos = n(),
            n_unidades = sum(n_unidades, na.rm = TRUE),
            area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
            area_da_construcao = sum(area_da_construcao, na.rm = TRUE)) %>%
  ungroup() %>%
  pivot_longer(cols = -c(periodo, zoneamento_grupo)) %>%
  full_join(
    # Adiciona valores totais SP como zoneamento SP
    st_drop_geometry(alvaras_por_lote) %>%
      mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                                 legislacao == "PDE2002eLPUOS2004" ~ "t0",
                                 TRUE ~ NA_character_)) %>%
      filter(!is.na(periodo)) %>%
      # Adiciona valores por zoneamento
      mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                                 legislacao == "PDE2002eLPUOS2004" ~ "t0",
                                 TRUE ~ NA_character_)) %>%
      filter(!is.na(periodo)) %>%
      group_by(periodo) %>%
      summarize(zoneamento_grupo = "SP",
                n_empreendimentos = n(),
                n_unidades = sum(n_unidades, na.rm = TRUE),
                area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
                area_da_construcao = sum(area_da_construcao, na.rm = TRUE)) %>%
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
  full_join(
    # Adiciona valores totais Macroáreas Urbanas como macroarea "MEM, MUC, MQU ou MRVU"
    st_drop_geometry(alvaras_por_lote) %>%
      mutate(periodo = case_when(legislacao == "PDE2014eLPUOS2016" ~ "t1",
                                 legislacao == "PDE2002eLPUOS2004" ~ "t0",
                                 TRUE ~ NA_character_)) %>%
      filter(!is.na(periodo) & local != "Outro") %>%
      mutate(local = "EETU's/ EETU's Futuros/ MEM - Possíveis EETU's") %>%
      group_by(periodo, local) %>%
      summarize(n_empreendimentos = n(),
                n_unidades = sum(n_unidades, na.rm = TRUE),
                area_do_terreno = sum(area_do_terreno, na.rm = TRUE),
                area_da_construcao = sum(area_da_construcao, na.rm = TRUE)) %>%
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

# Linha Metro
linha_metro <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "linha_metro.parquet"))

# Linha Trem
linha_trem <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "linha_trem.parquet"))

# Linha Ônibus
corredor_onibus <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "corredor_onibus.parquet"))


# 1. Legislação --------------------------------------------------------------

# ## Evolução da distribuição da incidência da legislação urbana aplicada em cada alvará (2008-2021)  
# plot_legislacao <- alvaras_por_lote %>%
#   st_drop_geometry() %>%
#   mutate(legislacao = case_when(
#     legislacao == "PDE2002eLPUOS2004" ~ "PDE2002 e LPUOS2004",
#     legislacao == "PDE2014eLPUOS2016" ~ "PDE2014 e LPUOS2016",
#     legislacao == "PDE2014eLPUOS2004" ~ "PDE2014 e LPUOS2004",
#     TRUE ~ as.character(legislacao)) %>%
#       factor(levels = c("PDE2002 e LPUOS2004", "PDE2014 e LPUOS2004", 
#                         "PDE2014 e LPUOS2016"))) %>%
#   count(legislacao, ano_execucao) %>%
#   group_by(ano_execucao) %>%
#   mutate(prop = n/sum(n))



# 2. Mapa Zonas EETU -----------------------------------------------------------

## Mapa identificando os EETU por infraestrutura de transporte que ativa o eixo 

#
plot_map_zonas_eetu <- zonas_eetu

# #
# plot_map_corredor_onibus <- corredor_onibus
# 
# #
# plot_map_linha_trem <- linha_trem
# 
# #
# plot_map_linha_metro <- linha_metro


# 3. Evolução -----------------------------------------------------
# Evolução do número de empreendimentos, unidades residenciais, área construída e área de terreno licenciados no perímetro dos atuais EETU (2013-2021) 

#
plot_eetu_empreendimentos <- 
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  filter(zoneamento_grupo == "EETU") %>%
  count(ano_execucao)

#
plot_eetu_unidades <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  filter(zoneamento_grupo == "EETU") %>%
  count(ano_execucao, wt = n_unidades)

#
plot_eetu_area_da_construcao <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >= 2013) %>%
  filter(zoneamento_grupo == "EETU") %>%
  count(ano_execucao, wt = area_da_construcao/1000)

#
plot_eetu_area_do_terreno <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  filter(zoneamento_grupo == "EETU") %>%
  count(ano_execucao, wt = area_do_terreno/1000)

# 4. Zoneamentos ----------------------------------------------

# Distribuição percentual das unidades residenciais licenciadas entre os perímetros das zonas atuais (períodos e regramentos selecionados) 

#
# plot_percent_zoneamento <- alvaras_por_lote %>%
#   st_drop_geometry() %>%
#   count(zoneamento_grupo, periodo, wt = n_unidades) %>%
#   filter(!is.na(periodo)) %>%
#   group_by(periodo) %>%
#   mutate(prop = n/sum(n))

# 5. Zoneamento Variações ------------------------------------------------------------

# Variações no número de empreendimentos, unidades residenciais e áreas de construção e terreno licenciados no perímetro dos atuais EETU (regramentos selecionados) 

#
plot_eetu_shifts_alvaras <- shifts_alvaras_zoneamento %>%
  filter(zoneamento_grupo == "EETU")

# 6. Evolução -------------------------------------------------------------

# Evolução do número de unidades por empreendimento, da área construída por unidade, da cota parte e do número de pavimentos dos empreendimentos licenciados no perímetro dos atuais EETU (2013-2021) 
#
plot_eetu_media_unidades <- alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  filter(zoneamento_grupo == "EETU") %>%
  group_by(ano_execucao) %>%
  summarize(n_unidades = mean(n_unidades, na.rm = TRUE))

# #
# plot_eetu_unidades_area_media <-
#   alvaras_por_lote %>%
#   st_drop_geometry() %>%
#   group_by(ano_execucao) %>%
#   filter(ano_execucao >=2013) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   summarize(area_por_unidade = mean(area_da_construcao/n_unidades, na.rm = TRUE))
# 
# #
# plot_eetu_cota_parte <-
#   alvaras_por_lote %>%
#   st_drop_geometry() %>%
#   filter(ano_execucao >=2013) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   group_by(ano_execucao) %>%
#   filter(ano_execucao >=2013) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   summarize(cota_parte = mean(cota_parte, na.rm = TRUE))

#
plot_eetu_pavimentos <-
  alvaras_por_lote %>%
  st_drop_geometry() %>%
  filter(ano_execucao >=2013) %>%
  filter(zoneamento_grupo == "EETU") %>%
  group_by(ano_execucao) %>%
  filter(ano_execucao >=2013) %>%
  filter(zoneamento_grupo == "EETU") %>%
  summarize(n_pavimentos_por_bloco = mean(n_pavimentos_por_bloco, na.rm = TRUE))

# 7. Zoneamento por Categoria ---------------------------------------------------

## Distribuição percentual das unidades residenciais licenciadas entre os perímetros das zonas atuais por categoria (períodos selecionados e regramento aplicado)
# plot_percent_zoneamento_por_categoria <- alvaras_por_lote %>%
#   count(zoneamento_grupo, periodo, categoria_de_uso_grupo, wt = n_unidades) %>%
#   filter(!is.na(periodo)) %>%
#   group_by(periodo, categoria_de_uso_grupo) %>%
#   mutate(prop = n/sum(n))

# 8. Unidades por categoria -------------------------------------------------------------

## Evolução do número de unidades residenciais licenciadas no perímetro dos atuais EETU por tipo de empreendimento (2013-2021) 
#
# plot_eetu_unidades_por_categoria <- alvaras_por_lote %>%
#   st_drop_geometry() %>%
#   filter(ano_execucao >=2013 & zoneamento_grupo == "EETU") %>%
#   count(categoria_de_uso_grupo, ano_execucao, wt = n_unidades)

# 9. Distância ao CBD ---------------------------------------------------------

## Distribuição dos empreendimentos em EETU's em função de sua distância ao centro da cidade (regramento aplicado)

# Gerar gráfico como o abaixo mostrando a localização dos ERM e ERP em função da distância ao centro para pré-PDE e pós-PDE 
#
# plot_densidade_cbd_t0 <-
#   st_drop_geometry(alvaras_por_lote) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   filter(legislacao %in% c("PDE2002eLPUOS2004")) %>%
#   ggplot(aes(distancia_cbd, ..density.., group = categoria_de_uso_grupo, fill = categoria_de_uso_grupo)) +
#   geom_density(alpha = .9) +
#   scale_x_continuous(breaks = seq(0, 35, 5),
#                      labels = function(x) paste0(x, " km")) +
#   scale_y_continuous(limits = c(0, 0.25),
#                      breaks = seq(0, 0.25, 0.1)) +
#   theme_pander(base_size = 30, base_family = "lato") +
#   labs(title = "",
#        subtitle = "PDE2002 e LPUOS2004",
#        x = "",
#        y = "") +
#   theme(plot.subtitle = element_text(size = 24, hjust = 0.5),
#         axis.text.x = element_text(size = 18),
#         legend.title = element_blank(),
#         legend.position = "none") 

#
# plot_densidade_cbd_t1 <-
#   st_drop_geometry(alvaras_por_lote) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   filter(legislacao %in% c("PDE2014eLPUOS2016")) %>%
#   ggplot(aes(distancia_cbd, ..density.., group = categoria_de_uso_grupo, fill = categoria_de_uso_grupo)) +
#   geom_density(alpha = .9) +
#   scale_x_continuous(breaks = seq(0, 35, 5),
#                      labels = function(x) paste0(x, " km")) +
#   scale_y_continuous(limits = c(0, 0.25),
#                      breaks = seq(0, 0.25, 0.1)) +
#   theme_pander(base_size = 30, base_family = "lato") +
#   labs(title = "",
#        subtitle = "PDE2014 e LPUOS2016",
#        x = "",
#        y = "") +
#   theme(plot.subtitle = element_text(size = 24, hjust = 0.5),
#         axis.text.x = element_text(size = 18),
#         legend.title = element_blank(),
#         legend.position = c(0.8, 0.6)) 

##
# plot_histograma_cbd_t0 <-
#   st_drop_geometry(alvaras_por_lote) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   filter(legislacao %in% c("PDE2002eLPUOS2004")) %>%
#   ggplot(aes(distancia_cbd, ..count.., 
#              fill = categoria_de_uso_grupo, shape = categoria_de_uso_grupo)) +
#   geom_histogram(position = "stack", color = "black", alpha = .9, 
#                  breaks = seq(0, 30, 1)) +
#   # geom_jitter(aes(x= distancia, y= ..count..), stat = "count", 
#   #             alpha = 0.3) +
#   scale_y_continuous(breaks = seq(0, 300, 20),
#                      limits = c(0, 80)) +
#   scale_x_continuous(breaks = seq(0, 35, 5),
#                      labels = function(x) paste0(x, " km")) +
#   theme_pander(base_size = 30, base_family = "lato") +
#   labs(title = "",
#        x = "",
#        y = "") +
#   theme(plot.title = element_text(hjust = 0, face = "bold"),
#         axis.text.x = element_text(size = 18),
#         legend.title = element_blank(),
#         legend.position = "none") 
# 
# ##
# plot_histograma_cbd_t1 <-
#   st_drop_geometry(alvaras_por_lote) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   filter(legislacao %in% c("PDE2014eLPUOS2016")) %>%
#   ggplot(aes(distancia_cbd, ..count.., 
#              fill = categoria_de_uso_grupo, shape = categoria_de_uso_grupo)) +
#   geom_histogram(position = "stack", color = "black", alpha = .9, 
#                  breaks = seq(0, 30, 1)) +
#   scale_y_continuous(breaks = seq(0, 300, 20),
#                      limits = c(0, 80)) +
#   scale_x_continuous(breaks = seq(0, 35, 5),
#                      labels = function(x) paste0(x, " km")) +
#   theme_pander(base_size = 30, base_family = "lato") +
#   labs(title = "",
#        caption = "*Obs: Centro = Praça da Sé* <br>",
#        x = "",
#        y = "") +
#   theme(plot.title = element_text(hjust = 0, face = "bold"),
#         axis.text.x = element_text(size = 18),
#         legend.title = element_blank(),
#         legend.position = "none",
#         plot.caption = element_markdown(size = 18)) 



# 10. Acessibilidade ---------------------------------------------------------


## Distribuição dos empreendimentos em EETU's em função de sua acessibilidade (regramento aplicado)

# Gerar gráfico como o abaixo mostrando a localização dos ERM e ERP em função da distância ao centro para pré-PDE e pós-PDE 
#
# plot_densidade_acessibilidade_t0 <-
#   st_drop_geometry(alvaras_por_lote) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   filter(legislacao %in% c("PDE2002eLPUOS2004")) %>%
#   ggplot(aes(log(acessibilidade_empregos), ..density.., group = categoria_de_uso_grupo, fill = categoria_de_uso_grupo)) +
#   geom_density(alpha = .9) +
#   scale_x_continuous(breaks = seq(9, 12, 1),
#                      limits = c(8, 12.62),
#                      labels = c("- acessibilidade", "", "",
#                                 "+ acessibilidade")) +
#   coord_trans(x = "reverse") +
#   scale_y_continuous(limits = c(0,1.25),
#                      breaks = breaks_pretty(n = 4)) +  theme_pander(base_size = 30, base_family = "lato") +
#   labs(title = "",
#        subtitle = "PDE2002 e LPUOS2004",
#        x = "",
#        y = "") +
#   theme(plot.subtitle = element_text(size = 24, hjust = 0.5),
#         axis.ticks = element_line(colour = "black"),
#         axis.line = element_line(colour = "black"),
#         axis.text.x = element_text(size = 18),
#         legend.title = element_blank(),
#         legend.position = "none") 
# 
# #
# plot_densidade_acessibilidade_t1 <-
#   st_drop_geometry(alvaras_por_lote) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   filter(legislacao %in% c("PDE2014eLPUOS2016")) %>%
#   ggplot(aes(log(acessibilidade_empregos), ..density.., group = categoria_de_uso_grupo, fill = categoria_de_uso_grupo)) +
#   geom_density(alpha = .9) +
#   scale_x_continuous(breaks = seq(9, 12, 1),
#                      limits = c(8, 12.62),
#                      labels = c("- acessibilidade", "", "",
#                                 "+ acessibilidade")) +
#   scale_y_continuous(limits = c(0,1.25),
#                      breaks = pretty_breaks(n = 4)) +
#   coord_trans(x = "reverse") +
#   theme_pander(base_size = 30, base_family = "lato") +
#   labs(title = "",
#        subtitle = "PDE2014 e LPUOS2016",
#        x = "",
#        y = "") +
#   theme(plot.subtitle = element_text(size = 24, hjust = 0.5),
#         axis.ticks = element_line(colour = "black"),
#         axis.line = element_line(colour = "black"),
#         axis.text.x = element_text(size = 18),
#         legend.title = element_blank(),
#         legend.text = element_text(size = 18),
#         legend.position = c(0.8, 0.6)) 
# 
# ##
# plot_histograma_acessibilidade_t0 <-
#   st_drop_geometry(alvaras_por_lote) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   filter(legislacao %in% c("PDE2002eLPUOS2004")) %>%
#   ggplot(aes(log(acessibilidade_empregos), ..count.., 
#              fill = categoria_de_uso_grupo, shape = categoria_de_uso_grupo)) +
#   geom_histogram(position = "stack", color = "black", alpha = .9#, 
#                  #breaks = seq(0, 30, 1)
#   ) +
#   scale_y_continuous(breaks = seq(0, 80, 20),
#                      limits = c(0, 90)) +
#   scale_x_continuous(breaks = seq(9, 12, 1),
#                      limits = c(8, 12.62),
#                      labels = c("- acessibilidade", "", "",
#                                 "+ acessibilidade"),
#   ) +
#   coord_trans(x = "reverse") +
#   theme_pander(base_size = 24, base_family = "lato") +
#   labs(title = "",
#        x = "",
#        y = "") +
#   theme(plot.title = element_text(hjust = 0, face = "bold"),
#         axis.ticks = element_line(colour = "black"),
#         axis.line = element_line(colour = "black"),
#         axis.text.x = element_text(size = 18),
#         legend.title = element_blank(),
#         legend.position = "none") 
# 
# ##
# plot_histograma_acessibilidade_t1 <-
#   st_drop_geometry(alvaras_por_lote) %>%
#   filter(zoneamento_grupo == "EETU") %>%
#   filter(legislacao %in% c("PDE2014eLPUOS2016")) %>%
#   ggplot(aes(log(acessibilidade_empregos), ..count.., 
#              fill = categoria_de_uso_grupo, shape = categoria_de_uso_grupo)) +
#   geom_histogram(position = "stack", color = "black", alpha = .9#, 
#                  #breaks = seq(0, 30, 1)
#   ) +
#   scale_y_continuous(breaks = seq(0, 80, 20),
#                      limits = c(0, 90)) +
#   scale_x_continuous(breaks = seq(9, 12, 1),
#                      limits = c(8, 12.62),
#                      labels = c("- acessibilidade", "", "",
#                                 "+ acessibilidade"),
#   ) +
#   coord_trans(x = "reverse") +
#   theme_pander(base_size = 24, base_family = "lato") +
#   labs(title = "",
#        # caption = "*Obs: Centro = Praça da Sé* <br>",
#        x = "",
#        y = "") +
#   theme(plot.title = element_text(hjust = 0, face = "bold"),
#         axis.ticks = element_line(colour = "black"),
#         axis.line = element_line(colour = "black"),
#         axis.text.x = element_text(size = 18),
#         legend.title = element_blank(),
#         legend.position = "none",
#         plot.caption = element_markdown(size = 18)) 


# 11. Transporte ------------------------------------------------------------

## Evolução do número de empreendimentos licenciados por tipo de Eixo de Adensamento (2013-2021)

# Gráfico mostrando qual infraestrutura atraiu mais empreendimentos ao longo de todo o período 
plot_eetu_empreendimentos_por_transporte <- alvaras_zonas_eetu_por_lote %>%
  st_drop_geometry() %>%
  filter(!is.na(tipo_transp)) %>%
  filter(between(ano_execucao, 2013, 2021)) %>%
  group_by(ano_execucao, tipo_transp) %>%
  summarize(n = n_distinct(id))


# 12. Tranporte por categoria --------------------------------------------------


## Evolução do número de empreendimentos licenciados por tipo de Eixo de Adensamento e por categoria (2013-2021)

# Gráfico mostrando qual infraestrutura atraiu mais empreendimentos de ERM e ERP ao longo de todo o período 

#
# plot_eetu_empreendimentos_por_transporte_categoria <- alvaras_zonas_eetu_por_lote %>%
#   st_drop_geometry() %>%
#   filter(!is.na(tipo_transp)) %>%
#   filter(between(ano_execucao, 2013, 2021)) %>%
#   group_by(ano_execucao, categoria_de_uso_grupo, tipo_transp) %>%
#   summarize(n_alvaras = n_distinct(id))

# 13. Infraestruturas  --------------------------------------------------------

## Infraestruturas de transporte de referência por densidade de unidades residenciais lançadas a partir dos atuais PDE e LPUOS (2014-2021)

# Gráfico identificando as estações que mais atraíram. Desse gráfico podemos retirar a identificação de metrô/trem/Onibus e incluir informação da porcentagem de ERPs, similar aos gráfico da NT da MEM 

plot_eetu_unidades_area_top_30 <- alvaras_zonas_eetu_por_eixo %>%
  st_drop_geometry() %>%
  filter(!is.na(eixo)) %>%
  top_n(30, n_unidades_por_area) %>%
  transmute(eixo = paste0(tipo_infra, " ", eixo),
            n_unidades_por_area,
            percent_erp)


# 14. Mapa Eixos por densidade de unidades -------------------------------------

#
plot_map_eixos_unidades_area_top_10 <- 
    st_as_sf(
    alvaras_zonas_eetu_por_eixo %>%
      st_drop_geometry() %>%
      filter(!is.na(tipo_transp)) %>%
      mutate(rank = dense_rank(desc(n_unidades_por_area)),
             is_top_10 = if_else(rank <= 10, TRUE, FALSE)),
    crs = CRS,
    coords = c("X", "Y"))


# 15. Infraestrutura por densidade de unidades ERM -----------------------------
## Infraestruturas de transporte de referência por densidade de unidades residenciais ERM lançadas a partir dos atuais PDE e LPUOS (2014-2021)

# Gráfico identificando as estações que mais atraíram. Desse gráfico podemos retirar a identificação de metrô/trem/Onibus e incluir informação da porcentagem de ERPs, similar aos gráfico da NT da MEM 
# plot_eetu_erm_area_top_30 <- alvaras_zonas_eetu_por_eixo %>%
#   st_drop_geometry() %>%
#   filter(!is.na(tipo_transp)) %>%
#   top_n(30, n_erm_por_area) %>%
#   transmute(tipo_transp,
#             eixo = paste0(tipo_infra, " ", eixo),
#             n_erm_por_area)

# 16. Infraestrutura por densidade de unidades ERP -----------------------------

## Infraestruturas de transporte de referência por densidade de unidades residenciais ERP lançadas a partir dos atuais PDE e LPUOS (2014-2021)
# plot_eetu_erp_area_top_30 <- alvaras_zonas_eetu_por_eixo %>%
#   st_drop_geometry() %>%
#   filter(!is.na(tipo_transp)) %>%
#   top_n(30, n_erp_por_area) %>%
#   transmute(tipo_transp,
#             eixo = paste0(tipo_infra, " ", eixo),
#             n_erp_por_area)

# 17. Diferencial em função do CBD ---------------------------------------------
## Diferencial de unidades licenciadas em Eixos em função da distância ao centro - regramento anterior vs regramento atual)
plot_eetu_diferencial_legislacao_cbd <-
  alvaras_zonas_eetu_por_eixo_por_legislacao %>%
  filter(!is.na(tipo_transp)) %>%
  complete(legislacao, eixo) %>%
  group_by(eixo) %>%
  mutate(tipo_transp = first(na.omit(tipo_transp)),
         distancia_cbd = mean(distancia_cbd, na.rm = TRUE)) %>%
  filter(legislacao %in% c("PDE2014eLPUOS2016", "PDE2002eLPUOS2004")) %>%
  group_by(legislacao, tipo_transp, eixo, distancia_cbd) %>%
  summarize(n_unidades = sum(n_unidades_eixo, na.rm = TRUE)) %>%
  ungroup() %>%
  pivot_wider(names_from = "legislacao",
              values_from = "n_unidades") %>%
  mutate(across(where(is.numeric), ~ replace_na(.x, 0)),
         diff_unidades = `PDE2014eLPUOS2016` - `PDE2002eLPUOS2004`)


# 18. Diferencial em função da acessibilidade --------------------------------

## Diferencial de unidades licenciadas em Eixos em função da acessibilidade de empregos - regramento anterior vs regramento atual)
# plot_eetu_diferencial_legislacao_acessibilidade <- alvaras_zonas_eetu_por_eixo_por_legislacao %>%
#   filter(!is.na(tipo_transp)) %>%
#   complete(legislacao, eixo) %>%
#   group_by(eixo) %>%
#   mutate(tipo_transp = first(na.omit(tipo_transp)),
#          acessibilidade_empregos = mean(acessibilidade_empregos, na.rm = TRUE)) %>%
#   filter(legislacao %in% c("PDE2014eLPUOS2016", "PDE2002eLPUOS2004")) %>%
#   group_by(legislacao, tipo_transp, eixo, acessibilidade_empregos) %>%
#   summarize(n_unidades = sum(n_unidades_eixo, na.rm = TRUE)) %>%
#   ungroup() %>%
#   pivot_wider(names_from = "legislacao",
#               values_from = "n_unidades") %>%
#   mutate(across(where(is.numeric), ~ replace_na(.x, 0)),
#          diff_unidades = `PDE2014eLPUOS2016` - `PDE2002eLPUOS2004`)

# 19. Terreno incorporado ----------------------------------------------------

# ## Evolução da área de terreno por regramento selecionado (2013-2021) 
# 
# plot_potencial_area_zoneamento <-
#   zonas_eetu_potencial %>%
#   st_drop_geometry() %>%
#   filter(!is.na(tipo_eixo)) %>%
#   # mutate(TipoEixo = if_else(TipoEixo == "Decreto", "EETU", TipoEixo)) %>%
#   group_by(tipo_eixo) %>%
#   summarize(
#     area_zoneamento = round(sum(area_zoneamento, na.rm = TRUE)/1000000, 1))
# 
# #
# plot_potencial_area_terreno_acum <- 
#   alvaras_por_lote %>%
#   st_drop_geometry() %>%
#   filter(ano_execucao >=2013) %>%
#   # filter(!is.na(porte)) %>%
#   filter(local != "Outro") %>%
#   group_by(ano_execucao, local) %>%
#   summarize(
#     area_do_terreno = sum(area_do_terreno/1000000, na.rm = TRUE)
#   ) %>%
#   ungroup() %>%
#   group_by(local) %>%
#   mutate(acum_area_do_terreno = cumsum(area_do_terreno)) %>%
#   ungroup() %>%
#   # group_by(ano_execucao) %>%
#   mutate(total = sum(area_do_terreno, na.rm = TRUE))


# Outbound -------------------------------------------------------------

#
fs::dir_create(here("inputs", "3_portal_dados", 
                    #"Projetos", "AcessoOportunidadesPDE", 
                    "Historias", "01_EixoNaoETudoIgual"))

# Gesoerver exige formato geospeacial
# Procedimento de criar uma dummy espacial para contornar o problema
list_outputs <- mget(ls(.GlobalEnv, pattern = "plot_"), envir = .GlobalEnv) %>%
  map_if(~ inherits(.x, "sf"), ~ st_transform(.x, crs = "EPSG:4326")) %>%
  map_if(~ !inherits(.x, "sf"), ~ .x %>% mutate(across(everything(), as.character))) %>%
  map_if(~ !inherits(.x, "sf"), ~ .x %>% mutate(X = 0, Y = 0)) %>%
  map_if(~ !inherits(.x, "sf"), ~ st_as_sf (.x, coords = c("X", "Y")))

#
# list_outputs %>%
#   map2(names(.), 
#        # ~sfarrow::st_write_parquet(.x,
#        #                            here("outputs", "Eixos", .y,
#        #                                 paste0(.y, ".parquet")))
#        ~ write_csv(.x,
#                  here("inputs", "3_portal_dados", "Historias", 
#                       "1_EixoNaoETudoIgual", paste0(.y, ".csv")))
#   )

list_outputs %>%
  map2(names(.), 
       ~sfarrow::st_write_parquet(.x,
                                  here("inputs", "3_portal_dados", "Historias", 
                                       "01_EixoNaoETudoIgual", paste0(.y, ".parquet")))
  )

