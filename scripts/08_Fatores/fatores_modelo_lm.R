# ANÁLISE MODELAGEM - DETERMINANTES DO MERCADO IMOBILIÁRIO SP
# FORMATO 1: ÁREA DE PONDERAÇÃO E REGRESSÃO LINEAR MÚLTIPLA

library(here)
library(sf)
library(raster)
library(tidyverse)
library(tidylog)
library(geobr) #devtools::install_github'('"ipeaGIT/geobr", subdir = "r-package")
library(DataExplorer)
library(skimr)
library(broom)
library(stargazer)
library(ggplot2)
library(ggcorrplot)


# 1. Importação -----------------------------------------------------------

#
alvaras_por_unidade_pre_pde <- arrow::read_parquet(here("inputs", "2_trusted", "Fatores", 
                            "fatores_por_area_ponderacao_pre_pde.parquet")) %>%
  dplyr::mutate(zoneamento_grupo = fct_relevel(zoneamento_grupo, "Outros", after = 0))

#
alvaras_por_unidade_pos_pde <-arrow::read_parquet(here("inputs", "2_trusted", "Fatores", 
                                                       "fatores_por_area_ponderacao_pos_pde.parquet")) %>%
  dplyr::mutate(zoneamento_grupo = fct_relevel(zoneamento_grupo, "Outros", after = 0))

# 2. Analisa correlações ------------------------------------------

# A - Pŕe PDE 2014

##
alvaras_por_unidade_pre_pde %>%
  skim()

##
alvaras_por_unidade_pre_pde %>%
  plot_missing()

# Correlações x ~ x
## Correlação alta: renda_per_capita, n_empregos_por_km2, n_comercios_por_km2 e acessibilidade empregos
alvaras_por_unidade_pre_pde %>% 
  keep(is.numeric) %>%
  dplyr::select(-c(area, n_empreendimentos_por_km2,n_empreendimentos_erm_por_km2,
                   n_empreendimentos_erp_por_km2,n_unidades_por_km2,
                   n_unidades_erm_por_km2,n_unidades_erp_por_km2)) %>%
  plot_correlation(cor_args = list(use = "pairwise.complete.obs"),
                   ggtheme = list(theme_minimal(base_family = "", 
                                                base_size = 12)),
                   theme_config = list(legend.position = "none",
                                       axis.title = element_blank(),
                                       axis.text.x = element_text(angle = 10)))

# Correlações y ~ x
cor_alvaras_y_pre_pde <- 
  tibble(
    var = c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2","distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
    n_empreendimentos_erm_por_km2 = map_dbl(
      c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2","distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
      ~ cor(alvaras_por_unidade_pre_pde["n_empreendimentos_erm_por_km2"],
            alvaras_por_unidade_pre_pde[.x], use = "complete.obs")
    ),
    n_empreendimentos_erp_por_km2 = map_dbl(
      c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2","distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
      ~ cor(alvaras_por_unidade_pre_pde["n_empreendimentos_erp_por_km2"],
            alvaras_por_unidade_pre_pde[.x], use = "complete.obs")
    ),
    n_unidades_erm_por_km2 = map_dbl(
      c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2","distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
      ~ cor(alvaras_por_unidade_pre_pde["n_unidades_erm_por_km2"],
            alvaras_por_unidade_pre_pde[.x], use = "complete.obs")
    ),
    n_unidades_erp_por_km2 = map_dbl(
      c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2", "distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
      ~ cor(alvaras_por_unidade_pre_pde["n_unidades_erp_por_km2"],
            alvaras_por_unidade_pre_pde[.x], use = "complete.obs")
    )
  )

#
cor_alvaras_y_pre_pde

# B - Pós PDE 2014

##
alvaras_por_unidade_pos_pde %>%
  skim()

##
alvaras_por_unidade_pos_pde %>%
  plot_missing()


# Correlações x ~ x
## Correlação alta: renda_per_capita, n_empregos_por_km2, n_comercios_por_km2 e acessibilidade empregos
alvaras_por_unidade_pos_pde %>% 
  keep(is.numeric) %>%
  dplyr::select(-c(area, n_empreendimentos_por_km2,n_empreendimentos_erm_por_km2, 
                   n_empreendimentos_erp_por_km2,n_unidades_por_km2, 
                   n_unidades_erm_por_km2,n_unidades_erp_por_km2)) %>%
  plot_correlation(cor_args = list(use = "pairwise.complete.obs"),
                   ggtheme = list(theme_minimal(base_family = "", 
                                                base_size = 12)),
                   theme_config = list(legend.position = "none",
                                       axis.title = element_blank(),
                                       axis.text.x = element_text(angle = 10)))


# Correlações y ~ x
cor_alvaras_y_pos_pde <- 
  tibble(
    var = c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2",
            "distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
    n_empreendimentos_erm_por_km2 = map_dbl(
      c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2",             "distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
      ~ cor(alvaras_por_unidade_pos_pde["n_empreendimentos_erm_por_km2"],
            alvaras_por_unidade_pos_pde[.x], use = "complete.obs")
    ),
    n_empreendimentos_erp_por_km2 = map_dbl(
      c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2",             "distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
      ~ cor(alvaras_por_unidade_pos_pde["n_empreendimentos_erp_por_km2"],
            alvaras_por_unidade_pos_pde[.x], use = "complete.obs")
    ),
    n_unidades_erm_por_km2 = map_dbl(
      c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2",             "distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
      ~ cor(alvaras_por_unidade_pos_pde["n_unidades_erm_por_km2"],
            alvaras_por_unidade_pos_pde[.x], use = "complete.obs")
    ),
    n_unidades_erp_por_km2 = map_dbl(
      c("percent_solo_disponivel","n_empregos_por_km2","n_equipamentos_por_km2",             "distancia", "densidade_pop","renda_per_capita", "perc_com_esgoto"),
      ~ cor(alvaras_por_unidade_pos_pde["n_unidades_erp_por_km2"],
            alvaras_por_unidade_pos_pde[.x], use = "complete.obs")
    )
  )
#
cor_alvaras_y_pos_pde


# 3. Estabelece parâmetros para Regressão Linear Múltipla ------------------------

#
lm_ajust <- function(df, response_var, explanatory_vars, log.transf = c(FALSE, TRUE), ...){
  df <- df
  response_var <- response_var
  explanatory_vars <- paste(explanatory_vars, collapse = " + ")
  
  if(log.transf == TRUE){
    formula1 <- as.formula(paste("log(", response_var, ") ~ ", explanatory_vars ))
    lm(formula = formula1,
       data = df %>%
         filter_at(response_var, ~ . != 0),
       ...)
  } else {
    formula1 <- as.formula(paste(response_var, " ~ ", explanatory_vars))
    lm(formula = formula1,
       data = df,
       ...)   
  }
}

#
response_vars <-
  list("n_empreendimentos_por_km2", "n_empreendimentos_erm_por_km2", 
       "n_empreendimentos_erp_por_km2","n_unidades_por_km2", 
       "n_unidades_erm_por_km2", "n_unidades_erp_por_km2"
  ) %>%
  set_names()

#Para não zerar o log
#alvaras_por_unidade_pos_pde$n_empreendimentos_erm_por_km2[alvaras_por_unidade_pos_pde$n_empreendimentos_erm_por_km2==0]<-min(alvaras_por_unidade_pos_pde$n_empreendimentos_por_km2,na.rm = T)
#alvaras_por_unidade_pos_pde$n_empreendimentos_erp_por_km2[alvaras_por_unidade_pos_pde$n_empreendimentos_erp_por_km2==0]<-min(alvaras_por_unidade_pos_pde$n_empreendimentos_por_km2,na.rm = T)
#alvaras_por_unidade_pos_pde$n_unidades_erm_por_km2[alvaras_por_unidade_pos_pde$n_unidades_erm_por_km2==0]<-min(alvaras_por_unidade_pos_pde$n_unidades_por_km2[alvaras_por_unidade_pos_pde$n_unidades_por_km2!=0],na.rm = T)
#alvaras_por_unidade_pos_pde$n_unidades_erp_por_km2[alvaras_por_unidade_pos_pde$n_unidades_erp_por_km2==0]<-min(alvaras_por_unidade_pos_pde$n_unidades_por_km2[alvaras_por_unidade_pos_pde$n_unidades_por_km2==0],na.rm = T)


# FORMA FUNCIONAL 1
explanatory_vars1 <- 
  c("zoneamento_grupo","percent_solo_disponivel","log(area_lote_disponivel)"
    # ,"ca_total"
    # ,"log(n_comercios_por_km2)"
    # ,"log(n_empregos_por_km2)"
    # ,"log(acessibilidade_empregos)"
    # ,"log(n_equipamentos_por_km2)"
    # ,"distancia", "densidade_pop"
    # ,"log(renda_per_capita)"
    # ,"perc_com_esgoto"
  )

# FORMA FUNCIONAL 2
explanatory_vars2 <- 
  c("zoneamento_grupo","percent_solo_disponivel","log(area_lote_disponivel)"
    # ,"ca_total"
    # ,"log(n_comercios_por_km2)"
    # ,"log(n_empregos_por_km2)"
    # ,"log(acessibilidade_empregos)"
    # ,"log(n_equipamentos_por_km2)"
    ,"distancia", "densidade_pop"
    ,"log(renda_per_capita)"
    ,"perc_com_esgoto"
  )

# FORMA FUNCIONAL 3
explanatory_vars3 <- 
  c("zoneamento_grupo","percent_solo_disponivel","log(area_lote_disponivel)"
    # ,"ca_total"
    # ,"log(n_comercios_por_km2)"
    ,"log(n_empregos_por_km2)"
    # "log(acessibilidade_empregos)"
    ,"log(n_equipamentos_por_km2)"
    ,"distancia", "densidade_pop"
    #,"log(renda_per_capita)"
    ,"perc_com_esgoto"
  )

# FORMA FUNCIONAL 4
explanatory_vars4 <- 
  c("zoneamento_grupo","percent_solo_disponivel","log(area_lote_disponivel)"
    # ,"ca_total"
    #,"log(n_comercios_por_km2)"
    # ,"log(n_empregos_por_km2)"
    ,"log(acessibilidade_empregos)"
    ,"log(n_equipamentos_por_km2)"
    ,"distancia", "densidade_pop"
    #,"log(renda_per_capita)"
    ,"perc_com_esgoto"
  )


# FORMA FUNCIONAL 5
explanatory_vars5 <- 
  c("zoneamento_grupo"
    #,"percent_solo_disponivel","log(area_lote_disponivel)"
    #,"ca_total"
    #,"log(n_comercios_por_km2)"
    #,"log(n_empregos_por_km2)"
    #,"log(acessibilidade_empregos)"
    #,"log(n_equipamentos_por_km2)"
    ,"distancia", "densidade_pop"
    ,"log(renda_per_capita)"
    ,"perc_com_esgoto"
  )

# FORMA FUNCIONAL 6
explanatory_vars6 <- 
  c("zoneamento_grupo"
    #"percent_solo_disponivel","log(area_lote_disponivel)"
    #,"ca_total"
    #,"log(n_comercios_por_km2)"
    ,"log(n_empregos_por_km2)"
    #,"log(acessibilidade_empregos)"
    ,"log(n_equipamentos_por_km2)"
    ,"distancia", "densidade_pop"
    #,"log(renda_per_capita)"
    ,"perc_com_esgoto"
  )

# FORMA FUNCIONAL 7
explanatory_vars7 <- 
  c("zoneamento_grupo"
    #"percent_solo_disponivel","log(area_lote_disponivel)"
    #,"ca_total"
    #,"log(n_comercios_por_km2)"
    #,"log(n_empregos_por_km2)"
    ,"log(acessibilidade_empregos)"
    ,"log(n_equipamentos_por_km2)"
    ,"distancia", "densidade_pop"
    #,"log(renda_per_capita)"
    ,"perc_com_esgoto"
  )

# FORMA FUNCIONAL 8
explanatory_vars8 <- 
  c("zoneamento_grupo"
    #"percent_solo_disponivel","log(area_lote_disponivel)"
    ,"ca_total"
    #,"log(n_comercios_por_km2)"
    #,"log(n_empregos_por_km2)"
    ,"log(acessibilidade_empregos)"
    ,"log(n_equipamentos_por_km2)"
    ,"distancia", "densidade_pop"
    #,"log(renda_per_capita)"
    ,"perc_com_esgoto"
  )


# 4. Aplica Regressão Linear Múltipla - Pré PDE 2014 ------------------------

#
m1pre <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pre_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars1,
                 log.transf = TRUE))
#
m2pre <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pre_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars2, 
                 log.transf = TRUE))

#
m3pre <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pre_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars3, 
                 log.transf = TRUE))
#
m4pre <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pre_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars4, 
                 log.transf = TRUE))
#
m5pre <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pre_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars5, 
                 log.transf = TRUE))

#
m6pre <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pre_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars6, 
                 log.transf = TRUE))

m7pre <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pre_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars7, 
                 log.transf = TRUE))

m8pre <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pre_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars8, 
                 log.transf = TRUE))


# 5. Aplica Regressão Linear Múltipla - Pós PDE 2014 ------------------------

#
m1 <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pos_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars1,
                 log.transf = TRUE))
#
m2 <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pos_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars2, 
                 log.transf = TRUE))

#
m3 <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pos_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars3, 
                 log.transf = TRUE))

#
m4 <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pos_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars4, 
                 log.transf = TRUE))

#
m5 <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pos_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars5, 
                 log.transf = TRUE))

#
m6 <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pos_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars6, 
                 log.transf = TRUE))

#
m7 <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pos_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars7, 
                 log.transf = TRUE))

#
m8 <- response_vars %>%
  map(~ lm_ajust(df = alvaras_por_unidade_pos_pde, 
                 response_var = .x, explanatory_vars = explanatory_vars8, 
                 log.transf = TRUE))

# 6. Analisa - Pré PDE 2014 ------------------------


# Mapeia a função summary
m1pre %>%
  map(summary)

#
m2pre %>%
  map(summary)

#
m3pre %>%
  map(summary)

#
m4pre %>%
  map(summary)

#
m5pre %>%
  map(summary)

#
m6pre %>%
  map(summary)

m7pre %>%
  map(summary)

m8pre %>%
  map(summary)

# A) Pre PDE

# Compara diferentes formas funcionais
stargazer(
  list(m1pre[1], m2pre[1],
       m3pre[1], m4pre[1]),
  type = "text",
  keep.stat = c("n", "rsq"))

#
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_pre_pde.html"))
stargazer(
  list(m1pre[1], m2pre[1],
       m3pre[1], m4pre[1]),
  title="Resultado Modelo Regressão Empreendimentos", type="html", df=FALSE, digits=4,
  dep.var.caption = "Alvarás entre 2014 e 2021 | PDE2014 & LPUOS2016",
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio", "Área de Solo Disponível","Fragmentação do Solo Disponível",
                     "log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()

# B) Pre PDE - ERM's
#
stargazer(
  list(m1pre[4], m2pre[4],
       m3pre[4], m4pre[4]),
  type = "text",
  keep.stat = c("n", "rsq"))

#
stargazer(list(m1pre[2], m2pre[2],
               m3pre[2], m4pre[2]),
          type = "text",
          keep.stat = c("n", "rsq"))

##
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_erm_pre_pde.html"))
stargazer(
  list(m1pre[2], m2pre[2],
       m3pre[2], m4pre[2]),
  title="Resultado Modelo Regressão Empreendimentos ERM", type="html", 
  df=FALSE, digits=4, dep.var.caption = "Alvarás entre 2014 e 2021 | PDE2014 & LPUOS2016", 
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio", "Área de Solo Disponível","Fragmentação do Solo Disponível",
                     "log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()


# C) Pre PDE - ERP's
#
stargazer(
  list(m1pre[3], m2pre[3],
       m3pre[3], m4pre[3]),
  type = "text",
  keep.stat = c("n", "rsq"))

##
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_erp_pre_pde.html"))
stargazer(
  list(m1pre[3], m2pre[3],
       m3pre[3], m4pre[3]),
  title="Resultado Modelo Regressão Empreendimentos ERP", type="html", df=FALSE, digits=4, 
  dep.var.caption = "Alvarás entre 2013 e 2021", 
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio", "Área de Solo Disponível","Fragmentação do Solo Disponível",
                     "log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()

# D) Pre PDE - Unidades
stargazer(
  list(m5pre[4], m6pre[4],
       m7pre[4], m8pre[4]),
  type = "text",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  keep.stat = c("n", "rsq"))

##
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_por_uh_pre_pde.html"))
stargazer(
  list(m5pre[4], m6pre[4],
       m7pre[4], m8pre[4]),
  title="Resultado Modelo Regressão Unidades", type="html", df=FALSE, digits=4,
  dep.var.caption = "Alvarás entre 2013 e 2021",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio",
                     "CA máximo","log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()



# E) Pre PDE - Unidades ERM
#
stargazer(
  list(m5pre[5], m6pre[5],
       m7pre[5], m8pre[5]),
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  type = "text",
  keep.stat = c("n", "rsq"))

##
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_erm_por_uh_pre_pde.html"))
stargazer(
  list(m5pre[5], m6pre[5],
       m7pre[5], m8pre[5]),
  title="Resultado Modelo Regressão Unidades ERM", type="html", 
  df=FALSE, digits=4, dep.var.caption = "Alvarás entre 2013 e 2021",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio",
                     "CA máximo","log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()


# F) Pre PDE - Unidades ERP
#
stargazer(
  list(m5pre[6], m6pre[6],
       m7pre[6], m8pre[6]),
  type = "text",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  keep.stat = c("n", "rsq"))

##
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_erp_por_uh_pre_pde.html"))
stargazer(
  list(m5pre[6], m6pre[6],
       m7pre[6], m8pre[6]),
  title="Resultado Modelo Regressão Unidades ERP", type="html", df=FALSE, digits=4, 
  dep.var.caption = "Alvarás entre 2013 e 2021",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio",
                     "CA máximo","log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()

# G) Correlações
#
cor(alvaras_por_unidade_pre_pde$n_unidades_erm_por_km2,
    alvaras_por_unidade_pre_pde$densidade_pop,use = "complete.obs")
cor(alvaras_por_unidade_pre_pde$n_unidades_erp_por_km2,
    alvaras_por_unidade_pre_pde$densidade_pop,use = "complete.obs")

cor(alvaras_por_unidade_pre_pde$n_unidades_erm_por_km2,
    alvaras_por_unidade_pre_pde$distancia,use = "complete.obs")
cor(alvaras_por_unidade_pre_pde$n_unidades_erp_por_km2,
    alvaras_por_unidade_pre_pde$distancia,use = "complete.obs")

cor(alvaras_por_unidade_pre_pde$n_unidades_erm_por_km2,
    alvaras_por_unidade_pre_pde$renda_per_capita,use = "complete.obs")
cor(alvaras_por_unidade_pre_pde$n_unidades_erp_por_km2,
    alvaras_por_unidade_pre_pde$renda_per_capita,use = "complete.obs")

cor(alvaras_por_unidade_pre_pde$n_unidades_erm_por_km2,
    alvaras_por_unidade_pre_pde$perc_com_esgoto,use = "complete.obs")
cor(alvaras_por_unidade_pre_pde$n_unidades_erp_por_km2,
    alvaras_por_unidade_pre_pde$perc_com_esgoto,use = "complete.obs")


# 7. Analisa - Pós PDE 2014 ------------------------

# Mapeia a função summary
m1 %>%
  map(summary)

#
m2 %>%
  map(summary)

#
m3 %>%
  map(summary)

#
m4 %>%
  map(summary)

#
m5 %>%
  map(summary)

#
m6 %>%
  map(summary)

#
m7 %>%
  map(summary)

#
m8 %>%
  map(summary)


# A) Pós PDE

# Compara diferentes formas funcionais
stargazer(
  list(m1[1], m2[1],
       m3[1], m4[1]),
  type = "text",
  keep.stat = c("n", "rsq"))

#
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_pos_pde.html"))
stargazer(
  list(m1[1], m2[1],
       m3[1], m4[1]),
  title="Resultado Modelo Regressão Empreendimentos", type="html", df=FALSE, digits=4,
  dep.var.caption = "Alvarás entre 2013 e 2021",
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio", "Área de Solo Disponível","Fragmentação do Solo Disponível",
                     "log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()

# B) Pós PDE - ERM's

#
stargazer(list(m1[2], m2[2],
               m3[2], m4[2]),
          type = "text",
          keep.stat = c("n", "rsq"))

##
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_erm_pos_pde.html"))
stargazer(
  list(m1[2], m2[2],
       m3[2], m4[2]),
  title="Resultado Modelo Regressão Empreendimentos ERM", type="html", 
  df=FALSE, digits=4, dep.var.caption = "Alvarás entre 2013 e 2021", 
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio", "Área de Solo Disponível","Fragmentação do Solo Disponível",
                     "log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()

# C) Pos PDE - ERP's
#
stargazer(
  list(m1[3], m2[3],
       m3[3], m4[3]),
  type = "text",
  keep.stat = c("n", "rsq"))

##
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_erp_pos_pde.html"))
stargazer(
  list(m1[3], m2[3],
       m3[3], m4[3]),
  title="Resultado Modelo Regressão Empreendimentos ERP", type="html", df=FALSE, digits=4, 
  dep.var.caption = "Alvarás entre 2013 e 2021", 
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio", "Área de Solo Disponível","Fragmentação do Solo Disponível",
                     "log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()

# D) Pos PDE - Unidades
stargazer(
  list(m5[4], m6[4],
       m7[4], m8[4]),
  type = "text",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  keep.stat = c("n", "rsq"))


#
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_por_uh_pos_pde.html"))
stargazer(
  list(m5[4], m6[4],
       m7[4], m8[4]),
  title="Resultado Modelo Regressão Unidades", type="html", df=FALSE, digits=4,
  dep.var.caption = "Alvarás entre 2013 e 2021", 
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio",
                     "CA máximo","log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()

# E) Pos PDE - Unidades ERM
#
stargazer(
  list(m5[5], m6[5],
       m7[5], m8[5]),
  type = "text",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  keep.stat = c("n", "rsq"))

##
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_erm_por_uh_pos_pde.html"))
stargazer(
  list(m5[5], m6[5],
       m7[5], m8[5]),
  title="Resultado Modelo Regressão Unidades ERM", type="html", 
  df=FALSE, digits=4, dep.var.caption = "Alvarás entre 2013 e 2021",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio",
                     "CA máximo","log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()

# F) Pos PDE - Unidades ERP

#
stargazer(
  list(m5[6], m6[6],
       m7[6], m8[6]),
  type = "text",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  keep.stat = c("n", "rsq"))

##
sink(here("outputs", "Exploratoria", "Fatores", "modelo_lm_fatores_erp_por_uh_pos_pde.html"))
stargazer(
  list(m5[6], m6[6],
       m7[6], m8[6]),
  title="Resultado Modelo Regressão Unidades ERP", type="html", df=FALSE, digits=4, 
  dep.var.caption = "Alvarás entre 2013 e 2021",
  order=c(1,2,3,4,5,7,6,8,9,10,11,12,13),
  covariate.labels=c("Zona EETU", "Zona EETU Futuros","Zona ZC e ZMs","Zona ZEIS Aglomerado", 
                     "Zona ZEIS Vazio",
                     "CA máximo","log(empregos/km2)","log(acesso a empregos)", 
                     "log(equipamentos/km2)", "Distância ao CBD", 
                     "Densidade populacional", "log(renda per capita)", 
                     "Cobertura esgoto"))
sink()


# 8. Exporta --------------------------------------------------------------

#
write_csv(cor_alvaras_y_pre_pde,
          here("outputs", "Exploratoria", "Fatores",
               "cor_alvaras_vs_fatores_pre_pde.csv"))

#
write_csv(cor_alvaras_y_pos_pde,
          here("outputs", "Exploratoria", "Fatores",
               "cor_alvaras_vs_fatores_pos_pde.csv"))

