library(here)
library(tidyverse)
library(tidylog)
library(sf)
library(sfweight) # remotes::install_github("Josiahparry/sfweight")
library(spdep)
library(broom)
library(ggplot2)
library(patchwork)
library(plm)
library(tmap)
library(gt)
library(scales)
library(arrow)

# 1. Importação -----------------------------------------------------------


# Sistema de coordenadas Geográficas de referência SIRGAS 
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

alvaras_por_lote <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Licenciamentos",
       "alvaras_por_lote.parquet")) %>%
  filter(ano_execucao >=2013)


# 2. Geografia  ----------------------------------------------------------------

# Cidade de São Paulo
sp_limite <- st_read(here("inputs", "1_inbound",
                          "Geosampa", "BordasCidade", 
                          "BorderMSP_SIRGAS.shp")) %>%
  st_transform(CRS)

#
grid <- st_make_grid(sp_limite, cellsize = c(1500,1500), crs = CRS, 
                     what = 'polygons') %>%
  st_sf('geometry' = ., data.frame(id_unidade = seq_along(.))) %>%
  st_intersection(sp_limite) %>%
  select(id_unidade) %>%
  st_transform(4674)

# Atribui ao objeto unidade a escolha da escala espacial: grid de 1,5km ou distritos
unidade <- grid

##
periodo_analise <- periodo_analise <- list(c(2013:2015), c(2019:2021))

## 
alvaras <- st_join(alvaras_por_lote, unidade[,c("id_unidade")], join = st_within)

## Aggregate values in cells
for(i in 1:length(periodo_analise)) {
  j = periodo_analise[[i]]
  alvaras_summary <- alvaras[alvaras$ano %in% j,] %>%
    group_by(id_unidade, categoria_de_uso_grupo) %>% 
    dplyr::summarize(
      ano = paste(j[1],"-", j[length(j)], sep=""),
      n_alvaras = n(), 
      n_unidades = sum(n_unidades,na.rm = T),
      area_da_construcao = sum(area_da_construcao,na.rm = T),
      n_alvaras_ERM = length(categoria_de_uso_grupo[which(categoria_de_uso_grupo == "ERM")]),
      n_unidades_ERM = sum(n_unidades[which(categoria_de_uso_grupo == "ERM")],na.rm = T),
      n_alvaras_ERP = length(categoria_de_uso_grupo[which(categoria_de_uso_grupo  == "ERP")]),
      n_unidades_ERP = sum(n_unidades[which(categoria_de_uso_grupo == "ERP")],na.rm = T)
    ) %>%
    ungroup()
  if(exists("alvaras_por_unidade")) {
    alvaras_por_unidade <- rbind(alvaras_por_unidade, alvaras_summary)} 
  else {
    alvaras_por_unidade <- alvaras_summary
  }
  print(j)
  rm(alvaras_summary)
}

# Check consistência
##
alvaras_por_unidade <- alvaras_por_unidade[!is.na(alvaras_por_unidade$id_unidade),]
##
sum(alvaras_por_unidade$n_unidades[alvaras_por_unidade$ano=="2013-2015"]) == sum(alvaras$n_unidades[alvaras$ano>=2013&alvaras$ano<=2015],na.rm = T)
sum(alvaras_por_unidade$n_unidades_ERP[alvaras_por_unidade$ano=="2013-2015"]) == sum(alvaras$n_unidades[alvaras$ano>=2013&alvaras$ano<=2015&alvaras$categoria_de_uso_grupo=="ERP"],na.rm = T)
sum(alvaras_por_unidade$n_unidades_ERM[alvaras_por_unidade$ano=="2013-2015"]) == sum(alvaras$n_unidades[alvaras$ano>=2013&alvaras$ano<=2015&alvaras$categoria_de_uso_grupo=="ERM"],na.rm = T)

##
sum(alvaras_por_unidade$n_alvaras[alvaras_por_unidade$ano=="2019-2021"]) == nrow(alvaras[alvaras$ano>=2019&alvaras$ano<=2021,])
sum(alvaras_por_unidade$n_alvaras_ERP[alvaras_por_unidade$ano== "2019-2021"]) == nrow(alvaras[alvaras$ano>=2019&alvaras$ano<=2021&alvaras$categoria_de_uso_grupo=="ERP",])
sum(alvaras_por_unidade$n_alvaras_ERM[alvaras_por_unidade$ano== "2019-2021"]) == nrow(alvaras[alvaras$ano>=2019&alvaras$ano<=2021&alvaras$categoria_de_uso_grupo=="ERM",])


# 3. Estruturação ------------------------------------------------------

# Necessário balancear os dados
is.pbalanced(alvaras_por_unidade)


## Make the dataset balanced
for (i in 1:length(periodo_analise)) {
  j = periodo_analise[[i]]
  unidade_por_ano_i <- data.frame(unidade[,c("id_unidade")])
  unidade_por_ano_i$ano <- paste(j[1],"-", j[length(j)], sep="")
  if (!exists("unidade_por_ano")) {
    unidade_por_ano<-unidade_por_ano_i[,c("id_unidade","ano")]}
  else {
    unidade_por_ano<- rbind(unidade_por_ano,unidade_por_ano_i[,c("id_unidade","ano")])
  }
  print(j)
  rm(unidade_por_ano_i)
}

#
is.pbalanced(unidade_por_ano)

#
alvaras_por_unidade <- merge(unidade_por_ano, alvaras_por_unidade,
                             by = c("id_unidade","ano"), all.x = T)

#
rm(unidade_por_ano)

#
names(alvaras_por_unidade)
summary(alvaras_por_unidade)

#
NAisZero <- c("n_alvaras", "n_unidades", "area_da_construcao","n_alvaras_ERM",
              "n_unidades_ERM","n_alvaras_ERP", "n_unidades_ERP")
##
alvaras_por_unidade[,NAisZero][is.na(alvaras_por_unidade[,NAisZero])] <- 0

#
is.pbalanced(alvaras_por_unidade)

# 4. Funções ---------------------------------------------------------

# Moran Global - contiguidade geográfica

get_moran <- function(periodo, var, signif) {
  alvaras_por_unidade_i <- alvaras_por_unidade[alvaras_por_unidade$ano == periodo,
                                               c("ano", "id_unidade", var)]
  unidade_nb <- poly2nb(unidade %>% inner_join(alvaras_por_unidade_i, by = "id_unidade"))
  unidade_listw <- nb2listw(unidade_nb, zero.policy = TRUE)
  alvaras_moran.test <- moran.test(alvaras_por_unidade_i[,var],
                                   unidade_listw,
                                   zero.policy = TRUE) %>%
    tidy() %>%
    transmute(#p_valor = p.value,
      #estimador = estimate1,
      cor_espacial = if_else(p.value < signif, estimate1, NA_real_))
  return(tibble(periodo = periodo, var = var, alvaras_moran.test))
}

# Índice G - concentração geográfica
get_g <- function(periodo, var) {
  alvaras_por_unidade_i <- alvaras_por_unidade[alvaras_por_unidade$ano == periodo,]
  alvaras_g <- data.frame(alvaras_por_unidade_i[,"id_unidade"])
  names(alvaras_g)<-"id_unidade"
  g_ERP <-((alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == paste(var,"_ERP",sep=""))]/sum(alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == paste(var,"_ERP",sep=""))])) - 
             (alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == var)])/sum(alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == var)]))^2
  summary(g_ERP)
  g_ERP <- g_ERP/(1-sum((alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == var)]/sum(alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == var)]))^2))
  g_ERP[is.infinite(g_ERP)] <- 0
  g_ERP <- sum(g_ERP)
  g_ERM <-((alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == paste(var,"_ERM",sep=""))]/sum(alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == paste(var,"_ERM",sep=""))])) - 
             (alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == var)])/sum(alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == var)]))^2
  g_ERM <- g_ERM/(1-sum((alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == var)]/sum(alvaras_por_unidade_i[,which(names(alvaras_por_unidade_i) == var)]))^2))
  g_ERM[is.infinite(g_ERM)]<-0
  g_ERM <- sum(g_ERM)
  return(tibble(periodo = periodo, var = var, g_ERP, g_ERM))
  # print(paste("Indice G de", var,"do tipo ERP =", g_ERP))
  # print(paste("Indice G de", var,"do tipo ERM =", g_ERM))
}


# alvaras_por_unidade$BalanceIndex_2013.2015 <-
#   1 - (abs(
#     alvaras_por_unidade$n_alvaras_ERM - #a
#       (
#         (mean(alvaras_por_unidade$n_alvaras_ERM) / mean(alvaras_por_unidade$n_alvaras_ERP)) #alpha 
#       * alvaras_por_unidade$n_alvaras_ERP) #b
#     ) / 
#       (alvaras_por_unidade$n_alvaras_ERM + #a
#          (
#         (mean(alvaras_por_unidade$n_alvaras_ERM) / mean(alvaras_por_unidade$n_alvaras_ERP))  #alpha 
#         * alvaras_por_unidade$n_alvaras_ERP) #b
#        )
#     )

get_entropia <- function() {
  alvaras_entropia_1 <- alvaras_por_unidade %>%
    group_by(ano) %>%
    mutate(n_erm_sp = sum(n_alvaras_ERM),
           n_erp_sp = sum(n_alvaras_ERP),
           media_erm_sp = mean(n_alvaras_ERM, na.rm = TRUE),
           media_erp_sp = mean(n_alvaras_ERP, na.rm = TRUE)) %>%
    group_by("id_grid" = id_unidade, "periodo" = ano) %>%
    summarize(n_erm = sum(n_alvaras_ERM),
              n_erp = sum(n_alvaras_ERP),
              alpha = media_erm_sp/media_erp_sp,
              balance_index = 1 - (abs(n_erm - (alpha*n_erp)) / (n_erm + (alpha*n_erp))),
              entropia = if_else(is.nan(balance_index), 0, balance_index)) %>%
    ungroup() %>%
    group_by(periodo) %>%
    summarize(var = "n_alvaras", entropia = mean(entropia, na.rm = TRUE))
  
  #
  alvaras_entropia_2 <- alvaras_por_unidade %>%
    group_by(ano) %>%
    mutate(n_erm_sp = sum(n_unidades_ERM),
           n_erp_sp = sum(n_unidades_ERP),
           media_erm_sp = mean(n_unidades_ERM, na.rm = TRUE),
           media_erp_sp = mean(n_unidades_ERP, na.rm = TRUE)) %>%
    group_by("id_grid" = id_unidade, "periodo" = ano) %>%
    summarize(n_erm = sum(n_unidades_ERM),
              n_erp = sum(n_unidades_ERP),
              alpha = media_erm_sp/media_erp_sp,
              balance_index = 1 - (abs(n_erm - (alpha*n_erp)) / (n_erm + (alpha*n_erp))),
              entropia = if_else(is.nan(balance_index), 0, balance_index)) %>%
    ungroup() %>%
    group_by(periodo) %>%
    summarize(var = "n_unidades", entropia = mean(entropia, na.rm = TRUE))
  
  alvaras_entropia <- bind_rows(alvaras_entropia_1, alvaras_entropia_2)
  
  return(alvaras_entropia)
}

# 5. Reestruturação ------------------------------------------------------------
# (Esta seção pode ser suprimida com o melhoramento das funções)

# A - Reshape para criar tabela com estatísticas para a cidade de São Paulo
## 
alvaras_moran <- list("n_alvaras", "n_unidades", "n_alvaras_ERM", 
                      "n_unidades_ERM","n_alvaras_ERP", "n_unidades_ERP") %>% 
  rbind(map_dfr(., ~ get_moran(periodo = "2013-2015", var = .x, signif = 0.1)),
        map_dfr(., ~ get_moran(periodo = "2019-2021", var = .x, signif = 0.1))) %>%
  slice(-1) %>%
  mutate(cor_espacial = as.numeric(cor_espacial),
         categoria = case_when(str_detect(var, "ERP") == T ~ "ERP",
                               str_detect(var, "ERM") == T ~ "ERM",
                               T ~ "global"),
         var = if_else(categoria != "global", str_sub(var, end = -5L), var)) %>%
  pivot_wider(names_from = categoria, values_from = cor_espacial) %>%
  rename_with(., ~ str_c("Moran_", .x), -c(periodo, var))


##
alvaras_g <- list("n_alvaras", "n_unidades") %>%
  set_names() %>%
  bind_rows(map_dfr(., ~ get_g(periodo = "2013-2015", var = .x)),
            map_dfr(., ~ get_g(periodo = "2019-2021", var = .x))) %>%
  slice(-1) %>%
  select(-c(1:2)) 

##
alvaras_entropia <- get_entropia()

# Objeto alvaras_sfstats --> Índices Moran e G para São Paulo
alvaras_sfstats <- list(alvaras_moran, alvaras_g, alvaras_entropia) %>%
  reduce(left_join, by= c("periodo", "var"))

# 6. Exportação -----------------------------------------------------------------

#
write_parquet(alvaras_sfstats,
              here("inputs", 
                   "2_trusted", "Licenciamentos",
                   "alvaras_sfstats_no_geo.parquet"))
