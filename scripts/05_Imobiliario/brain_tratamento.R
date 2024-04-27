# Deflacionar valores
# Avaliar

library(here)
library(readxl)
library(sf)
library(tidyverse)
library(tidylog)
library(lubridate)
library(skimr)
library(gt)
library(patchwork)
library(DataExplorer)
library(stringr)
library(waldo)
library(sfarrow)
library(deflateBR)

# 1. Importação ---------------------------------------------------------

#
# i_am("inputs/Lancamentos/Brain/ArquivosOriginais/Base São Paulo 22_02_2022.xlsx")

#
lancamentos_raw <- read_excel(here("inputs", 
                                   "1_inbound",
                                   "Brain", "ArquivosOriginais", 
                                   "Base São Paulo 22_02_2022.xlsx")) %>%
  filter(!is.na(`Data de Lancamento`))

#
lancamentos_pre <- lancamentos_raw %>%
  mutate(id_tipo_empreendimento = row_number()) %>% # Adiciona id de empreendimento-tipo 
  group_by(Empreendimento) %>%
  mutate(id_empreendimento = cur_group_id()) %>% # Adiciona id de empreendimento
  ungroup() %>%
  select(id_tipo_empreendimento, id_empreendimento, everything()) %>%
  mutate(Tipologia = as.character(Tipologia),
         Metragem = as.numeric(`Área Privativa`),
         lancamento_inicial = dmy(paste0("01/", str_sub(`Data de Lancamento`, -7L))),
         across(contains("Preço"), as.numeric),
         across(where(is.numeric), as.numeric),
         across(where(is.numeric), ~ if_else(.==0, NA_real_, .))
  )

# Variáveis alvo do tratamento
lancamentos_pre %>% 
  select(-starts_with("id")) %>%
  select(where(is.numeric)) %>%
  glimpse()

# Key para agregar informações úteis por empreendimento que não serão alvo do tratamento
lancamentos_tipo_key <- lancamentos_pre %>% 
  select(id_tipo_empreendimento, Empreendimento, Latitude, 
         Longitude, lancamento_inicial, Tipo, Tipologia) %>%
  group_by(id_tipo_empreendimento) %>%
  summarize(#id_tipo_empreendimento = first(id_tipo_empreendimento),
            empreendimento = first(Empreendimento),
            uso = first(Tipo),
            dormitorios = first(Tipologia),
            data_lancamento_inicial = first(lancamento_inicial),
            Latitude = first(Latitude),
            Longitude = first(Longitude))

#
lancamentos_key <- lancamentos_pre %>% 
  select(id_empreendimento, Empreendimento, Latitude, 
         Longitude, lancamento_inicial, Tipo, Tipologia) %>%
  group_by(id_empreendimento) %>%
  summarize(#id_tipo_empreendimento = first(id_tipo_empreendimento),
    empreendimento = first(Empreendimento),
    uso = first(Tipo),
    dormitorios = first(Tipologia),
    data_lancamento_inicial = first(lancamento_inicial),
    Latitude = first(Latitude),
    Longitude = first(Longitude))


#
zoneamento <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao",
       "zoneamento.parquet")) %>%
  st_transform(crs = 4674)


# 2. Checagem preliminar -----------------------------------------------------

# A - Checando match com números da Brain
##
teste1 <- lancamentos_pre %>%
  mutate("Ano" = year(lancamento_inicial)) %>%
  filter(Ano > 2012) %>%
  group_by(Ano) %>%
  summarize(#"Empreendimentos" = n_distinct(id_empreendimento),
    Ano = as.character(first(Ano)),
    `Oferta Lançada` = sum(`Oferta Lançada`, na.rm = TRUE)) %>%
  ungroup()


# Esses foram os números declarados pela BRAIN
teste2 <- data.frame(Ano = as.character(seq(2013, 2021, 1)),
                     "Unidades_BRAIN" = c(40443, 36641, 26879, 25083, 29965, 
                                          45072, 74249, 75252, 54752))

teste3 <- data.frame(Ano = as.character(seq(2013, 2021, 1)),
                     "Unidades_SECOVI" = c(34188,33955,22960,19359,31379,
                                           37124,65312,59978,49563))

#
teste_a <- teste1 %>% 
  left_join(teste2, by = "Ano") %>% 
  left_join(teste3, by = "Ano") 


#
teste_a %>%
  gt()

#
ggplot(teste_a %>% pivot_longer(cols = 2:4)) +
  geom_col(aes(Ano, value, fill = name)) +
  geom_text(aes(Ano, value, label = value), size = 2.25, vjust = -0.5) +
  labs(x = "Ano", y = "") +
  facet_wrap(~ name, ncol = 1) +
  scale_fill_viridis_d() +
  theme(legend.title= element_blank())

# B - Checando distribuição das variáveis alvo do tratamento
##
teste4 <- lancamentos_raw %>%
  select(c(starts_with("Venda Líquida"))) %>%
  transmute("2013" = rowSums(select(., contains("2013"))),
            "2014" = rowSums(select(., contains("2014"))),
            "2015" = rowSums(select(., contains("2015"))),
            "2016" = rowSums(select(., contains("2016"))),
            "2017" = rowSums(select(., contains("2017"))),
            "2018" = rowSums(select(., contains("2018"))),
            "2019" = rowSums(select(., contains("2019"))),
            "2020" = rowSums(select(., contains("2020"))),
            "2021" = rowSums(select(., contains("2021")))) %>%
  summarize(across(where(is.numeric), sum)) %>%
  pivot_longer(cols = everything()) %>%
  transmute("Ano" = name, "Venda Líquida" = value)

#
teste5 <- lancamentos_raw %>%
  mutate(across(starts_with("Preço"), as.numeric)) %>%
  select(c(starts_with("Preço"))) %>%
  transmute("2013" = rowSums(select(., contains("2013"))),
            "2014" = rowSums(select(., contains("2014"))),
            "2015" = rowSums(select(., contains("2015"))),
            "2016" = rowSums(select(., contains("2016"))),
            "2017" = rowSums(select(., contains("2017"))),
            "2018" = rowSums(select(., contains("2018"))),
            "2019" = rowSums(select(., contains("2019"))),
            "2020" = rowSums(select(., contains("2020"))),
            "2021" = rowSums(select(., contains("2021")))) %>%
  summarize(across(where(is.numeric), ~ sum(if_else(.x != 0, 1, 0)))) %>%
  pivot_longer(cols = everything()) %>%
  transmute("Ano" = name, "Com preço" = value)

#
teste_b <- teste1 %>% 
  left_join(teste4, by = "Ano") %>%
  left_join(teste5, by = "Ano")

#
teste_b %>%
  gt()

#
ggplot(teste_b %>% pivot_longer(cols = 2:4)) +
  geom_col(aes(Ano, value, fill = name)) +
  geom_text(aes(Ano, value, label = value), size = 2.25, vjust = -0.5) +
  labs(x = "", y = "") +
  facet_wrap(~ fct_reorder(name, value), ncol = 1) +
  scale_fill_viridis_d() +
  theme(legend.title= element_blank())


# 3. Tratamento ---------------------------------------------------------

#
lancamentos_pivot <- lancamentos_pre %>%
  pivot_longer(cols = c(`Venda Líquida 01/2013`:`Preço 12/2021`)) %>%
  mutate(id = row_number(),
         mes = str_sub(name, -7L),
         ano = as.numeric(str_sub(name, -4L)),
         data_lancamento = dmy(paste0("01/", (str_sub(name, -7L)))),
         name = case_when(str_detect(name, "Oferta") ~ "oferta_tipo_mes",
                          str_detect(name, "Venda") ~ "venda_tipo_mes",
                          str_detect(name, "Preço") ~ "preco_tipo_mes")) %>%
  pivot_wider() 

#
glimpse(lancamentos_pivot)

#
lancamentos_tipo_mes <- lancamentos_pivot %>%
  group_by(ano, mes, data_lancamento, id_tipo_empreendimento) %>%
  summarize(id_empreendimento = first(id_empreendimento),
            oferta_lancada = first(na.omit(`Oferta Lançada`)),
            oferta_tipo_mes = first(na.omit(oferta_tipo_mes)),
            venda_tipo_mes = first(na.omit(venda_tipo_mes)),
            preco_tipo_mes = first(na.omit(preco_tipo_mes)),
            metragem_unidade = first(na.omit(Metragem)),
            vgv_tipo_mes = venda_tipo_mes * preco_tipo_mes) %>%
  ungroup()

# Acrescenta preços deflacionados
lancamentos_tipo_mes <- lancamentos_tipo_mes %>%
  mutate(preco_tipo_mes_deflacionado = deflate(nominal_values = preco_tipo_mes,
                                                nominal_dates = data_lancamento,
                                                real_date = "12/2021",
                                                index = "igpm"),
          vgv_tipo_mes_deflacionado = venda_tipo_mes * preco_tipo_mes_deflacionado) %>%
  ungroup()

#
glimpse(lancamentos_tipo_mes)

#
lancamentos_tipo_ano <- lancamentos_tipo_mes %>%
  group_by(ano, id_tipo_empreendimento) %>%
  summarize(id_empreendimento = first(id_empreendimento),
            n_unidades = first(oferta_lancada),
            n_unidades_lancadas = sum(oferta_tipo_mes, na.rm = TRUE),
            n_unidades_vendidas = sum(venda_tipo_mes, na.rm = TRUE),
            vgv = sum(vgv_tipo_mes,na.rm=T),
            vgv_deflacionado = sum(vgv_tipo_mes_deflacionado, na.rm = TRUE),
            metragem_unidade = first(metragem_unidade),
            preco_unidade = vgv / n_unidades_vendidas,
            preco_unidade_deflacionado = vgv / n_unidades_vendidas,
            area_construida_vendida = metragem_unidade*n_unidades_vendidas,
            preco_m2 = vgv / area_construida_vendida,
            preco_m2_deflacionado = vgv_deflacionado / area_construida_vendida) %>%
  ungroup() %>%
  mutate(across(where(is.numeric), ~ replace(.x, is.infinite(.x), NA_real_)))

#
lancamentos_tipo <- lancamentos_tipo_ano %>%
  # filter(!is.na(preco_unidade)) %>%
  # mutate(n_unidades_vendidas = n_unidades_vendidas) %>%
  left_join(lancamentos_tipo_key, by = "id_tipo_empreendimento") %>%
  select(ano, # Index
         id_tipo_empreendimento, id_empreendimento, empreendimento, 
         data_lancamento_inicial, uso, dormitorios, Latitude, Longitude, # Keys (não mudam com a série temporal) 
         everything() # Values
         ) 


# 4. Testes de consistência -----------------------------------------------

# Quantidade de id's de empreendimento deve ser idêntica
compare(unique(lancamentos_tipo$id_empreendimento), unique(lancamentos_pre$id_empreendimento))
compare(unique(lancamentos_tipo$id_tipo_empreendimento), unique(lancamentos_pre$id_tipo_empreendimento))

# Valores de venda totais devem ser manter ao longo do tratamento da base de dados
sum(lancamentos_raw[,substr(names(lancamentos_raw),1,5)=="Venda"])
sum(lancamentos_pre[,substr(names(lancamentos_pre),1,5)=="Venda"],na.rm = T)
sum(lancamentos_pivot[,substr(names(lancamentos_pivot),1,5)=="venda"],na.rm = T)
sum(lancamentos_tipo_mes[,substr(names(lancamentos_tipo_mes),1,5)=="venda"],na.rm = T)
sum(lancamentos_tipo_ano[,"n_unidades_vendidas"],na.rm = T)
sum(lancamentos_tipo[,"n_unidades_vendidas"],na.rm = T)

# valores de preço médios devem se manter longo do tratamento da base de dados até a base por mês
#sum(lancamentos_raw[,substr(names(lancamentos_raw),1,5)=="Preço"])  # Objeto veio como string
sum(lancamentos_pre[,substr(names(lancamentos_pre),1,5)=="Preço"],na.rm = T)
sum(lancamentos_pivot[,substr(names(lancamentos_pivot),1,5)=="preco"],na.rm = T)
sum(lancamentos_tipo_mes[,substr(names(lancamentos_tipo_mes),1,5)=="preco"],na.rm = T)

# Valores de ofertas lançadas devem ser manter ao longo do tratamento da base de dados
sum(lancamentos_raw$`Oferta Lançada`,na.rm = T)
sum(lancamentos_pre$`Oferta Lançada`,na.rm = T)
sum(lancamentos_pivot$`Oferta Lançada`[!duplicated(lancamentos_pivot$id_tipo_empreendimento)],na.rm = T)
sum(lancamentos_tipo_mes$oferta_lancada[!duplicated(lancamentos_tipo_mes$id_tipo_empreendimento)],na.rm = T)
sum(lancamentos_tipo_ano$n_unidades[!duplicated(lancamentos_tipo_ano$id_tipo_empreendimento)],na.rm = T)

# Valores de VGV devem ser manter ao longo do tratamento da base de dados a partir da base por mês
sum(lancamentos_tipo_mes$vgv_tipo_mes,na.rm = T)
sum(lancamentos_tipo_ano$vgv,na.rm = T)
sum(lancamentos_tipo$vgv,na.rm = T)


# 5. Remodelagem ----------------------------------------------------------

# Reorganizando conjunto de dados por empreendimento
lancamentos_ano <- lancamentos_tipo_mes %>%
  group_by(ano, id_empreendimento) %>%
  summarize(n_unidades = first(oferta_lancada),
            n_unidades_lancadas = sum(oferta_tipo_mes, na.rm = TRUE),
            n_unidades_vendidas = sum(venda_tipo_mes, na.rm = TRUE),
            vgv = sum(vgv_tipo_mes,na.rm=T),
            vgv_deflacionado = sum(vgv_tipo_mes_deflacionado, na.rm = TRUE),
            metragem_unidade = weighted.mean(metragem_unidade, oferta_lancada),
            preco_unidade = vgv / n_unidades_vendidas,
            preco_unidade_deflacionado = vgv / n_unidades_vendidas,
            area_construida_vendida = metragem_unidade*n_unidades_vendidas,
            preco_m2 = vgv / area_construida_vendida,
            preco_m2_deflacionado = vgv_deflacionado / area_construida_vendida) %>%
  ungroup() %>%
  mutate(across(where(is.numeric), ~ replace(.x, is.infinite(.x), NA_real_)))

#
lancamentos <- lancamentos_ano %>%
  # filter(!is.na(preco_unidade)) %>%
  # mutate(n_unidades_vendidas = n_unidades_vendidas) %>%
  left_join(lancamentos_key, by = "id_empreendimento") %>%
  select(ano, # Index
         #id_tipo_empreendimento, 
         id_empreendimento, empreendimento, 
         data_lancamento_inicial, uso, dormitorios, Latitude, Longitude, # Keys (não mudam com a série temporal) 
         everything() # Values
  ) 


# 7. DataViz -------------------------------------------------------------------

#
glimpse(lancamentos)

#
lancamentos %>%
  select(-c(id_empreendimento, Latitude, Longitude)) %>%
  plot_correlation(cor_args = list(use = "pairwise.complete.obs"),
                   theme_config = list(axis.title = element_blank(),
                                       axis.text.x = element_text(angle = 10),
                                       legend.position = "none"))
#
lancamentos %>%
  ggplot(aes(data_lancamento_inicial)) +
  geom_bar(color = "grey") +
  scale_x_date(breaks = "1 years", date_labels = "%Y") 

# Série temporal com todos registros --> 100%
lancamentos %>%
  ggplot(aes(ano)) +
  geom_bar() +
  scale_x_continuous(breaks = seq(2013, 2021, 1))

# Não-respostas em preço ao longo do tempo

lancamentos %>%
  filter(!is.na(preco_unidade)) %>%
  ggplot(aes(ano)) +
  geom_bar() +
  scale_x_continuous(breaks = seq(2013, 2021, 1))

#
lancamentos %>%
  mutate(com_preco = if_else(is.na(preco_unidade), "Sem preço", "Com preço")) %>%
  ggplot(aes(ano, fill = com_preco)) +
  geom_bar(position = "dodge", color = "black") +
  scale_y_continuous(breaks = seq(0, 7000, 500), limits = c(0, 3000)) +
  scale_x_continuous(breaks = seq(2013, 2021, 1)) +
  theme(legend.position = "bottom")


#
lancamentos %>%
  mutate(com_preco_m2 = if_else(is.na(preco_m2), "Sem preço m²", "Com preço m²")) %>%
  ggplot(aes(ano, fill = com_preco_m2)) +
  geom_bar(position = "dodge", color = "black") +
  scale_y_continuous(breaks = seq(0, 7000, 500), limits = c(0, 3000)) +
  scale_x_continuous(breaks = seq(2013, 2021, 1)) +
  theme(legend.position = "bottom")

#
lancamentos %>%
  plot_bar("n_unidades_vendidas")

#
lancamentos %>%
  ggplot(aes(n_unidades_vendidas, preco_m2, color = dormitorios)) +
  geom_point() +
  facet_wrap(~ dormitorios) 

#
lancamentos %>%
  ggplot(aes(n_unidades_vendidas, preco_m2, color = dormitorios)) +
  geom_point() +
  facet_grid(uso ~ dormitorios) 


# 6. GIS ------------------------------------------------------------------

# #
# lancamentos %>%
#   filter(is.na(Latitude)) %>%
#   count()
# 
# #
# CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
# 
# #
# BordasCidade <- st_read(here("inputs", "Complementares", 
#                              "BordasCidade", "BorderMSP_SIRGAS.shp")) %>%
#   st_transform(.x, crs = 4674) 
# 
# # Alguns empreendimentos tem latitude fora do perímetro de SP
# lancamentos %>%
#   filter(!is.na(Latitude)) %>%
#   sf::st_as_sf(coords = c("Longitude", "Latitude"), crs = 4674) %>%
#   st_join(BordasCidade, join = negate(st_within), left = FALSE) %>%
#   st_drop_geometry() %>%
#   count()
# 
# # Importa tipologia de zoneamento e agrega informações por empreendimento
# ZonasLeiZoneamento <- st_read(here("inputs", "Complementares", "Zoneamento", 
#                                    "ZonasLeiZoneamento", "ZoningSPL_Clean_Class.shp")) %>%
#   select(Zone) %>%
#   st_transform(crs = st_crs(4674)) %>%
#   # st_make_valid(.) %>%
#   filter(st_is_valid(.) == TRUE) %>%
#   filter(!is.na(Zone)) %>%
#   left_join(read_excel(here("inputs", "Complementares", "Zoneamento", 
#                             "ZonasLeiZoneamento", "Class_Join.xls")), # tipologia nossa
#             by = "Zone") %>%
#   transmute(zoneamento_ajust = as.factor(Zone),
#             zoneamento_grupo = case_when(Zone == "ZEIS-1" ~ "ZEIS-Aglomerado",
#                                          str_starts(Zone, "ZEIS") ~ "ZEIS-Vazio",
#                                          TRUE ~ ZoneGroup) %>%
#               fct_relevel(c("ZCs&ZMs", "EETU", "ZEIS-Aglomerado", "ZEIS-Vazio", "ZETM",
#                             "ZPI&DE", "ZE&P", "ZE&PR", "ZCOR")) %>%
#               fct_other(drop = c("ZPI&DE", "ZE&P", "ZE&PR", "ZCOR", "ZETM"), 
#                         other_level = "Outros"),
#             zoneamento_grupo = fct_relevel(zoneamento_grupo,
#                                            c("ZCs&ZMs", "EETU", "ZEIS-Aglomerado", 
#                                              "ZEIS-Vazio", "Outros")),
#             ca = as.factor(FARmax),
#             solo_uso = as.factor(case_when(Use == "mixed use" ~ "misto",
#                                            Use == "unique use" ~ "único",
#                                            TRUE ~ NA_character_)))
# 
# # 
# Macroareas <- st_read(here("inputs", "Complementares", "Macroareas",
#                            "SIRGAS_SHP_MACROAREAS.shp")) %>%
#   transmute(macroarea = as_factor(mc_sigla)) %>%
#   st_transform(crs = st_crs(4674))
# 
# #
# eixos <- st_read(here("outputs", "Eixos", "Eixos.shp")) %>%
#   set_names(as_vector(read_csv(here("outputs", "Eixos", 
#                                     "EixosLabels.csv"), show_col_types = FALSE))) %>%
#   select(id_Eixo, NomeTransp, TipoTransp, ) %>%
#   st_transform(4674)
# 
# #
# distritos <- st_read(here("inputs", "Complementares", "Distritos", 
#                           "DistrictsMSP_SIRGAS.shp")) %>%
#   st_transform(4674)
# 
# #
# subprefeituras <- st_read(
#   here("inputs", "Complementares", "Subprefeituras", "SubprefectureMSP_SIRGAS.shp"),
#   crs = CRS) %>%
#   st_transform(4674) %>%
#   select(-sp_codigo)
# 
# #
# lancamentos.shp <- lancamentos %>%
#   filter(!is.na(Latitude)) %>%
#   sf::st_as_sf(coords = c("Longitude", "Latitude"), crs = 4674) %>%
#   # st_filter(BordasCidade, .predicate = st_within) %>%
#   st_join(eixos, join = st_intersects) %>%
#   st_join(ZonasLeiZoneamento, join = st_nearest_feature) %>%
#   st_join(distritos, join = st_within) %>%
#   st_join(subprefeituras, join = st_within) %>%
#   st_join(Macroareas, join = st_within, largest = TRUE) 


lancamentos.shp <- lancamentos %>%
  filter(!is.na(Latitude)) %>%
  sf::st_as_sf(coords = c("Longitude", "Latitude"), crs = 4674) %>%
  st_join(st_make_valid(zoneamento))


#
anti_join(lancamentos, lancamentos.shp, by = c("ano", "id_empreendimento")) %>%
  view("Geografia inválida")

# Duplicados | Pode ser ajustado com parâmetro "largest"
st_drop_geometry(lancamentos.shp) %>%
  filter(duplicated(.)) %>%
  unique() %>%
  view("brain_duplicados")

# Exlui duplicados e reposiciona variáveis
lancamentos.shp <- lancamentos %>%
  filter(!is.na(Latitude)) %>%
  select(id_empreendimento, ano, Latitude, Longitude) %>%
  sf::st_as_sf(coords = c("Longitude", "Latitude"), crs = 4674) %>%
  inner_join(st_drop_geometry(lancamentos.shp) %>% 
               filter(!duplicated(.)), by = c("ano", "id_empreendimento")) %>%
  select(ano, id_empreendimento, empreendimento:macroarea, geometry) # reposiciona
   

# 6. Exportação --------------------------------------------------------------

# # Formato .shp
# st_write(lancamentos.shp, here("inputs", "Lancamentos", "Brain", "ArquivosTratados"),
#          layer="Brain", delete_layer = TRUE, driver="ESRI Shapefile")
# 
# # Adiciona labels da Base Tratada
# write_csv(data.frame(labels = names(lancamentos.shp)), 
#           here("inputs", "Lancamentos", "Brain", "ArquivosTratados",
#                "BrainLabels.csv"))

# Formato parquet
sfarrow::st_write_parquet(lancamentos.shp, here("inputs", "2_trusted", "Lancamentos",
                                                     "brain_lancamentos.parquet"))

