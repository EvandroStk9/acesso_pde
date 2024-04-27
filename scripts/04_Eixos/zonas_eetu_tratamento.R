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
options(scipen = 99999,
        error = beep)

# 1. Importa -----------------------------------------------------------

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

#
get_shapefiles(
  files = c("Eixos_EETU_AtivadosDecreto", "Distrito")
)


## Praça da Sé como CBD
cbd <- data.frame(long = -46.633959, lat= -23.550382) %>%
  st_as_sf(coords = c("long", "lat"),
           crs = 4674) %>%
  st_transform(crs = CRS)

#
zoneamento <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao",
       "zoneamento.parquet"))

#
eixos <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos.parquet"))
#
isocronas <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos_por_isocrona.parquet")
)


# 2. Estrutura --------------------------------------------------------------


# Juntando os eixos (EETU e por decreto)
zonas_eetu <- zoneamento %>%
  filter(zoneamento == "ZEU" | zoneamento == "ZEUa") %>%
  st_join(eixos_eetu_ativados_decreto %>%
            transmute(decreto_eixo = ed_decreto),
          largest = TRUE) %>%
  mutate(
    id = rownames(.) %>% as.integer())

# 3. Identifica infraestrutura mais próxima  -----------------------------------


#
eixos_metro <- eixos %>% 
  filter(tipo_transp == "METRÔ")

#
eixos_trem <- eixos %>% 
  filter(tipo_transp == "TREM")

#
eixos_corredor <- eixos %>% 
  filter(tipo_transp == "ÔNIBUS")


# #
# nearest_metro <- 
#   tibble(
#     id = rownames(zonas_eetu) %>% as.numeric(),
#     id_metro = st_nearest_feature(zonas_eetu, eixos_metro),
#     dist_metro = st_distance(zonas_eetu, 
#                              eixos_metro[st_nearest_feature(zonas_eetu, eixos_metro),], 
#                              by_element = TRUE) %>% as.vector()
#   ) 
# 
# #
# nearest_trem <- 
#   tibble(
#     id = rownames(zonas_eetu) %>% as.numeric(),
#     id_trem = st_nearest_feature(zonas_eetu, eixos_trem),
#     dist_trem = st_distance(zonas_eetu, 
#                             eixos_trem[st_nearest_feature(zonas_eetu, eixos_trem),], 
#                             by_element = TRUE) %>% as.vector()
#   ) 
# 
# #
# nearest_corredor <- 
#   tibble(
#     id = rownames(zonas_eetu) %>% as.numeric(),
#     id_corredor = st_nearest_feature(zonas_eetu, 
#                                      eixos_corredor),
#     dist_corredor = st_distance(zonas_eetu, 
#                                 eixos_corredor[st_nearest_feature(zonas_eetu, 
#                                                                   eixos_corredor),], 
#                                 by_element = TRUE) %>% as.vector()) 

##
nearest_metro_1 <- st_nearest_feature(zonas_eetu, 
                                      eixos_metro %>%
                                        filter(ind_area_expansao == FALSE)) %>%
  as.data.frame() %>%
  set_names("id_metro") %>%
  mutate(id = rownames(.) %>% as.integer())

##
nearest_metro_2 <- st_nearest_feature(zonas_eetu, 
                                     eixos_metro %>%
                                       filter(ind_area_expansao == TRUE)) %>%
  as.data.frame() %>%
  set_names("id_metro") %>%
  mutate(id = rownames(.) %>% as.integer())

#
nearest_trem <- st_nearest_feature(zonas_eetu, eixos_trem) %>%
  as.data.frame() %>%
  set_names("id_trem") %>%
  mutate(id = rownames(.) %>% as.integer())

nearest_corredor <- st_nearest_feature(zonas_eetu, eixos_corredor) %>%
  as.data.frame() %>%
  set_names("id_corredor") %>%
  mutate(id = rownames(.) %>% as.integer())


# 4. Tipifica eixo ---------------------------------------------------------

# Assinala eixo se quadra toca buffer de 400m e está incluída no buffer de 600m
# Operadores espaciais: sf::st_buffer, sf::st_within e sf::st_intersects
# Raios estendidos em virtude da presença de missing data
eixos_metro_400m_1 <- 
  # Caso 1 - Área de expansão = TRUE - Linhas Verde, Amarela e Lilás
    st_within(zonas_eetu,
              st_buffer(eixos_metro %>%
                          filter(ind_area_expansao == FALSE), 
                        dist = 720), # Margem de erro entre 10% e 20%
              sparse = TRUE,
              largest = TRUE) %>%
      as.data.frame() %>%
      transmute(id = row.id,
                id_metro = col.id,
                metro = 1) %>% # identificador binário para assinalar Tipo de transporte
      inner_join(st_intersects(zonas_eetu,
                               st_buffer(eixos_metro %>%
                                           filter(ind_area_expansao == FALSE), dist = 450),
                               sparse = TRUE,
                               largest = TRUE) %>%
                   as.data.frame() %>%
                   transmute(id = row.id,
                             id_metro = col.id,
                             metro = 1))

#
eixos_metro_400m_2 <-
    # Caso 2 - Área de expansão = TRUE - Linhas Azul, Vermelha e Prata
    st_within(zonas_eetu,
              st_buffer(eixos_metro %>%
                          filter(ind_area_expansao == TRUE), dist = 2000), 
              sparse = TRUE,
              largest = TRUE) %>%
      as.data.frame() %>%
      transmute(id = row.id,
                id_metro = col.id,
                metro = 1) %>% # identificador binário para assinalar Tipo de transporte
      inner_join(st_intersects(zonas_eetu,
                               st_buffer(eixos_metro %>%
                                           filter(ind_area_expansao == TRUE), dist = 800),
                               sparse = TRUE,
                               largest = TRUE) %>%
                   as.data.frame() %>%
                   transmute(id = row.id,
                             id_metro = col.id,
                             metro = 1))
  
  #
  eixos_trem_400m <- st_within(zonas_eetu, st_buffer(eixos_trem, dist = 600),
                               sparse = TRUE,
                               largest = TRUE) %>%
    as.data.frame() %>%
    transmute(id = row.id,
              id_trem = col.id,
              trem = 1) %>%
    inner_join(st_intersects(zonas_eetu,
                             st_buffer(eixos_trem, dist = 440),
                             sparse = TRUE,
                             largest = TRUE) %>%
                 as.data.frame() %>%
                 transmute(id = row.id,
                           id_trem = col.id,
                           trem = 1))
  
  # Na primeira versão, assinalava-se eixo por exclusão das outras (metrô ou trem, senão corredor)
  # Nesta versão, assinala-se eixo de acordo com critério de infraestrutura
  eixos_corredor_150m <- st_within(zonas_eetu,
                                   st_buffer(eixos_corredor, dist = 360),
                                   sparse = TRUE,
                                   largest = TRUE) %>%
    as.data.frame() %>%
    transmute(id = row.id,
              id_corredor = col.id,
              corredor = 1) %>%
    inner_join(st_intersects(zonas_eetu,
                             st_buffer(eixos_corredor, dist = 175),
                             sparse = TRUE,
                             largest = TRUE) %>%
                 as.data.frame() %>%
                 transmute(id = row.id,
                           id_corredor = col.id,
                           corredor = 1))
  
  # Criando key com todos os id's de interesse
  zonas_eetu_key <- st_drop_geometry(zonas_eetu) %>% # pressuposto: todo eetu deve ser tipificado
    select(id) %>%
    full_join(eixos_metro_400m_1 %>%
                inner_join(nearest_metro_1, by = c("id", "id_metro")), 
              by = "id") %>%
    full_join(eixos_metro_400m_2 %>%
                inner_join(nearest_metro_2, by = c("id", "id_metro")), 
              by = "id", suffix = c("_1", "_2")) %>%
    full_join(eixos_trem_400m %>%
                inner_join(nearest_trem, by = c("id", "id_trem")), 
              by = "id") %>%
    full_join(eixos_corredor_150m %>%
                inner_join(nearest_corredor, by = c("id", "id_corredor")), 
              by = "id")
  
  # Criando critério para assinalar tipo de Transporte que origina o eixo
  zonas_eetu_tipificacao <- zonas_eetu_key %>%
    transmute(id, id_metro_1, id_metro_2, id_trem, id_corredor, 
              # id_eixo = case_when(
              #   metro == 1 ~ id_metro,
              #   trem == 1 ~ id_trem,
              #   corredor == 1 ~ id_corredor,
              #   TRUE ~ NA_real_
              # ),
              # dist_metro, dist_trem, dist_corredor,
              tipo_transp = case_when(
                metro_1 == 1 ~ "METRÔ",
                metro_2 == 1 ~ "METRÔ",
                trem == 1 ~ "TREM",
                corredor == 1 ~ "ÔNIBUS",
                TRUE ~ NA_character_)#,
              # dist = case_when(
              #   metro == 1 ~ dist_metro,
              #   trem == 1 ~ dist_trem,
              #   corredor == 1 ~ dist_corredor,
              #   TRUE ~ NA_real_
              # )
    ) %>%
    left_join(st_drop_geometry(eixos_metro %>%
                                 filter(ind_area_expansao == FALSE)) %>%
                transmute(id_metro_1 = row_number(),
                          eixo_metro_1 = eixo,
                          tipo_transp,
                          tipo_infra_metro_1 = tipo_infra,
                          n_pontos_acesso_1 = n_pontos_acesso, 
                          tipo_transp_secundario_1 = tipo_transp_secundario,
                          situacao_metro_1 = situacao_transp
                          ),
              by = c("id_metro_1", "tipo_transp")) %>%
    left_join(st_drop_geometry(eixos_metro %>%
                                 filter(ind_area_expansao == TRUE)) %>%
                transmute(id_metro_2 = row_number(),
                          tipo_transp,
                          eixo_metro_2 = eixo,
                          tipo_infra_metro_2 = tipo_infra,
                          n_pontos_acesso_2 = n_pontos_acesso, 
                          tipo_transp_secundario_2 = tipo_transp_secundario,
                          situacao_metro_2 = situacao_transp),
              by = c("id_metro_2", "tipo_transp")) %>%
    left_join(st_drop_geometry(eixos_trem) %>%
                transmute(id_trem = row_number(),
                          eixo_trem = eixo,
                          tipo_infra_trem = tipo_infra,
                          n_pontos_acesso_trem = n_pontos_acesso, 
                          tipo_transp, #tipo_transp_secundario,
                          situacao_trem = situacao_transp),
              by = c("id_trem", "tipo_transp")) %>%
    left_join(st_drop_geometry(eixos_corredor) %>%
                transmute(id_corredor = row_number(),
                          eixo_corredor = eixo,
                          tipo_infra_corredor = tipo_infra,
                          n_pontos_acesso_corredor = n_pontos_acesso,
                          tipo_transp, #tipo_transp_secundario,
                          situacao_corredor = situacao_transp),
              by = c("id_corredor", "tipo_transp")) %>%
    # left_join(st_drop_geometry(eixos) %>%
    #             mutate(id_eixo = row_number()),
    #           by = c("id_eixo", "tipo_transp")) 
    transmute(
      id,
      eixo = coalesce(eixo_metro_1, eixo_metro_2, eixo_trem, eixo_corredor),
      tipo_infra = coalesce(tipo_infra_metro_1, tipo_infra_metro_2, 
                            tipo_infra_trem, tipo_infra_corredor),
      n_pontos_acesso = coalesce(n_pontos_acesso_1, n_pontos_acesso_2,
                                 n_pontos_acesso_trem, n_pontos_acesso_corredor),
      tipo_transp,
      tipo_transp_secundario = coalesce(tipo_transp_secundario_1, 
                                        tipo_transp_secundario_2),
      situacao_transp = coalesce(situacao_metro_1, situacao_metro_2,
                                 situacao_trem, situacao_corredor)#,
      # dist_transp = dist,
      # tipo_eixo = if_else(!is.na(tipo_transp), "Nova regra", NA_character_)
    )
  
  # Eixos por zoneamento (quadras)
  zonas_eetu_tipificados <- zonas_eetu %>%
    left_join(zonas_eetu_tipificacao, 
              by = "id") %>%
    mutate(
      tipo_eixo = case_when(
        zoneamento %in% c("ZEU", "ZEUa") & !is.na(decreto_eixo) ~ "Decreto",
        zoneamento %in% c("ZEU", "ZEUa") & is.na(decreto_eixo) ~ "EETU",
        # !is.na(eixo) ~ tipo_eixo,
        TRUE ~ NA_character_)
    ) %>%
    select(-id) %>%
    group_by(eixo, tipo_infra) %>%
    mutate(area_eixo = sum(area_zoneamento)) %>%
    ungroup()
  
  # 6. Tipifica isócronas ---------------------------------------------------
  
  #
  isocronas_tipificacao <- zonas_eetu_tipificados %>%
    transmute(id_lote = rownames(.) %>% as.numeric,
              tipo_transp) %>%
    # slice_sample(prop = 0.01) %>%
    st_join(isocronas %>% 
              filter(tipo_transp == "METRÔ") %>%
              transmute(isocrona_metro = isocrona),
            largest = FALSE) %>%
    st_join(isocronas %>% 
              filter(tipo_transp == "TREM") %>%
              transmute(isocrona_trem = isocrona),
            largest = FALSE) %>%
    st_join(isocronas %>% 
              filter(tipo_transp == "ÔNIBUS") %>%
              transmute(isocrona_onibus = isocrona),
            largest = FALSE) %>%
    group_by(id_lote) %>%
    mutate(
      isocrona = case_when(
        tipo_transp == "METRÔ" ~ isocrona_metro,
        tipo_transp == "TREM" ~ isocrona_trem,
        tipo_transp == "ÔNIBUS" ~ isocrona_onibus,
      ),
      teste = case_when(
        tipo_transp == "METRÔ" & isocrona_metro == min(isocrona_metro) ~ "A",
        tipo_transp == "TREM" & isocrona_trem == min(isocrona_trem) ~ "B",
        tipo_transp == "ÔNIBUS" & isocrona_onibus == min(isocrona_onibus) ~ "C",
        is.na(isocrona) ~ "D",
        TRUE ~ NA_character_)) %>%
    group_by(id_lote, tipo_transp, teste) %>%
    summarize(across(everything(), first)) %>%
    ungroup() %>%
    filter(!is.na(teste))
  
  #
  zonas_eetu_ajust <- zonas_eetu_tipificados %>%
    mutate(id_lote = rownames(.) %>% as.numeric()) %>%
    inner_join(st_drop_geometry(isocronas_tipificacao) %>%
                 transmute(id_lote, isocrona),
               by = "id_lote") %>%
    select(-id_lote) %>%
    mutate(situacao_transp = if_else(tipo_transp != "METRÔ", "OPERANDO", situacao_transp),
           distancia_cbd = round(as.numeric(st_distance(., cbd)) / 1000, 1)) %>%
    relocate(isocrona, .before = geometry)
  
  # 7. Aplica regras de negócio ---------------------------------------------
  
  # A - EETU's por estação
  zonas_eetu_por_estacao_no_geo <- zonas_eetu_ajust %>%
    st_drop_geometry() %>%
    group_by(eixo, tipo_infra
    ) %>%
    summarize(
      situacao_transp = first(situacao_transp, na_rm = TRUE),
      n_quadras_eetu = length(which(tipo_eixo == "EETU")),
      n_quadras_decreto = length(which(tipo_eixo == "Decreto")),
      n_quadras_total = n_quadras_eetu + n_quadras_decreto,
      area_eetu = sum(area_zoneamento[which(tipo_eixo == "EETU")], na.rm = TRUE) / 1000000,
      area_decreto = sum(area_zoneamento[which(tipo_eixo == "Decreto")], na.rm = TRUE)  / 1000000,
      area_total = area_eetu + area_decreto,
    ) %>%
    ungroup()
  
  #
  zonas_eetu_por_estacao <- eixos %>%
    group_by(eixo, tipo_infra) %>%
    summarize(
      tipo_transp = if_else(length(eixo) > 1, "METRÔ", first(tipo_transp)),
      geometry = first(st_centroid(geometry)),
      across(everything(), first)
    ) %>%
    ungroup() %>% 
    left_join(zonas_eetu_por_estacao_no_geo %>%
                group_by(eixo) %>%
                select(-situacao_transp) %>%
                summarize(across(everything(), first)),
              by = c("eixo", "tipo_infra"
              )) %>%
    # Problema: estações são pontos, corredores são linhas
    st_cast("POINT") %>%
    st_as_sf()
  
  # B - EETU's por distrito
  zonas_eetu_por_distrito_no_geo <- zonas_eetu_ajust %>%
    # select(-names(quadras_ajust)) %>%
    st_drop_geometry() %>%
    group_by(distrito) %>%
    summarize(
      n_quadras_eetu = length(which(tipo_eixo == "EETU")),
      n_quadras_decreto = length(which(tipo_eixo == "Decreto")),
      n_quadras_total = n_quadras_eetu + n_quadras_decreto,
      area_eetu = sum(area_zoneamento[which(tipo_eixo == "EETU")], na.rm = TRUE) / 1000000,
      area_decreto = sum(area_zoneamento[which(tipo_eixo == "Decreto")], na.rm = TRUE)  / 1000000,
      area_total = area_eetu + area_decreto
    ) %>%
    ungroup() %>%
    left_join(distritos %>% 
                transmute(distrito = ds_nome,
                          area_distrito = unclass(st_area(.)) / 1000000) %>%
                st_drop_geometry(),
              by = "distrito") %>%
    mutate(
      area_total_perc = area_total/area_distrito)
  
  #
  zonas_eetu_por_distrito <- distritos %>%
    transmute(distrito = ds_nome) %>%
    left_join(zonas_eetu_por_distrito_no_geo, by = "distrito")
  
  
  # C - EETU's por isocrona 
  zonas_eetu_por_isocrona_no_geo <- zonas_eetu_ajust %>%
    st_drop_geometry() %>%
    group_by(isocrona, tipo_transp) %>%
    summarize(
      n_quadras_eixo = n(),
      n_quadras_eetu = length(which(tipo_eixo == "EETU")),
      n_quadras_decreto = length(which(tipo_eixo == "Decreto")),
      n_quadras_total = n_quadras_eetu + n_quadras_decreto,
      area_eetu = sum(area_zoneamento[which(tipo_eixo == "EETU")], na.rm = TRUE),
      area_decreto = sum(area_zoneamento[which(tipo_eixo == "Decreto")], na.rm = TRUE),
      area_total = area_eetu + area_decreto,
    ) %>%
    ungroup()
  
  
  #
  # zonas_eetu_ajust %>% st_drop_geometry() %>% count(is.na(eixo))

# 6. Exporta para datalake -----------------------------------------------------

#
fs::dir_create(here("inputs", "2_trusted", "Eixos"))

#
sfarrow::st_write_parquet(zonas_eetu_ajust,
                          here("inputs", "2_trusted", "Eixos",
                               "zonas_eetu.parquet"))

#
sfarrow::st_write_parquet(zonas_eetu_por_estacao,
                          here("inputs", "2_trusted", "Eixos",
                               "zonas_eetu_por_estacao.parquet"))

#
sfarrow::st_write_parquet(zonas_eetu_por_distrito,
                          here("inputs", "2_trusted", "Eixos",
                               "zonas_eetu_por_distrito.parquet"))

#
arrow::write_parquet(zonas_eetu_por_isocrona_no_geo,
                     here("inputs", "2_trusted", "Eixos",
                          "zonas_eetu_por_isocrona_no_geo.parquet"))

# 7. Exporta para usuário ------------------------------------------------------

# # Zonas EETU
# fs::dir_create(here("outputs", "Eixos", "zonas_eetu"))
# 
# #
# st_write(zonas_eetu_ajust, here("outputs", "Eixos", "zonas_eetu",
#                                 "zonas_eetu.shp"),
#          layer="zonas_eetu", delete_layer = TRUE, driver="ESRI Shapefile")
# 
# #
# write_csv(data.frame(labels = names(zonas_eetu_ajust)), 
#           here("outputs", "Eixos", "zonas_eetu", "zonas_eetu_labels.csv"))
# 
# # Por Estação
# fs::dir_create(here("outputs", "Eixos", "zonas_eetu_por_estacao"))
# 
# #
# st_write(zonas_eetu_por_estacao, 
#          here("outputs", "Eixos", "zonas_eetu_por_estacao", "zonas_eetu_por_estacao.shp"),
#          layer="zonas_eetu_por_estacao", delete_layer = TRUE, 
#          driver="ESRI Shapefile")
# 
# #
# write_csv(data.frame(labels = names(zonas_eetu_por_estacao)), 
#           here("outputs", "Eixos", "zonas_eetu_por_estacao",
#                "zonas_eetu_por_estacao_labels.csv"))
# 
# # Por Distrito
# fs::dir_create(here("outputs", "Eixos", "zonas_eetu_por_distrito"))
# 
# #
# st_write(zonas_eetu_por_distrito, 
#          here("outputs", "Eixos", "zonas_eetu_por_estacao", "zonas_eetu_por_distrito.shp"),
#          layer="zonas_eetu_por_distrito", delete_layer = TRUE, 
#          driver="ESRI Shapefile")
# 
# # 
# write_csv(data.frame(labels = names(zonas_eetu_por_distrito)), 
#           here("outputs", "Eixos", "zonas_eetu_por_estacao",
#                "zonas_eetu_por_distrito_labels.csv"))
# 
# # Por isócrona
# fs::dir_create(here("outputs", "Eixos", "zonas_eetu_por_isocrona"))
# 
# #
# writexl::write_xlsx(
#   zonas_eetu_por_isocrona_no_geo,
#   here("outputs", "Eixos", "zonas_eetu_por_isocrona",
#        "zonas_eetu_por_isocrona_no_geo"))

# 8. Compara --------------------------------------------------------------

# #
# eixos_old <- sfarrow::st_read_parquet(here("outputs", "Eixos", "eixos.parquet")) %>%
#   st_transform(4674)
# 
# #
# eixos_old_count <- eixos_old %>%
#   st_drop_geometry() %>%
#   # filter(TipoTransp != "Ônibus") %>%
#   count(NomeTransp, TipoTransp) %>%
#   arrange(desc(n))
# 
# #
# eixos_count <- eixos %>%
#   st_drop_geometry() %>%
#   # filter(tipo_transp != "Ônibus") %>%
#   count(eixo, tipo_transp, emt_situac) %>%
#   arrange(desc(n))
# 
# #
# eixos_compair <- eixos_count %>%
#   left_join(eixos_old_count, by = c("eixo" = "NomeTransp",
#                                     "tipo_transp" = "TipoTransp"), 
#             suffix = c("", "_old")) %>%
#   mutate(n_old = if_else(is.na(n_old) | emt_situac == "PROJETADA", 0, n_old),
#          diff = n - n_old) 
# 
# # 1129 eixos de 5342 (21.1%)
# eixos_na <- eixos %>%
#   filter(is.na(eixo))
# 
# #
# eixos_na %>%
#   st_drop_geometry() %>%
#   count(dist_metro <= 600 | dist_trem <= 600 | dist_corredor <= 300)
