# Pacotes
library(here)
library(fs)
library(beepr)
library(sf)
library(tidyverse)
library(tidylog)
library(skimr)
library(sfarrow)
library(future)
library(furrr)


# Funções
source(here("scripts", "functions.R"))

# Opções
options(scipen = 99999, error = beep)
# future::plan("multisession", workers = future::availableCores() - 1)


# 1. Importa -------------------------------------------------------------------


# Sistema geográfico de referência
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

# Padrão regex para os objetos que queremos encontrar na lista de arquivos
files <- c("Eixos_EETU", "CorredorOnibus", "EstacaoMetro", "EstacaoTrem", 
           "Corredores", "CorredorItaquera", "Operacao", "AIU",
           "Distrito", "Macroareas")

#
get_shapefiles(
  files = files
)


## Praça da Sé como CBD
geo_sp_cbd <- data.frame(long = -46.633959, lat= -23.550382) %>%
  st_as_sf(coords = c("long", "lat"),
           crs = 4674) %>%
  st_transform(crs = CRS)


#
isocronas <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos_por_isocrona.parquet")
)

#
zonas_eetu <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu.parquet")
)

#
zonas_eetu_lotes <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu_lotes.parquet")) %>%
  filter(lo_tp_lote == "F")

#
eixos <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos.parquet")
)

#
eixos_por_buffer <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos_por_buffer.parquet")
)

#
buffer_externo_nova_regra <- eixos_por_buffer %>%
  filter((tipo_infra == "ESTAÇÃO" & buffer == 1000) | 
           (tipo_infra == "CORREDOR" & buffer == 450))

# Lotes
lotes <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao",
       "lotes.parquet")) %>%
  filter(lo_tp_lote == "F") %>%
  # Áreas de Operação Urbana não podem ser EETU
  filter(is.na(operacao_urbana) | 
           operacao_urbana == "AGUA BRANCA (perímetro expandido)") %>%
  # Arco Tietê incluído na nova regra
  filter(!mem_setor %in% c("ARCO TAMANDUATEI", "ARCO JURUBATUBA", 
                           "ARCO PINHEIROS") |
           is.na(mem_setor)) %>%
  # Somente Perímetro expandido do PIU Setor Central é incluído na nova regra
  filter(is.na(aiu_perimetro) | aiu_perimetro == "Expandido") %>%
  # Zonas Residenciais não podem ser EETU
  filter(str_starts(zoneamento, "ZER|ZOE|ZEP|ZEIS|ZCOR|ZPDS|ZC-ZEIS|ZPR", 
                    negate = TRUE)) %>%
  st_filter(buffer_externo_nova_regra, .predicate = st_within) %>%
  mutate(id_lote = rownames(.) %>% as.numeric())


# 2. Estrutura -----------------------------------------------------------------

#
# zoneamento_ajust <- zonas_eetu %>%
#   # filter(zoneamento == "ZEU" | zoneamento == "ZEUa") %>%
#   st_join(eixos_eetu_ativados_decreto %>%
#             transmute(decreto_eixo = ed_decreto),
#           largest = TRUE) %>%
#   mutate(
#     tipo_transp = str_to_title(tipo_transp),
#     id = rownames(.) %>% as.numeric())


# #
# lotes_parts <- split(
#   x = lotes %>%
#     mutate(id_lote = rownames(.) %>% as.numeric()),
#   f = cut(seq_len(nrow(lotes)),
#           as.numeric(future::availableCores() - 1),
#           labels = FALSE)
# )
# 
# #
# lotes_ajust_list <- future_map(
#   lotes_parts,
#   ~ .x %>% st_filter(buffer_externo_nova_regra, .predicate = st_within))
# 
# #
# lotes_ajust <- bind_rows(lotes_ajust_list) %>%
#   # lotes %>%
#   # st_join(quadras_ajust %>% select(qd_id), largest = TRUE) %>%
#   transmute(id_lote = rownames(.) %>% as.numeric(),
#             id_zoneamento)

lotes_ajust <- lotes %>%
  transmute(id_lote, 
            id_zoneamento)

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

# Tomando o id da infra de transporte mais próxima
nearest_metro_lotes <- 
  tibble(
    id_lote = rownames(lotes_ajust) %>% as.numeric(),
    id_metro = st_nearest_feature(lotes_ajust, eixos_metro),
    # dist_metro = st_distance(
    #   lotes_ajust, 
    #   estacao_metro_ajust[st_nearest_feature(lotes_ajust, estacao_metro_ajust),], 
    #   by_element = TRUE) %>% as.vector()
  ) 

#
nearest_trem_lotes <- 
  tibble(
    id_lote = rownames(lotes_ajust) %>% as.numeric(),
    id_trem = st_nearest_feature(lotes_ajust, eixos_trem),
    # dist_trem = st_distance(
    #   lotes_ajust, 
    #   estacao_trem[st_nearest_feature(lotes_ajust, estacao_trem),], 
    #   by_element = TRUE) %>% as.vector()
  ) 

#
nearest_corredor_lotes <- 
  tibble(
    id_lote = rownames(lotes_ajust) %>% as.numeric(),
    id_corredor = st_nearest_feature(lotes_ajust, eixos_corredor),
    # dist_corredor = st_distance(
    #   lotes_ajust, 
    #   corredor_onibus_ajust[st_nearest_feature(lotes_ajust, corredor_onibus_ajust),], 
    #   by_element = TRUE) %>% as.vector()
  ) 


# 4. Tipifica eixo ---------------------------------------------------------

#
lotes_metro_1000m <- st_within(lotes_ajust,
                               st_buffer(eixos_metro, dist = 1000),
                               sparse = TRUE) %>%
  as.data.frame() %>% 
  transmute(id_lote = row.id,
            id_metro = col.id,
            metro = 1)

#
lotes_trem_1000m <- st_within(lotes_ajust, 
                              st_buffer(eixos_trem, dist = 1000), 
                              sparse = TRUE) %>%
  as.data.frame() %>%
  transmute(id_lote = row.id,
            id_trem = col.id,
            trem = 1)

##
lotes_corredor_450m <- st_within(lotes_ajust,
                                 st_buffer(eixos_corredor, dist = 450), 
                                 sparse = TRUE) %>%
  as.data.frame() %>%
  transmute(id_lote = row.id,
            id_corredor = col.id,
            corredor = 1)

# Criando key com todos os id's de interesse
eixos_novos_key <- st_drop_geometry(lotes_metro_1000m) %>%
  inner_join(nearest_metro_lotes, by = c("id_lote", "id_metro")) %>%
  full_join(lotes_trem_1000m %>%
              inner_join(nearest_trem_lotes, by = c("id_lote", "id_trem")), 
            by = "id_lote") %>%
  full_join(lotes_corredor_450m %>%
              inner_join(nearest_corredor_lotes, by = c("id_lote", "id_corredor")), 
            by = "id_lote")

# Criando critério para assinalar tipo de Transporte que origina o eixo
eixos_novos_tipificacao <- eixos_novos_key %>%
  transmute(
    id_lote, id_metro, id_trem, id_corredor,
    # dist_metro, dist_trem, dist_corredor,
    tipo_transp = case_when(metro == 1 ~ "METRÔ",
                            trem == 1 ~ "TREM",
                            corredor == 1 ~ "ÔNIBUS"),
    # tipo_eixo = if_else(!is.na(tipo_transp), "Nova regra", NA_character_)
  ) %>%
  left_join(st_drop_geometry(eixos_metro) %>%
              transmute(id_metro = row_number(),
                        eixo_metro = eixo, tipo_transp,
                        tipo_infra_metro = tipo_infra,
                        situacao_metro = situacao_transp),
            by = c("id_metro", "tipo_transp")) %>%
  left_join(st_drop_geometry(eixos_trem) %>%
              transmute(id_trem = row_number(),
                        eixo_trem = eixo, tipo_transp,
                        tipo_infra_trem = tipo_infra,
                        situacao_trem = situacao_transp),
            by = c("id_trem", "tipo_transp")) %>%
  left_join(st_drop_geometry(eixos_corredor) %>%
              transmute(id_corredor = row_number(),
                        eixo_corredor = eixo, 
                        tipo_infra_corredor = tipo_infra,
                        tipo_transp, situacao_corredor = situacao_transp),
            by = c("id_corredor", "tipo_transp")) %>%
  transmute(
    id_lote,
    eixo = coalesce(eixo_metro, eixo_trem, eixo_corredor),
    tipo_transp,
    tipo_infra = coalesce(tipo_infra_metro, tipo_infra_trem, tipo_infra_corredor),
    situacao = coalesce(situacao_metro, situacao_trem, situacao_corredor),
    # dist_transp = dist,
    tipo_eixo = if_else(!is.na(tipo_transp), "Nova regra", NA_character_))

# 5. Integra dados -------------------------------------------------------------


# § 1º Ficam excluídas das áreas de influência dos eixos:
#   
# I - as Zonas Exclusivamente Residenciais - ZER;
# 
# II - as Zonas de Ocupação Especial - ZOE;
# 
# III - as Zonas Especiais de Preservação Ambiental - ZEPAM;
# 
# IV - as Zonas Especiais de Interesse Social - ZEIS;
# 
# V - os perímetros das operações urbanas conforme estabelecido na legislação em vigor;
# 
# VI - as Zonas Especiais de Preservação Cultural - ZEPEC;
# 
# VII - as áreas que integram o Sistema de Áreas Protegidas, Áreas Verdes e Espaços Livres;
# 
# VIII - as áreas contidas na Macroárea de Estruturação Metropolitana, nos subsetores:
# a) Arco Tietê;
# b) Arco Tamanduateí;
# c) Arco Pinheiros;
# d) Arco Jurubatuba.

# 
eixos_novos_no_geo <- lotes_ajust %>%
  st_drop_geometry() %>%
  left_join(lotes %>%
              mutate(id_lote = rownames(.) %>% as.numeric()) %>%
              st_drop_geometry(),
            by = "id_lote") %>%
  # Áreas de Operação Urbana não podem ser EETU
  filter(is.na(operacao_urbana) | 
           operacao_urbana == "AGUA BRANCA (perímetro expandido)") %>%
  # Arco Tietê incluído na nova regra
  filter(!mem_setor %in% c("ARCO TAMANDUATEI", "ARCO JURUBATUBA", 
                           "ARCO PINHEIROS") |
           is.na(mem_setor)) %>%
  # Somente Perímetro expandido do PIU Setor Central é incluído na nova regra
  filter(is.na(aiu_perimetro) | aiu_perimetro == "Expandido") %>%
  # Zonas Residenciais não podem ser EETU
  filter(str_starts(zoneamento, "ZER|ZOE|ZEP|ZEIS|ZCOR|ZPDS|ZC-ZEIS|ZPR", 
                    negate = TRUE)) %>%
  # EETU' já demarcados precisam ser excluídos
  # filter(zoneamento == "ZEU" | zoneamento == "ZEUa", negate = TRUE) %>%
  # select(id_lote) %>%
  inner_join(eixos_novos_tipificacao, by = c("id_lote")) %>%
  # Eixos_tipificacao %>%
  # left_join(st_drop_geometry(eixos_metro) %>%
  #             mutate(id_metro = row_number()), by = "id_metro") %>%
  # left_join(st_drop_geometry(eixos_trem) %>%
  #             mutate(id_trem = row_number()), by = "id_trem") %>%
  # left_join(st_drop_geometry(eixos_corredor) %>%
  #             mutate(id_corredor = row_number()), by = "id_corredor") %>%
  # mutate(
  #   eixo = str_to_title(case_when(
  #     tipo_transp == "METRÔ" ~ paste0("Estação ", eixo),
  #     tipo_transp == "TREM" ~ paste0("Estação ", eixo),
#     tipo_transp == "ÔNIBUS" ~ eixo), 
#     locale = "br"),
# ) %>%
# full_join(st_drop_geometry(eixos_por_quadra)) %>%
group_by(id_lote) %>%
  summarize(
    across(everything(), ~first(na.omit(.x)))
  )

#
# eixos_nova_regra <- 
#   bind_rows(
#     lotes_ajust %>%
#       select(id_lote, id_zoneamento) %>%
#       inner_join(eixos_novos_no_geo),
#     zoneamento %>%
#       filter(zoneamento == "ZEU" | zoneamento == "ZEUa") %>%
#       select(id_zoneamento) %>%
#       mutate(id_lote = NA_real_) %>%
#       inner_join(zoneamento %>% st_drop_geometry())
#   ) %>%
#   mutate(
#     situacao_transp = if_else(tipo_transp != "METRÔ", "OPERANDO", emt_situac),
#     distancia_cbd = round(as.numeric(st_distance(., geo_sp_cbd)) / 1000, 1)
#   ) 

# eixos_eetu <- zoneamento_ajust %>%
#   st_drop_geometry() %>%
#   select(-isocrona, -id)

# zonas_eetu_lotes <- lotes_ajust %>%
#   filter(id_zoneamento %in% zonas_eetu$id_zoneamento) %>%
#   st_join(zonas_eetu, largest = TRUE, left = FALSE)

#
eixos_nova_regra <- bind_rows(
  lotes_ajust %>%
    inner_join(eixos_novos_no_geo %>%
                 filter(zoneamento != "ZEU" & zoneamento != "ZEUa")),
  # zoneamento_ajust %>%
  #   select(id, id_zoneamento) %>%
  #   inner_join(zonas_eetu %>% st_drop_geometry(), by = "id_zoneamento")
  zonas_eetu_lotes
) %>%
  mutate(
    # tipo_eixo = case_when(
    #   !is.na(tipo_transp) & 
    #     (zoneamento == "ZEU" | zoneamento == "ZEUa") ~ "EETU",
    #   !is.na(tipo_transp) ~ "Nova regra",
    #   TRUE ~ NA_character_),
    situacao_transp = if_else(tipo_transp != "METRÔ", "OPERANDO", situacao_transp),
    distancia_cbd = round(as.numeric(st_distance(., geo_sp_cbd)) / 1000, 1)) %>%
  select(lo_setor,
         lo_quadra,
         lo_lote,
         lo_condomi,
         lo_tp_quad,
         lo_tp_lote,
         area_lote,
         id_zoneamento,
         zoneamento,
         zoneamento_grupo,
         mem_setor,
         solo_uso,
         ca_maximo,
         macroarea,
         distrito,
         tipo_eixo,
         eixo,
         tipo_transp,
         situacao_transp,
         distancia_cbd,
         #isocrona
  )

# 6. Tipifica isócrona  ---------------------------------------------
# REGRA MUUUITO LENTA E EXPANSIVA EM MEMÓRIA RAM


# #
rm(lotes, lotes_parts, lotes_ajust_list, lotes_ajust, zonas_eetu_lotes,
   lotes_corredor_450m, lotes_metro_1000m, lotes_trem_1000m,
   eixos_novos_key, eixos_novos_no_geo, eixos_novos_tipificacao,
   eixos_eetu, eixos_metro, eixos_corredor, eixos_por_buffer, zoneamento_ajust,
   nearest_corredor_lotes, nearest_metro_lotes, nearest_trem_lotes)

# #
# eixos_nova_regra <- sfarrow::st_read_parquet(
#   here("inputs", "2_trusted", "Eixos",
#        "zonas_eetu_lotes_contidas_450m_1000m.parquet"))
# 
# #
# isocronas_tipificacao <- eixos_nova_regra %>%
#   transmute(id_lote = rownames(.) %>% as.numeric,
#             tipo_transp) %>%
#   # slice_sample(prop = 0.01) %>%
#   st_join(isocronas %>%
#             filter(tipo_transp == "METRÔ") %>%
#             transmute(isocrona_metro = isocrona),
#           largest = FALSE) %>%
#   st_join(isocronas %>%
#             filter(tipo_transp == "TREM") %>%
#             transmute(isocrona_trem = isocrona),
#           largest = FALSE) %>%
#   st_join(isocronas %>%
#             filter(tipo_transp == "ÔNIBUS") %>%
#             transmute(isocrona_onibus = isocrona),
#           largest = FALSE) %>%
#   group_by(id_lote) %>%
#   mutate(
#     isocrona = case_when(
#       tipo_transp == "METRÔ" ~ isocrona_metro,
#       tipo_transp == "TREM" ~ isocrona_trem,
#       tipo_transp == "ÔNIBUS" ~ isocrona_onibus,
#     ),
#     teste = case_when(
#       tipo_transp == "METRÔ" & isocrona_metro == min(isocrona_metro) ~ "A",
#       tipo_transp == "TREM" & isocrona_trem == min(isocrona_trem) ~ "B",
#       tipo_transp == "ÔNIBUS" & isocrona_onibus == min(isocrona_onibus) ~ "C",
#       is.na(isocrona) ~ "D",
#       TRUE ~ NA_character_)) %>%
#   group_by(id_lote, tipo_transp, teste) %>%
#   summarize(across(everything(), first)) %>%
#   ungroup() %>%
#   filter(!is.na(teste))
# 
# #
# eixos_nova_regra_ajust <- eixos_nova_regra %>%
#   mutate(id_lote = rownames(.) %>% as.numeric()) %>%
#   inner_join(st_drop_geometry(isocronas_tipificacao) %>%
#                transmute(id_lote, isocrona),
#              by = "id_lote") %>%
#   select(-id_lote) %>%
#   relocate(isocrona, .before = geometry)


#
eixos_nova_regra_ajust <- eixos_nova_regra


# 7. Aplica regras de negócio  ---------------------------------------------

#
eixos_nova_regra_ajust <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu_lotes_contidas_450m_1000m.parquet"))


# A - Por estação/ infra de transporte
#
eixos_nova_regra_por_estacao_no_geo <- eixos_nova_regra_ajust %>%
  mutate(eixo = str_to_upper(eixo)) %>%
  st_drop_geometry() %>%
  group_by(#macroarea, mem_setor, 
    eixo#, tipo_transp
  ) %>%
  summarize(
    n_lotes_eetu_2016 = length(which(tipo_eixo == "EETU")),
    n_lotes_eetu_decreto = length(which(tipo_eixo == "Decreto")),
    n_lotes_eetu_atual = n_lotes_eetu_2016 + n_lotes_eetu_decreto,
    n_lotes_eetu_novos = length(which(tipo_eixo == "Nova regra")),
    n_lotes_eetu_novos_arco_tiete = length(which(mem_setor == "ARCO TIETE")),
    area_eetu_2016 = sum(area_lote[which(tipo_eixo == "EETU")], na.rm = TRUE) / 1000000,
    area_eetu_decreto = sum(area_lote[which(tipo_eixo == "Decreto")], na.rm = TRUE) / 1000000,
    area_eetu_atual = area_eetu_2016 + area_eetu_decreto,
    area_eetu_novos = sum(area_lote[which(tipo_eixo == "Nova regra")], na.rm = TRUE) / 1000000,
    var_perc_atual_novos = if_else(area_eetu_novos != 0 & area_eetu_atual != 0,
                                   (area_eetu_atual+area_eetu_novos)/area_eetu_atual - 1,
                                   NA_real_),
    area_eetu_novo_arco_tiete = sum(area_lote[which(mem_setor == "ARCO TIETE")] / 1000000, 
                                    na.rm = TRUE)#,
  ) %>%
  ungroup()

#
eixos_nova_regra_por_estacao <- eixos %>%
  group_by(eixo) %>%
  summarize(
    tipo_transp = if_else(length(eixo) > 1, "METRÔ", first(tipo_transp)),
    geometry = first(st_centroid(geometry)),
    across(everything(), first)
  ) %>%
  ungroup() %>% 
  left_join(eixos_nova_regra_por_estacao_no_geo %>%
              group_by(eixo) %>%
              summarize(across(everything(), first)),
            by = c("eixo"#, "tipo_transp"
            )) %>%
  st_cast("POINT")

# B - Por distrito
eixos_nova_regra_por_distrito_no_geo <- eixos_nova_regra_ajust %>%
  # select(-names(quadras_ajust)) %>%
  st_drop_geometry() %>%
  group_by(distrito) %>%
  summarize(
    n_lotes_eetu_2016 = length(which(tipo_eixo == "EETU")),
    n_lotes_eetu_decreto = length(which(tipo_eixo == "Decreto")),
    n_lotes_eetu_atual = n_lotes_eetu_2016 + n_lotes_eetu_decreto,
    n_lotes_eetu_novos = length(which(tipo_eixo == "Nova regra")),
    n_lotes_eetu_novos_arco_tiete = length(which(mem_setor == "ARCO TIETE")),
    area_eetu_2016 = sum(area_lote[which(tipo_eixo == "EETU")], na.rm = TRUE) / 1000000,
    area_eetu_decreto = sum(area_lote[which(tipo_eixo == "Decreto")], na.rm = TRUE) / 1000000,
    area_eetu_atual = area_eetu_2016 + area_eetu_decreto,
    area_eetu_novos = sum(area_lote[which(tipo_eixo == "Nova regra")], na.rm = TRUE) / 1000000,
    var_perc_atual_novos = if_else(area_eetu_novos != 0 & area_eetu_atual != 0,
                                   (area_eetu_atual+area_eetu_novos)/area_eetu_atual - 1,
                                   NA_real_),
    area_eetu_novo_arco_tiete = sum(area_lote[which(mem_setor == "ARCO TIETE")] / 1000000, 
                                    na.rm = TRUE),
    distancia_media_cbd = mean(distancia_cbd)
  ) %>%
  ungroup() %>%
  left_join(distritos %>% 
              transmute(distrito = ds_nome,
                        area_distrito = unclass(st_area(.)) / 1000000) %>%
              st_drop_geometry(),
            by = "distrito") %>%
  mutate(
    area_eetu_atual_perc_distrito = area_eetu_atual/area_distrito,
    area_eetu_novo_perc_distrito = area_eetu_novos/area_distrito)

#
eixos_nova_regra_por_distrito <- distritos %>%
  transmute(distrito = ds_nome) %>%
  left_join(eixos_nova_regra_por_distrito_no_geo, by = "distrito")


# Por isocrona 
# eixos_nova_regra_por_isocrona_no_geo <- eixos_nova_regra_ajust %>%
#   st_drop_geometry() %>%
#   group_by(#macroarea, mem_setor,
#     # eixo,
#     isocrona, tipo_transp) %>%
#   summarize(
#     n_lotes_eetu_2016 = length(which(tipo_eixo == "EETU")),
#     n_lotes_eetu_decreto = length(which(tipo_eixo == "Decreto")),
#     n_lotes_eetu_atual = n_lotes_eetu_2016 + n_lotes_eetu_decreto,
#     n_lotes_eetu_novos = length(which(tipo_eixo == "Nova regra")),
#     n_lotes_eetu_novos_arco_tiete = length(which(mem_setor == "ARCO TIETE")),
#     area_eetu_2016 = sum(area_lote[which(tipo_eixo == "EETU")], na.rm = TRUE) / 1000000,
#     area_eetu_decreto = sum(area_lote[which(tipo_eixo == "Decreto")], na.rm = TRUE),
#     area_eetu_atual = area_eetu_2016 + area_eetu_decreto,
#     area_eetu_novos = sum(area_lote[which(tipo_eixo == "Nova regra")], na.rm = TRUE) / 1000000,
#     var_perc_atual_novos = if_else(area_eetu_novos != 0 & area_eetu_atual != 0,
#                                    (area_eetu_atual+area_eetu_novos)/area_eetu_atual - 1,
#                                    NA_real_),
#     area_eetu_novo_arco_tiete = sum(area_lote[which(mem_setor == "ARCO TIETE")] / 1000000,
#                                     na.rm = TRUE),
#     distancia_media_cbd = mean(distancia_cbd)
#   ) %>%
#   ungroup()



# 8. Salva no datalake --------------------------------------------------------

#
sfarrow::st_write_parquet(eixos_nova_regra_ajust,
                          here("inputs", "2_trusted", "Eixos",
                               "zonas_eetu_lotes_contidas_450m_1000m.parquet"))

#
sfarrow::st_write_parquet(eixos_nova_regra_por_estacao,
                          here("inputs", "2_trusted", "Eixos",
                               "zonas_eetu_lotes_contidas_450m_1000m_por_estacao.parquet"))

#
sfarrow::st_write_parquet(eixos_nova_regra_por_distrito,
                          here("inputs", "2_trusted", "Eixos",
                               "zonas_eetu_lotes_contidas_450m_1000m_por_distrito.parquet"))

#
beepr::beep(2)
