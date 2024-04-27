# Pacotes
library(here)
library(fs)
library(beepr)
library(sf)
library(tidyverse)
library(tidylog)
library(skimr)
library(sfarrow)

# Funções
source(here("scripts", "functions.R"))

# Opções
options(scipen = 99999,
        error = beep)

# 1. Importa -------------------------------------------------------------------

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
zoneamento <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Legislacao",
       "zoneamento.parquet"))

#
zonas_eetu <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu.parquet")
)

#
eixos <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos.parquet")
)

# #
# isocronas_metro <- sfarrow::st_read_parquet(
#   here("inputs", "2_trusted", "Transporte",
#        "eixos_metro_por_isocrona.parquet"))
# 
# #
# isocronas_trem <- sfarrow::st_read_parquet(
#   here("inputs", "2_trusted", "Transporte",
#        "eixos_trem_por_isocrona.parquet"))
# 
# #
# isocronas_onibus <- sfarrow::st_read_parquet(
#   here("inputs", "2_trusted", "Transporte",
#        "eixos_onibus_por_isocrona.parquet"))


#
isocronas <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos_por_isocrona.parquet")
)

# 2. Estrutura -----------------------------------------------------------------

# Juntando os eixos (EETU e por decreto)
zoneamento_ajust <- zoneamento %>%
  # filter(zoneamento == "ZEU" | zoneamento == "ZEUa") %>%
  st_join(eixos_eetu_ativados_decreto %>%
            transmute(decreto_eixo = ed_decreto),
          largest = TRUE) %>%
  mutate(
    id = rownames(.) %>% as.numeric())

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
nearest_metro_quadras <- 
  tibble(
    id = rownames(zoneamento_ajust) %>% as.numeric(),
    id_metro = st_nearest_feature(zoneamento_ajust, eixos_metro),
    dist_metro = st_distance(
      zoneamento_ajust, 
      eixos_metro[st_nearest_feature(zoneamento_ajust, eixos_metro),], 
      by_element = TRUE) %>% as.vector()
  ) 

#
nearest_trem_quadras <- 
  tibble(
    id = rownames(zoneamento_ajust) %>% as.numeric(),
    id_trem = st_nearest_feature(zoneamento_ajust, eixos_trem),
    dist_trem = st_distance(
      zoneamento_ajust, 
      estacao_trem[st_nearest_feature(zoneamento_ajust, eixos_trem),], 
      by_element = TRUE) %>% as.vector()
  ) 

#
nearest_corredor_quadras <- 
  tibble(
    id = rownames(zoneamento_ajust) %>% as.numeric(),
    id_corredor = st_nearest_feature(zoneamento_ajust, eixos_corredor),
    dist_corredor = st_distance(
      zoneamento_ajust, 
      eixos_corredor[st_nearest_feature(zoneamento_ajust, eixos_corredor),], 
      by_element = TRUE) %>% as.vector()
  ) 

# 4. Funções ---------------------------------------------------------

#
get_zonas_eetu <- function(dist_alta_capacidade, dist_media_capacidade) {
  
  # A - Tipifica eixo por estação/corredor mais próximo
  
  # Metrô
  quadras_metro <- st_within(zoneamento_ajust,
                             st_buffer(eixos_metro, dist = dist_alta_capacidade[2]),
                             sparse = TRUE) %>%
    as.data.frame() %>%
    transmute(id = row.id,
              id_metro = col.id,
              metro = 1) %>% # identificador binário p/ assinalar infra transporte
    # Máximo de 1000m e mínimo de 800m
    inner_join(st_intersects(zoneamento_ajust,
                             st_buffer(eixos_metro, dist = dist_alta_capacidade[1]),
                             sparse = TRUE) %>%
                 as.data.frame() %>%
                 transmute(id = row.id,
                           id_metro = col.id,
                           metro = 1))
  
  # Trem
  quadras_trem <- st_within(zoneamento_ajust, st_buffer(eixos_trem, dist = dist_alta_capacidade[2]),
                            sparse = TRUE) %>%
    as.data.frame() %>%
    transmute(id = row.id,
              id_trem = col.id,
              trem = 1) %>%
    inner_join(st_intersects(zoneamento_ajust,
                             st_buffer(eixos_trem, dist = dist_alta_capacidade[1]),
                             sparse = TRUE) %>%
                 as.data.frame() %>%
                 transmute(id = row.id,
                           id_trem = col.id,
                           trem = 1))
  
  # Ônibus
  quadras_corredor <- st_within(zoneamento_ajust,
                                st_buffer(eixos_corredor, dist = dist_media_capacidade[2]),
                                sparse = TRUE) %>%
    as.data.frame() %>%
    transmute(id = row.id,
              id_corredor = col.id,
              corredor = 1) %>%
    inner_join(st_intersects(zoneamento_ajust,
                             st_buffer(eixos_corredor, dist = dist_media_capacidade[1]),
                             sparse = TRUE) %>%
                 as.data.frame() %>%
                 transmute(id = row.id,
                           id_corredor = col.id,
                           corredor = 1))
  
  # Criando key com todos os id's de interesse
  zonas_eetu_key <- st_drop_geometry(quadras_metro) %>%
    inner_join(nearest_metro_quadras, by = c("id", "id_metro")) %>%
    full_join(quadras_trem %>%
                inner_join(nearest_trem_quadras, by = c("id", "id_trem")),
              by = "id") %>%
    full_join(quadras_corredor %>%
                inner_join(nearest_corredor_quadras, by = c("id", "id_corredor")),
              by = "id")
  
  # Criando critério para assinalar tipo de Transporte que origina o eixo
  zonas_eetu_tipificacao <- zonas_eetu_key %>%
    transmute(
      id, id_metro, id_trem, id_corredor,
      dist_metro, dist_trem, dist_corredor,
      tipo_transp = case_when(metro == 1 ~ "METRÔ",
                              trem == 1 ~ "TREM",
                              corredor == 1 ~ "ÔNIBUS"),
      tipo_eixo = if_else(!is.na(tipo_transp), "Nova regra", NA_character_),
      dist = case_when(
        metro == 1 ~ dist_metro,
        trem == 1 ~ dist_trem,
        corredor == 1 ~ dist_corredor,
        TRUE ~ NA_real_)
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
      id,
      eixo = coalesce(eixo_metro, eixo_trem, eixo_corredor),
      tipo_transp,
      tipo_infra = coalesce(tipo_infra_metro, tipo_infra_trem, tipo_infra_corredor),
      situacao = coalesce(situacao_metro, situacao_trem, situacao_corredor),
      dist_transp = dist,
      tipo_eixo = if_else(!is.na(tipo_transp), "Nova regra", NA_character_))
  
  
  # 
  zonas_eetu_novos_no_geo <- zoneamento_ajust %>%
    st_drop_geometry() %>%
    
    ### Restrições ao EETU
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
    # Quadras de EETU já demarcadas precisam ser excluídas
    anti_join(
      st_drop_geometry(zoneamento_ajust) %>%
        filter(zoneamento == "ZEU" | zoneamento == "ZEUa"),
      by = c("id")) %>%
    ### 
    
    inner_join(zonas_eetu_tipificacao, by = c("id" = "id")) %>%
    mutate(
      tipo_eixo = case_when(
        zoneamento %in% c("ZEU", "ZEUa") & !is.na(decreto_eixo) ~ "Decreto",
        zoneamento %in% c("ZEU", "ZEUa") & is.na(decreto_eixo) ~ "EETU",
        !is.na(eixo) ~ tipo_eixo,
        TRUE ~ NA_character_
      )
    ) %>%
    group_by(id) %>%
    summarize(across(everything(), ~first(na.omit(.x)))) %>%
    ungroup()
  
  #
  zonas_eetu_demarcadas <- zonas_eetu %>%
    st_drop_geometry() %>%
    select(-isocrona)
  
  #
  zonas_eetu_tipificados <- bind_rows(
    zoneamento_ajust %>%
      select(id) %>%
      inner_join(zonas_eetu_novos_no_geo, by = "id"),
    zoneamento_ajust %>%
      select(id, id_zoneamento) %>%
      inner_join(zonas_eetu_demarcadas, by = "id_zoneamento")
  ) %>%
    mutate(
      tipo_eixo = case_when(
        zoneamento %in% c("ZEU", "ZEUa") & !is.na(decreto_eixo) ~ "Decreto",
        zoneamento %in% c("ZEU", "ZEUa") & is.na(decreto_eixo) ~ "EETU",
        !is.na(eixo) ~ tipo_eixo,
        TRUE ~ NA_character_),
      situacao_transp = if_else(tipo_transp != "METRÔ", "OPERANDO", situacao_transp),
      distancia_cbd = round(as.numeric(st_distance(., geo_sp_cbd)) / 1000, 1)
    ) %>%
    filter(!is.na(tipo_transp))
  
  
  
  #  B - Integra informação de isócronas
  
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
  zonas_eetu_tipificados_ajust <- zonas_eetu_tipificados %>%
    mutate(id_lote = rownames(.) %>% as.numeric()) %>%
    inner_join(st_drop_geometry(isocronas_tipificacao) %>%
                 transmute(id_lote, isocrona),
               by = c("id_lote")) %>%
    select(-id_lote) %>%
    # left_join(zonas_eetu %>%
    #             st_drop_geometry() %>%
    #             transmute(id, isocrona_eetu = isocrona)) %>%
    # mutate(isocrona = if_else(tipo_eixo == "EETU" | tipo_eixo == "Decreto",
    #                           isocrona_eetu, isocrona)) %>%
    relocate(isocrona, .before = geometry)
  
  
  return(zonas_eetu_tipificados_ajust)
}

#
get_zonas_eetu_por_estacao <- function(data) {
  
  #
  zonas_eetu_por_estacao_no_geo <- data %>%
    mutate(eixo = str_to_upper(eixo)) %>%
    st_drop_geometry() %>%
    group_by(#macroarea, mem_setor, 
      eixo, tipo_infra, #situacao_transp
    ) %>%
    summarize(
      # situacao_transp = first(situacao_transp, na_rm = TRUE),
      n_quadras_eetu_2016 = length(which(tipo_eixo == "EETU")),
      n_quadras_eetu_decreto = length(which(tipo_eixo == "Decreto")),
      n_quadras_eetu_atual = n_quadras_eetu_2016 + n_quadras_eetu_decreto,
      n_quadras_eetu_novos = length(which(tipo_eixo == "Nova regra")),
      n_quadras_eetu_novos_arco_tiete = length(which(mem_setor == "ARCO TIETE")),
      area_eetu_2016 = sum(area_zoneamento[which(tipo_eixo == "EETU")], na.rm = TRUE) / 1000000,
      area_eetu_decreto = sum(area_zoneamento[which(tipo_eixo == "Decreto")], na.rm = TRUE)  / 1000000,
      area_eetu_atual = area_eetu_2016 + area_eetu_decreto,
      area_eetu_novos = sum(area_zoneamento[which(tipo_eixo == "Nova regra")], na.rm = TRUE) / 1000000,
      var_perc_atual_novos = if_else(area_eetu_novos != 0 & area_eetu_atual != 0,
                              (area_eetu_atual+area_eetu_novos)/area_eetu_atual - 1,
                              NA_real_),
      area_eetu_novo_arco_tiete = sum(area_zoneamento[which(mem_setor == "ARCO TIETE")] / 1000000, 
                                      na.rm = TRUE),
      var_perc_atual_novos_arco_tiete = if_else(area_eetu_novo_arco_tiete != 0 & area_eetu_atual != 0,
                                         (area_eetu_novo_arco_tiete + area_eetu_atual)/area_eetu_atual -1,
                                         NA_real_)
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
                group_by(eixo, tipo_infra) %>%
                summarize(across(everything(), first)),
              by = c("eixo", "tipo_infra"#, "situacao_transp"
              )) %>%
    st_cast("POINT") %>%
    st_as_sf()
  
  return(zonas_eetu_por_estacao)
}

#
get_zonas_eetu_por_distrito <- function(data) {
  
  # B - Por distrito
  zonas_eetu_por_distrito_no_geo <- data %>%
    # select(-names(quadras_ajust)) %>%
    st_drop_geometry() %>%
    group_by(distrito) %>%
    summarize(
      n_quadras_eetu_2016 = length(which(tipo_eixo == "EETU")),
      n_quadras_eetu_decreto = length(which(tipo_eixo == "Decreto")),
      n_quadras_eetu_novas = length(which(tipo_eixo == "Nova regra")),
      n_quadras_eetu_novos_arco_tiete = length(which(mem_setor == "ARCO TIETE")),
      n_quadras_eetu_atual = n_quadras_eetu_2016 + n_quadras_eetu_decreto,
      area_eetu_2016 = sum(area_zoneamento[which(tipo_eixo == "EETU")], na.rm = TRUE) / 1000000,
      area_eetu_decreto = sum(area_zoneamento[which(tipo_eixo == "Decreto")], na.rm = TRUE)  / 1000000,
      area_eetu_atual = area_eetu_2016 + area_eetu_decreto,
      area_eetu_novos = sum(area_zoneamento[which(tipo_eixo == "Nova regra")], na.rm = TRUE) / 1000000,
      var_perc_atual_novos = if_else(area_eetu_novos != 0 & area_eetu_atual != 0,
                              (area_eetu_atual+area_eetu_novos)/area_eetu_atual - 1,
                              NA_real_),
      area_eetu_novo_arco_tiete = sum(area_zoneamento[which(mem_setor == "ARCO TIETE")] / 1000000, 
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
      area_eetu_novos_perc_distrito = area_eetu_novos/area_distrito)
  
  #
  zonas_eetu_por_distrito <- distritos %>%
    transmute(distrito = ds_nome) %>%
    left_join(zonas_eetu_por_distrito_no_geo, by = "distrito")
  
  return(zonas_eetu_por_distrito)
}


#
get_zonas_eetu_por_isocrona <- function(data) {
  
  # Por isocrona 
  zonas_eetu_por_isocrona_no_geo <- data %>%
    st_drop_geometry() %>%
    group_by(#macroarea, mem_setor,
      # eixo,
      isocrona, tipo_transp) %>%
    summarize(
      # situacao_transp = first(situacao_transp, na_rm = TRUE),
      n_quadras_eetu_2016 = length(which(tipo_eixo == "EETU")),
      n_quadras_eetu_decreto = length(which(tipo_eixo == "Decreto")),
      n_quadras_eetu_atual = n_quadras_eetu_2016 + n_quadras_eetu_decreto,
      n_quadras_eetu_novos = length(which(tipo_eixo == "Nova regra")),
      n_quadras_eetu_novos_arco_tiete = length(which(mem_setor == "ARCO TIETE")),
      area_eetu_2016 = sum(area_zoneamento[which(tipo_eixo == "EETU")], na.rm = TRUE) / 1000000,
      area_eetu_decreto = sum(area_zoneamento[which(tipo_eixo == "Decreto")], na.rm = TRUE)  / 1000000,
      area_eetu_atual = area_eetu_2016 + area_eetu_decreto,
      area_eetu_novos = sum(area_zoneamento[which(tipo_eixo == "Nova regra")], na.rm = TRUE) / 1000000,
      var_perc_atual_novos = if_else(area_eetu_novos != 0 & area_eetu_atual != 0,
                              (area_eetu_atual+area_eetu_novos)/area_eetu_atual - 1,
                              NA_real_),
      area_eetu_novos_arco_tiete = sum(area_zoneamento[which(mem_setor == "ARCO TIETE")] / 1000000, 
                                      na.rm = TRUE),
      # var_perc_atual_novos_arco_tiete = if_else(area_eetu_novo_arco_tiete != 0 & area_eetu != 0,
      #                                    (area_eetu_novo_arco_tiete + area_eetu)/area_eetu,
      #                                    NA_real_)
    ) %>%
    ungroup()
  
  return(zonas_eetu_por_isocrona_no_geo)
}

# 5. Tipifica eixo --------------------------------------------------------

# Cenário intermediário
# Dentro de 700m/400m
zonas_eetu_contidas_450m_700m <- get_zonas_eetu(
  dist_alta_capacidade = c(700, 700),
  dist_media_capacidade = c(450, 450)
)

# Cenário final
# Tocadas por 700m/400m
zonas_eetu_tocadas_400m_700m <- get_zonas_eetu(
  dist_alta_capacidade = c(700, 3000),
  dist_media_capacidade = c(400, 3000)
)

# #
# zonas_eetu_600m_a_800m <- get_zonas_eetu(
#   dist_alta_capacidade = c(600, 800),
#   dist_media_capacidade = c(300, 450)
# )
# 
# #
# zonas_eetu_800m_a_1000m <- get_zonas_eetu(
#   dist_alta_capacidade = c(800, 1000),
#   dist_media_capacidade = c(450, 600)
# )


# 6. Resume por estação ---------------------------------------------

#
zonas_eetu_tocadas_400m_700m_por_estacao <- get_zonas_eetu_por_estacao(
  data = zonas_eetu_tocadas_400m_700m
)

#
zonas_eetu_contidas_450m_700m_por_estacao <- get_zonas_eetu_por_estacao(
  data = zonas_eetu_contidas_450m_700m
)

# #
# zonas_eetu_600m_a_800m_por_estacao <- get_zonas_eetu_por_estacao(
#   data = zonas_eetu_600m_a_800m
# )
# 
# #
# zonas_eetu_800m_a_1000m_por_estacao <- get_zonas_eetu_por_estacao(
#   data = zonas_eetu_800m_a_1000m
# )


# 7. Resume por distrito ---------------------------------------------

#
zonas_eetu_tocadas_400m_700m_por_distrito <- get_zonas_eetu_por_distrito(
  data = zonas_eetu_tocadas_400m_700m
)

#
zonas_eetu_contidas_450m_700m_por_distrito <- get_zonas_eetu_por_distrito(
  data = zonas_eetu_contidas_450m_700m
)

# #
# zonas_eetu_600m_a_800m_por_distrito <- get_zonas_eetu_por_distrito(
#   data = zonas_eetu_600m_a_800m
# )
# 
# #
# zonas_eetu_800m_a_1000m_por_distrito <- get_zonas_eetu_por_distrito(
#   data = zonas_eetu_800m_a_1000m
# )


# 8. Resume por isócrona ---------------------------------------------

#
zonas_eetu_tocadas_400m_700m_por_isocrona_no_geo <- get_zonas_eetu_por_isocrona(
  data = zonas_eetu_tocadas_400m_700m
)

#
zonas_eetu_contidas_450m_700m_por_isocrona_no_geo <- get_zonas_eetu_por_isocrona(
  data = zonas_eetu_contidas_450m_700m
)

# #
# zonas_eetu_600m_a_800m_por_isocrona_no_geo <- get_zonas_eetu_por_isocrona(
#   data = zonas_eetu_600m_a_800m
# )
# 
# #
# zonas_eetu_800m_a_1000m_por_isocrona_no_geo <- get_zonas_eetu_por_isocrona(
#   data = zonas_eetu_800m_a_1000m
# )


# 9. Exporta para datalake -----------------------------------------------------

#
list_outputs <- mget(ls(.GlobalEnv, pattern = "^zonas_eetu_"))

# Cria diretórios
# names(list_outputs) %>%
#   map(~fs::dir_create(here("inputs", "2_trusted", "Eixos", .x)))

# Geospacial
list_outputs %>%
  keep(~is(.x, "sf")) %>%
  map2(names(.), 
       ~sfarrow::st_write_parquet(.x,
                                  here("inputs", "2_trusted", "Eixos",
                                       paste0(.y, ".parquet")))
  )

# Tabular
list_outputs %>%
  discard(~is(.x, "sf")) %>%
  map2(names(.), 
       ~arrow::write_parquet(.x,
                                  here("inputs", "2_trusted", "Eixos",
                                       paste0(.y, ".parquet")))
  )

# 10. Exporta para usuário -----------------------------------------------------


# # Cria diretórios
# names(list_outputs) %>%
#   map(~fs::dir_create(here("outputs", "Eixos", .x)))
# 
# # Exporta shapefiles
# list_outputs %>%
#   keep(~is(.x, "sf")) %>%
#   map2(names(.),
#        ~st_write(.x,
#                  here("outputs", "Eixos", .y,
#                       paste0(.y, ".shp")),
#                  layer = .y,
#                  delete_layer = TRUE, driver="ESRI Shapefile")
#   )
# 
# # Exporta labels .csv para shapefiles
# list_outputs %>%
#   keep(~is(.x, "sf")) %>%
#   map2(names(.),
#        ~write_csv(data.frame(labels = names(.x)),
#                   here("outputs", "Eixos", .y, paste0(.y, "_labels.csv")))
#   )
# 
# # Exporta planilhas
# list_outputs %>%
#   map2(names(.),
#        ~writexl::write_xlsx(.x,
#                             here("outputs", "Eixos", .y,
#                                  paste0(str_remove_all(.y, "_no_geo"), ".xlsx")))
#   )

