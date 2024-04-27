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
       "zoneamento.parquet")) %>%
  filter(macroarea == "MEM")

#
zonas_eetu <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Eixos",
       "zonas_eetu.parquet")
)


# Lotes
# lotes <- sfarrow::st_read_parquet(
#   here("inputs", "2_trusted", "Legislacao",
#        "lotes.parquet"))

#
eixos <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte",
       "eixos.parquet")
)

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

# Tomando o id da infra de transporte mais próxima
# nearest_metro_quadras <- 
#   tibble(
#     id = rownames(zoneamento_ajust) %>% as.numeric(),
#     id_metro = st_nearest_feature(zoneamento_ajust, eixos_metro),
#     dist_metro = st_distance(
#       zoneamento_ajust, 
#       eixos_metro[st_nearest_feature(zoneamento_ajust, eixos_metro),], 
#       by_element = TRUE) %>% as.vector()
#   ) 
# 
# #
# nearest_trem_quadras <- 
#   tibble(
#     id = rownames(zoneamento_ajust) %>% as.numeric(),
#     id_trem = st_nearest_feature(zoneamento_ajust, eixos_trem),
#     dist_trem = st_distance(
#       zoneamento_ajust, 
#       estacao_trem[st_nearest_feature(zoneamento_ajust, eixos_trem),], 
#       by_element = TRUE) %>% as.vector()
#   ) 
# 
# #
# nearest_corredor_quadras <- 
#   tibble(
#     id = rownames(zoneamento_ajust) %>% as.numeric(),
#     id_corredor = st_nearest_feature(zoneamento_ajust, eixos_corredor),
#     dist_corredor = st_distance(
#       zoneamento_ajust, 
#       eixos_corredor[st_nearest_feature(zoneamento_ajust, eixos_corredor),], 
#       by_element = TRUE) %>% as.vector()
#   ) 

#
nearest_metro_quadras <- st_nearest_feature(zoneamento_ajust, eixos_metro) %>%
  as.data.frame() %>%
  set_names("id_metro") %>%
  mutate(id = rownames(.) %>% as.integer())

#
nearest_trem_quadras <- st_nearest_feature(zoneamento_ajust, eixos_trem) %>%
  as.data.frame() %>%
  set_names("id_trem") %>%
  mutate(id = rownames(.) %>% as.integer())

#
nearest_corredor_quadras <- st_nearest_feature(zoneamento_ajust, eixos_corredor) %>%
  as.data.frame() %>%
  set_names("id_corredor") %>%
  mutate(id = rownames(.) %>% as.integer())

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
              tipo_transp = case_when(
                metro == 1 ~ "METRÔ",
                trem == 1 ~ "TREM",
                corredor == 1 ~ "ÔNIBUS",
                TRUE ~ NA_character_)) %>%
    left_join(st_drop_geometry(eixos_metro) %>%
                transmute(id_metro = row_number(),
                          eixo_metro = eixo,
                          tipo_infra_metro = tipo_infra,
                          n_pontos_acesso, tipo_transp, tipo_transp_secundario,
                          situacao_metro = situacao_transp),
              by = c("id_metro", "tipo_transp")) %>%
    left_join(st_drop_geometry(eixos_trem) %>%
                transmute(id_trem = row_number(),
                          eixo_trem = eixo,
                          tipo_infra_trem = tipo_infra,
                          n_pontos_acesso, tipo_transp, tipo_transp_secundario,
                          situacao_trem = situacao_transp),
              by = c("id_trem", "tipo_transp")) %>%
    left_join(st_drop_geometry(eixos_corredor) %>%
                transmute(id_corredor = row_number(),
                          eixo_corredor = eixo,
                          tipo_infra_corredor = tipo_infra,
                          n_pontos_acesso, tipo_transp, tipo_transp_secundario,
                          situacao_corredor = situacao_transp),
              by = c("id_corredor", "tipo_transp")) %>%
    transmute(
      id,
      eixo = coalesce(eixo_metro, eixo_trem, eixo_corredor),
      tipo_infra = coalesce(tipo_infra_metro, tipo_infra_trem, tipo_infra_corredor),
      n_pontos_acesso,
      tipo_transp,
      tipo_transp_secundario,
      situacao_transp = coalesce(situacao_metro, situacao_trem, situacao_corredor)) %>%
    mutate(tipo_eixo = if_else(!is.na(tipo_transp), "Potencial", NA_character_))
  
  
  
  # 
  zonas_eetu_novos_no_geo <- zoneamento_ajust %>%
    st_drop_geometry() %>%
    
    ### Restrições ao EETU
    # Áreas de Operação Urbana não podem ser EETU
    filter(is.na(operacao_urbana) | 
             operacao_urbana == "AGUA BRANCA (perímetro expandido)") %>%
    # Arco Tietê incluído na nova regra
    # filter(!mem_setor %in% c("ARCO TAMANDUATEI", "ARCO JURUBATUBA", 
    #                          "ARCO PINHEIROS") |
    #          is.na(mem_setor)) %>%
    # Somente Perímetro expandido do PIU Setor Central é incluído na nova regra
    # filter(is.na(aiu_perimetro) | aiu_perimetro == "Expandido") %>%
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


# 3. Tipifica --------------------------------------------------------------

#
zonas_eetu_potencial_mem <- get_zonas_eetu(
  dist_alta_capacidade = c(400, 600),
  dist_media_capacidade = c(150, 300)) %>%
  filter(zoneamento_grupo != "EETU")

# 4. Exporta para datalake ------------------------------------------------
#
sfarrow::st_write_parquet(zonas_eetu_potencial_mem,
                          here("inputs", "2_trusted", "Eixos",
                               "zonas_eetu_potencial_mem.parquet"))
