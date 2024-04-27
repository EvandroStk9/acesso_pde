# Pacotes
library(here)
library(fs)
library(beepr)
library(sf)
library(tidyverse)
library(sfarrow)

# Opções
options(scipen = 99999,
        error = beep)

# 1. Importa -------------------------------------------------------------------

eixos <- sfarrow::st_read_parquet(
  here("inputs", "2_trusted", "Transporte", 
       "eixos.parquet")
)

# 2. Cria buffers --------------------------------------------------------------

# Buffers de 50m em 50m até 1km
eixos_por_buffer <- crossing(eixos, buffer = seq(50, 1000, 50)) %>%
  st_as_sf() %>%
  mutate(
    geometry = st_buffer(geometry, dist = buffer)
  ) %>%
  relocate(geometry, .after = everything())

# 2. Seleciona -----------------------------------------------------------------
# Seleciona buffers de interesse


# Metrô e Trem --> 400m
# Corredor --> 150m
eixos_por_buffer_150m_400m <- eixos_por_buffer %>%
  filter((tipo_infra == "ESTAÇÃO" & buffer == 400) | 
           (tipo_infra == "CORREDOR" & buffer == 150))

# Metrô e Trem --> 600m
# Corredor --> 300m
eixos_por_buffer_300m_600m <- eixos_por_buffer %>%
  filter((tipo_infra == "ESTAÇÃO" & buffer == 600) | 
           (tipo_infra == "CORREDOR" & buffer == 300))

# Metrô e Trem --> 700m
# Corredor --> 400m
eixos_por_buffer_400m_700m <- eixos_por_buffer %>%
  filter((tipo_infra == "ESTAÇÃO" & buffer == 700) | 
           (tipo_infra == "CORREDOR" & buffer == 400))

# Metrô e Trem --> 700m
# Corredor --> 450m
eixos_por_buffer_450m_700m <- eixos_por_buffer %>%
  filter((tipo_infra == "ESTAÇÃO" & buffer == 700) | 
           (tipo_infra == "CORREDOR" & buffer == 450))

# Metrô e Trem --> 1000m
# Corredor --> 450m
eixos_por_buffer_450m_1000m <- eixos_por_buffer %>%
  filter((tipo_infra == "ESTAÇÃO" & buffer == 1000) | 
           (tipo_infra == "CORREDOR" & buffer == 450))

# Dissolve buffers de interesse

# 3. Dissolve ------------------------------------------------------------------

# 
eixos_por_buffer_150m_400m_dissolved <- eixos_por_buffer_150m_400m %>%
  st_union() %>%
  st_cast("POLYGON") %>%
  st_as_sf()

#
eixos_por_buffer_300m_600m_dissolved <- eixos_por_buffer_300m_600m %>%
  st_union() %>%
  st_cast("POLYGON") %>%
  st_as_sf()

#
eixos_por_buffer_400m_700m_dissolved <- eixos_por_buffer_400m_700m %>%
  st_union() %>%
  st_cast("POLYGON") %>%
  st_as_sf()


#
eixos_por_buffer_450m_700m_dissolved <- eixos_por_buffer_450m_700m %>%
  st_union() %>%
  st_cast("POLYGON") %>%
  st_as_sf()


#
eixos_por_buffer_450m_1000m_dissolved <- eixos_por_buffer_450m_1000m %>%
  st_union() %>%
  st_cast("POLYGON") %>%
  st_as_sf()


# 4. Exporta para datalake -----------------------------------------------------

#
# sfarrow::st_write_parquet(
#   eixos_por_buffer,
#   here("inputs", "2_trusted", "Eixos",
#        "eixos_por_buffer.parquet")
# )


#
list_outputs <- mget(ls(.GlobalEnv, pattern = "eixos_por_buffer"))

# Geospacial
list_outputs %>%
  # keep(~is(.x, "sf")) %>%
  map2(names(.), 
       ~sfarrow::st_write_parquet(.x,
                                  here("inputs", "2_trusted", "Transporte",
                                       paste0(.y, ".parquet")))
  )

