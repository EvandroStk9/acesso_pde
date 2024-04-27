library(here)
library(tidyverse)
library(sf)
library(elevatr)
library(raster)
library(osmextract)

# Reprodução de metodologia IPEA

# Requisito: Java 11 ou superior
cat(processx::run("java", args = "-version")$stderr)

#
CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"

# 1. Topografia  ---------------------------------------------------------------

#
sp_bordas <- st_read(here("Dados", "BordasCidade",
                          "BorderMSP_SIRGAS.shp")) %>%
  st_transform(CRS)

#
sp_topografia <- elevatr::get_elev_raster(sp_bordas, z = 9)

#
sp_topografia

#
plot(sp_topografia)

#
raster::writeRaster(sp_topografia, here("R5", "sp_topografia.tif"),
                    overwrite = TRUE)


# 2. Grade ---------------------------------------------------------------------

#
grid <- st_make_grid(sp_bordas, cellsize = c(1500,1500), crs = CRS, 
                     what = 'polygons') %>%
  st_sf('geometry' = .) %>%
  st_intersection(sp_bordas) %>%
  transmute(id = row_number(.),
            # lon = map_dbl(geometry, ~st_coordinates(.x)[[1]]),
            # lat = map_dbl(geometry, ~st_coordinates(.x)[[2]])
  )


ggplot() + geom_sf(data = grid)

#
write_csv(grid %>% 
            st_drop_geometry(),
          here("R5", "sp_grid.csv"))


# 3. Rede Viária ---------------------------------------------------------------

# Importação via pacote osmextract não foi bem sucedida
# Problemas com timeout
# Fez-se uso de rede viária de São Paulo previamente utilizada e baixada manualmente

#
# sp_rede_ruas <- osmextract::oe_get_network(sp_bordas, 
#                                            quiet = FALSE, force_download = TRUE, 
#                                            force_vectortranslate = TRUE)

# Não foi bem sucedida devido a problemas com timeout
# its_details <- osmextract::oe_match("Sao Paulo")

#
# osmextract::oe_download(
#   file_url = its_details$url, 
#   file_size = its_details$file_size,
#   provider = "geofabrik",
#   download_directory = here("R5"),
#   timeout = 1200
# )

#
# sudeste_rede_viaria <- osmextract::oe_read(here("R5", "sudeste-latest.osm.pbf"))

#
# glimpse(sudeste_rede_viaria)

#
# sp_rede_viaria <- sudeste_rede_viaria %>%
#   st_transform(crs = CRS) %>%
#   st_intersection(sp_bordas)

#
# ggplot() + geom_sf(data = sp_rede_viaria)

#
# st_write(sp_rede_viaria, here("R5", "sp_rede_viaria.osm.pbf"), driver = "OSM")
