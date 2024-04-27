

# 1. Inbound --------------------------------------------------------------

#
get_shapefiles_names <- function(){
  # Lista nomes de cada diretório contido na pasta "inputs"
  shp_names <- list.dirs(here("inputs", "1_inbound")) %>%
    str_subset("OriginalFiles|DadosOriginais", negate = TRUE) %>%
    map_chr(~ word(.x, -1, sep = "/"))
  
  # Lista todos os arquivos .shp e nomeia de acordo com nome do respectivo diretório
  shp_files <- list.dirs(here("inputs", "1_inbound")) %>%
    str_subset("OriginalFiles|DadosOriginais", negate = TRUE) %>%
    set_names(shp_names) %>%
    map(~ list.files(.x, pattern = ".shp$", full.names = TRUE)) %>%
    compact()
  
  #
  sort(names(shp_files))
}

#
get_shapefiles <- function(files, ...){
  # Lista nomes de cada diretório contido na pasta "inputs"
  shp_names <- list.dirs(here("inputs", "1_inbound")) %>%
    str_subset("OriginalFiles|DadosOriginais", negate = TRUE) %>%
    map_chr(~ word(.x, -1, sep = "/"))
  
  # Lista todos os arquivos .shp e nomeia de acordo com nome do respectivo diretório
  shp_files <- list.dirs(here("inputs", "1_inbound")) %>%
    str_subset("OriginalFiles|DadosOriginais", negate = TRUE) %>%
    set_names(shp_names) %>%
    map(~ list.files(.x, pattern = ".shp$", full.names = TRUE)) %>%
    compact()
  
  # Padrão regex para os objetos que queremos encontrar na lista de arquivos
  files <- files
  
  # Sistema geográfico de referência
  CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
  
  # Lê shapefiles, aplica CRS e reprojeta para SIRGAS 2000
  shp_list <- shp_files %>%
    keep(str_detect(names(.), paste(files, collapse = "|"))) %>%
    map(st_read) %>%
    map_if(~ is.na(st_crs(.x)), ~ st_set_crs(.x, value = CRS),
           .else = ~st_transform(.x, crs = CRS)) %>%
    # map(~ st_transform(.x, crs = CRS)) %>%
    set_names(~janitor::make_clean_names(.x, case = "snake"))
  
  # Importa shapefiles tratadas para o Global Environment
  list2env(shp_list, envir = .GlobalEnv)
  
}


# 2. Trusted --------------------------------------------------------------

#
get_trusted_names <- function(){
  # Lista nomes de cada diretório contido na pasta "inputs"
  trusted_dirs <- list.dirs(here("inputs", "2_trusted")) %>%
    str_subset("Pre$", negate = TRUE) %>%
    map_chr(~ word(.x, -1, sep = "/"))
  
  # Lista todos os arquivos .shp e nomeia de acordo com nome do respectivo diretório
  trusted_files <- list.dirs(here("inputs", "2_trusted")) %>%
    str_subset("Pre$", negate = TRUE) %>%
    set_names(trusted_dirs) %>%
    map(~ list.files(.x, pattern = ".parquet$", full.names = TRUE) %>%
          map_chr(~ word(.x, -1, sep = "/"))) %>%
    compact()
  
  #
  trusted_files
}

#
get_trusted_files <- function(files){
  # Lista nomes de cada diretório contido na pasta "inputs"
  trusted_dirs <- list.dirs(here("inputs", "2_trusted")) %>%
    str_subset("Pre$", negate = TRUE) %>%
    map_chr(~ word(.x, -1, sep = "/"))
  
  # Lista todos os arquivos .shp e nomeia de acordo com nome do respectivo diretório
  trusted_files <- list.dirs(here("inputs", "2_trusted")) %>%
    str_subset("Pre$", negate = TRUE) %>%
    set_names(trusted_dirs) %>%
    map(~ list.files(.x, pattern = ".parquet$", full.names = TRUE)) %>%
    compact()
  
  # Padrão regex para os objetos que queremos encontrar na lista de arquivos
  files <- files

  # Sistema geográfico de referência
  CRS <- "+proj=utm +zone=23 +south +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
  
  # Lê parquets
  trusted_list <- trusted_files %>%
    flatten() %>%
    keep(str_detect(map_chr(., ~ word(.x, -1, sep = "/")), 
                    pattern = paste0(files, collapse = "|"))) %>%
    set_names(~.x %>% 
                map_chr(~ word(.x, -1, sep = "/")) %>%
                str_remove(pattern = ".parquet")) %>%
    map_at(~str_detect(., pattern = "no_geo"), ~arrow::read_parquet(.x)) %>%
    map_at(~str_detect(., pattern = "no_geo", negate = TRUE),
           ~sfarrow::st_read_parquet(.x))
  
  # Importa shapefiles tratadas para o Global Environment
  list2env(trusted_list, envir = .GlobalEnv)
  
}
