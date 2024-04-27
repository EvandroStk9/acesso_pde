# SETUP ---------------------------------------------------------------------

# Abre o RProject e toma a pasta do RProject como diretório root

# Diretrizes:
# PascalCase para pastas
# 1_snake_case para scripts
# snake_case para funções

# No arquivo .Rprofile:
# 1. Delimita limite de 10GB memória RAM (no Linux)
# 2. Opção para não aparecer notação científica
# 3. Opção para barulho no momento de erro
usethis::edit_r_profile()

# No arquivo .Renviron
# 1. Chave de API para Github
# 2. Chave de API para Here Maps
usethis::edit_r_environ()

# Vê pacotes/livrarias instalados
## Obs: ainda por fazer! Ideia é mostrar pacotes/livrarias necessários ao projeto
tibble::tibble(
  # Package = names(installed.packages()[,3]),
  # Version = unname(installed.packages()[,3])
  Package = names(installed.packages()[,3]),
  Version = unname(installed.packages()[,3])
)

# Informações da sessão R
writeLines(capture.output(sessionInfo()), "sessionInfo.txt")

# Estabelece número de CPU's como total de CPU's menos 1
## Obs: pode deixar o computador mais lento!
future::plan("multisession", workers = future::availableCores() - 1)

#
scripts <- list.files(here::here("scripts"), pattern ="\\.R$",
                      recursive = TRUE, full.names = TRUE)

# Lista inputs do projeto
fs::dir_tree(here::here("inputs"), recurse = FALSE)

# A princípio, foram separados "Complementares" vs outras pastas com dados intermediários
# Transição para todos os dados "de entrada" na "inbound zone"
# Na inbound, catalogação se dá na hierarquia fonte_dos_/dados/base_de_dados/...
# No terceiro nível em diante, o datalake seguiria com padrão flexível e menos estruturado
fs::dir_tree(here::here("inputs", "1_inbound"), recurse = FALSE)



# ETL ------------------------------------------------------------------------

# Lista scripts do projeto
# Aqui as pastas são numeradas afim de esboçar um pipeline
fs::dir_tree(here::here("scripts"), recurse = FALSE)

# Arquivos intermediários na camada "trusted"
# Aqui todos os arquivos tem formato otimizado e seguem estrutura /base_de_dados/xxxxx.parquet
# As análises e modelos via de regra não consomem dados da camada "inbound" e sim da "trusted"
fs::dir_tree(here::here("inputs", "1_inbound"), recurse = FALSE)
fs::dir_tree(here::here("inputs", "2_trusted"), recurse = FALSE)

# Lista outputs dos scripts do projeto
fs::dir_tree(here::here("outputs"))


# 1. Acessibilidade  -----------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "01_Acessibilidade"))

# Acessibilidade a empregos
# Unidade de análise: grid 500m x 500m 
# Motivo: categoriza faixas de acessibilidade 
source(here::here("scripts", "01_Acessibilidade", "acessibilidade_tratamento.R"))
rm(list = ls())


# 2. Legislação -----------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "02_Legislacao"))

# Zoneamento
# Unidade de análise: perímetros de zona (zona-quadra)
# Regras: por grupos de zona
# Motivo: agregação de atributos a unidade básica de análise + categorização de grupos de zona
source(here::here("scripts", "02_Legislacao", "zoneamento_tratamento.R"))
rm(list = ls())

# Lotes
# Unidade de análise: lote
# CÓDIGO LENTO
# GRANDE VOUME DE RAM
source(here::here("scripts", "02_Legislacao", "lotes_tratamento.R"))
rm(list = ls())
.rs.restartR()

# MEM
# Unidade de análise: macroárea
# Motivo: selecionar zonas-quadra, setores, PIU's e perímetro geral da MEM
# Regras: por zona-quadra, por setores da MEM, por PIU
source(here::here("scripts", "02_Legislacao", "mem_tratamento.R"))
rm(list = ls())

# Quadras
# Unidade de análise: quadras
# Motivo: selecionar zonas-quadra, setores, PIU's e perímetro geral da MEM
# NÃO ESSENCIAL: dado de quadra acabou se mostrando dispensável pra aplicações
# CÓDIGO LENTO
source(here::here("scripts", "02_Legislacao", "quadras_tratamento.R"))
rm(list = ls())

# Legislação - outras informaçoes de contexto
# Macroáreas, PIU's, AIU Setor Central, Operações Urbanas
# Unidade de análise: quadras
# Motivo: selecionar zonas-quadra, setores, PIU's e perímetro geral da MEM
# CÓDIGO LENTO
source(here::here("scripts", "02_Legislacao", "contexto_legislacao.R"))
rm(list = ls())


# 3. Transporte -----------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "03_Transporte"))


# Linhas de transporte
# Metrô, trem e corredores de ônibus
source(here::here("scripts", "03_Transporte", "linhas_transporte.R"))

# Eixos de transporte
# No projeto como um todo considerar que Eixo de trasporte != Zona EETU/Zona de Eixo
# Faz tratamento do conjunto de dados 'eixos' e exporta em formatos parquet
# Trata dados de transporte - estações e corredores
# REQUER SCRIPT linhas_transporte.R
source(here::here("scripts", "03_Transporte", "eixos_tratamento.R"))
rm(list = ls())

# Buffers de eixos de transporte
# Gera buffers de 50m em 50m para todos os eixos em formato .parquet
# Gera buffers .shp para (estacao, corredor) = {(400m, 150m), (300m, 600m), (400m,700m), (1000m, 450m)} 
# Gera buffers dissolvidos .shp
source(here::here("scripts", "03_Transporte", "eixos_buffers.R"))
rm(list = ls())

# Isocronas de eixos de transporte
# Isocronas de 15, 20, 25 e 30 minutos
# Corredores divididos em pontos de aproximadamente 100m de distância entre si 
# CÓDIGO MUITO LENTO
source(here::here("scripts", "03_Eixos", "eixos_isocronas_metro.R"))
source(here::here("scripts", "03_Eixos", "eixos_isocronas_trem.R"))
source(here::here("scripts", "03_Eixos", "eixos_isocronas_onibus.R"))
source(here::here("scripts", "03_Eixos", "eixos_isocronas.R"))
rm(list = ls())


# 4. Zonas EETU ----------------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "04_Eixos"))

# A)

# EETU'S
# Faz tratamento do conjunto de dados 'zonas_eetu' e exporta em formato parquet
# Identificação da estação de transporte que "ativa" o eixo
#    Zoneamento \                /  zonas_eetu_por_estacao  
#                |-> zonas_eetu |-> zonas_eetu_por_distrito
#      Eixos    /                \  zonas_eetu_por_isocrona
source(here::here("scripts", "04_Eixos", "zonas_eetu_tratamento.R"))
rm(list = ls())

# POTENCIAIS - MEM
# Faz tratamento do conjunto de dados 'mem_eixos_por_quadra' + 'eixos_por_quadra'
# Exporta em formato .parquet, .shp e .xlsx
#              \
#    Zoneamento \ 
#                \
#   Zonas EETU    |-> zonas_eetu_potencial_mem
#                /    
#    Eixos      /     
#              /
source(here::here("scripts", "03_Eixos", "zonas_eetu_potencial_mem_tratamento.R"))
rm(list = ls())

# EETUS - lotes
# Gera arquivo geo com demarcação de lotes em Zonas EETU
source(here::here("scripts", "04_Eixos", "zonas_eetu_lotes.R"))
rm(list = ls())
.rs.restartR()


# B)

# EETUS nova regra - lotes
# Primeira proposta votada na câmara expandindo demarcação de eetu por lotes
# Usado na Nota Técnica sobre as mudanças nas regras de Eixo - Revisão PDE 2023
# CÓDIGO LENTO
# USA PARALELISMO
source(here::here("scripts", "04_Eixos", "zonas_eetu_ampliacao_lotes.R"))
rm(list = ls())
.rs.restartR()

# EETUS nova regra - quadras
# Proposta final votada na câmara
# CÓDIGO LENTO
source(here::here("scripts", "04_Eixos", "zonas_eetu_ampliacao_quadras.R"))
rm(list = ls())


# 5. Imobiliário ----------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "05_Imobiliario")) 


# i) PROJETO ALVARÁS PMSP/SMUL
# Pega dados brutos disponibilizados pela PMSP/SMUL e faz pré-processamento
# Arquivo .Rproj separado
# Meta longo prazo: transformar em pacote R a ser disponibilizado no CRAN

#
source(here::here("scripts", "05_Imobiliario", "alvaras_inputa_legislacao.R"))
rm(list = ls())

# ii) Faz tratamento do conjunto de dados 'alvaras' 
# Exporta em formatos .parquet
source(here::here("scripts", "05_Imobiliario", "alvaras_tratamento.R"))
rm(list = ls())

# Licenciamentos em zonas de eetu com informaações adicionais de eixo de transporte
source(here::here("scripts", "05_Imobiliario", "alvaras_zonas_eetu_tratamento.R"))
rm(list = ls())

# Faz anaĺise espacial e exporta tabela com estatísticas G, Moran e entropia
# Outputs são utilizados na análise da nota técnica 1
## Obs: com bug na função get_moran (estimativa moran global) 
source(here::here("scripts", "05_Imobiliario", "alvaras_sfstats.R"))

# C) LANÇAMENTOS IMOBILIÁRIOS

# DESATUALIZADO
# Faz tratamento do conjunto de dados 'brain' e exporta .parquet e .shp
## OBS: VALORES NEGATIVOS DE PREÇO E PREÇO M2!
source(here::here("scripts", "05_Imobiliario", "brain_tratamento.R"))
rm(list = ls())
.rs.restartR()

# Faz tratamento do conjunto de dados 'embraesp' e exporta em formato .shp
source(here::here("scripts", "05_Imobiliario", "embraesp_tratamento.R"))
rm(list = ls())

# 6. Elementos de contexto -----------------------------------------------

#
fs::dir_tree(here::here("scripts", "06_ElementosContexto"))

#
source(here::here("scripts", "06_ElementosContexto", "mapa_base.R"))
rm(list = ls())

# 7. Casos ----------------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "07_Casos")) 


#
source(here::here("scripts", "07_Casos", "casos_selecao.R"))
source(here::here("scripts", "07_Casos", "casos_isocronas.R")) 
source(here::here("scripts", "07_Casos", "casos_pois.R"))


# 8. Fatores ------------------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "08_Fatores"))

# A) Modelo Linear Múltiplo
# Tratamento p/ modelo de fatores urbanos determinantes da dinâmica imobiliária
source(here::here("scripts", "08_Fatores", "fatores_area_ponderacao_tratamento"))
rm(list = ls())

# Aplica modelagem a alvaras + covariaveis | Linear Múltipla
source(here::here("scripts", "08_Fatores", "fatores_lm.R"))
rm(list = ls())

# ANALYTICS ----------------------------------------------------------------


# 9. Exploratória --------------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "09_Exploratoria"))

# A) Licenciamentos
# Análise exploratória dos alvarás de licenciamento imobiliário
rmarkdown::render(
  here::here("scripts", "09_Exploratoria", "alvaras_analise.Rmd"),
  output_format = "html_document",
  output_dir = here::here("outputs", "Exploratoria"),
  output_file = 'alvaras_analise', 
  output_options = list(toc = TRUE,
                        toc_depth = 3,
                        toc_float = TRUE,
                        theme = "united"))
rm(list = ls())


# B) Lançamentos
# Nota técnica Preços (opção 1)
# Dados EMBRAESP!
# Faz análise dos dados em formato markdown/html
rmarkdown::render(
  here::here("scripts", "09_Exploratoria", "embraesp_analise_precos.Rmd"),
  output_format = "html_document",
  output_dir = here::here("outputs", "Exploratoria"),
  output_file = 'embraesp_analise_precos',
  output_options = list(toc = TRUE,
                        toc_depth = 3,
                        toc_float = TRUE,
                        theme = "united"))
rm(list = ls())


# C) Estudos de Caso

# Estudos de caso - selecão
# NECESSÁRIO ATUALIZAR COM DADOS NOVOS!
rmarkdown::render(
  here::here("scripts", "09_Exploratoria", "alvaras_casos_selecao.Rmd"),
  output_format = "html_document",
  output_dir = here::here("outputs", "Exploratoria"),
  output_file = 'alvaras_casos_selecao', 
  output_options = list(toc = TRUE,
                        toc_depth = 3,
                        toc_float = TRUE,
                        theme = "united")
)
rm(list = ls())


# Estudos de caso - análise
rmarkdown::render(
  here::here("scripts","09_Exploratoria", "alvaras_casos_analise.Rmd"),
  output_format = "html_document",
  output_dir = here::here("outputs", "Exploratoria"),
  output_file = 'alvaras_casos_analise', 
  output_options = list(toc = TRUE,
                        toc_depth = 3,
                        toc_float = TRUE,
                        theme = "united")
)
rm(list = ls())

# C) ANÁLISE DO MODELO DE FATORES DETERMINANTES
# Nota técnica 4
# Faz análise dos dados em formato word
# EM CONSTRUÇÃO!
# Nome correto seria "alvaras_analise_fatores.Rmd"?
rmarkdown::render(
  here::here("scripts", "09_Exploratoria", "fatores_analise.Rmd"),
  # here::here("scripts", "Alvaras", "alvaras_analise_fatores.Rmd"),
  output_format = "word_document",
  output_dir = here::here("outputs", "Exploratoria", "Fatores"),
  # output_dir = here::here("outputs", "Alvaras"),
  output_file = 'fatores_analise',
  # output_file = 'alvaras_analise_fatores', 
  output_options = list(toc = TRUE,
                        number_sections = TRUE,
                        reference_docx = here::here(
                          "inputs", "1_inbound", "Insper", "Institucional",
                          "papel-timbrado-insper.docx")
  ))
rm(list = ls())


#.rs.restartR()


# 10. Notas Técnicas -------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "10_NotasTecnicas"))

# Nota técnica 1 - Produção Habitacional Formal
rmarkdown::render(
  here::here("scripts", "10_NotasTecnicas", "nt1_analise_alvaras.Rmd"),
  output_format = "word_document",
  output_dir = here::here("outputs", "NT1_ProducaoHabitacionalFormal"),
  output_file = 'nt1_analise_alvaras', 
  output_options = list(toc = TRUE,
                        number_sections = TRUE,
                        reference_docx = here::here(
                          "inputs", "1_inbound", "Insper", "Institucional",
                          "papel-timbrado-insper.docx")
  ))
rm(list = ls())


# Nota técnica 2 - Eixo não é tudo igual
rmarkdown::render(
  here::here("scripts", "10_NotasTecnicas", "nt2_analise_eixos.Rmd"),
  output_format = "word_document",
  output_dir = here::here("outputs", "NT2_EixoNaoETudoIgual"),
  output_file = 'nt2_analise_eixos', 
  output_options = list(toc = TRUE,
                        number_sections = TRUE,
                        reference_docx = here::here(
                          "inputs", "1_inbound", "Insper", "Institucional",
                          "papel-timbrado-insper.docx")
  ))
rm(list = ls())

# Nota técnica 3
# Faz análise dos dados em formato word
rmarkdown::render(
  here::here("scripts", "10_NotasTecnicas", "nt3_analise_mem.Rmd"),
  output_format = "word_document",
  output_dir = here::here("outputs", "NT3_EnquantoOsPIUsNaoSaemDoPapel"),
  output_file = 'nt3_analise_mem', 
  output_options = list(toc = TRUE,
                        number_sections = TRUE,
                        reference_docx = here::here(
                          "inputs", "1_inbound", "Insper", "Institucional",
                          "papel-timbrado-insper.docx")
  ))
rm(list = ls())

# Nota técnica 5 (Nota Metodológica)
rmarkdown::render(
  here::here("scripts", "10_NotasTecnicas", "nt5_metodologia.Rmd"),
  output_format = "word_document",
  output_dir = here::here("outputs", "NT5_DadosAlvarasLicenciamento"),
  output_file = 'nt5_metodologia', 
  output_options = list(toc = TRUE,
                        number_sections = TRUE,
                        reference_docx = here::here(
                          "inputs", "1_inbound", "Insper", "Institucional",
                          "papel-timbrado-insper.docx")
  ))
rm(list = ls())

# Nota Técnica 6 - Gráficos da Nota técnica Vagas 
# A análise dos dados foi feita em RMarkdown e os gráficos colocados em documento docx
# A escrita do texto da nota técnica foi feita no Google Docs
# A construção de figuras ilustrativas incluídas na nota técnica foi feita no Google Slides
# Faz análise dos dados em formato word
rmarkdown::render(
  here::here("scripts", "10_NotasTecnicas", "nt6_analise_vagas.Rmd"),
  output_format = "word_document",
  output_dir = here::here("outputs", "NT6_VagasDeGaragemNosEixos"),
  output_file = 'nt6_analise_vagas', 
  output_options = list(toc = FALSE,
                        reference_docx = here::here(
                          "inputs", "1_inbound", "Insper", "Institucional",
                          "papel-timbrado-insper.docx")
  ))  
rm(list = ls())

 # 11. Portal -------------------------------------------------------

#
fs::dir_tree(here::here("scripts", "11_Portal"))

# Reprojeta dados geoespaciais para EPSG:4326 e deploy no Geoserver
# Dados requisitados pelas sessões do Terria
# Excluídos os dados de lotes que são tratados no script 2
source(here::here("scripts", "11_Portal", "dados_1.R"))
rm(list = ls())

# Reprojeta dados geoespaciais para EPSG:4326 e deploy no Geoserver
# Dados requisitados pelas sessões do Terria
# Dados gerais (exceto lotes) são tratados no script 1
source(here::here("scripts", "11_Portal", "dados_2.R"))
rm(list = ls())

# Cria pastas de dados customizados para visualização em histórias do portal
source(here::here("scripts", "11_Portal", "historia_01.R"))
# Historia 02 - Dados elaborados via QGIS
source(here::here("scripts", "11_Portal", "historia_03.R"))
source(here::here("scripts", "11_Portal", "historia_04.R"))
source(here::here("scripts", "11_Portal", "historia_05.R"))
rm(list = ls())


# Cria metadados para serem preenchidos via Planilha/Excel
# Arquivos são exportados para inputs/metadados/0_raw
# Arquivos preenchidos qualitativamente estão em inputs/metadados/1_trusted
source(here::here("scripts", "11_Portal", "interativo_metadados.R"))
source(here::here("scripts", "11_Portal", "interativo_dicionarios.R"))
rm(list = ls())