### Script to add WUI, fire distance and fire potential to san diego data

library(pacman)
p_load(here, readxl, chunkR, tidyverse, broom, future, inline, pbmcapply, data.table, fst, LaF, bigreadr, RSQLite, DBI)

dir <- here("/Raw_data/zillow")


#  Pull in layout information
layoutZAsmt <- read_excel(file.path(dir, 'layout.xlsx'), sheet = 1)
layoutZTrans <- read_excel(file.path(dir, 'layout.xlsx'), 
                           sheet = 2,
                           col_types = c("text", "text", "numeric", "text", "text", "text"))

con = dbConnect(SQLite(), dbname = "/Volumes/G-DRIVE mobile SSD R-Series/ZTrans/sorting_wui.sqlite")