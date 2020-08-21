library(pacman)
p_load(here, readxl, chunkR, tidyverse, broom, future, inline, pbmcapply, data.table, fst, LaF, bigreadr, RSQLite, DBI)

dir <- here("/Raw_data/zillow")


#  Pull in layout information
layoutZAsmt <- read_excel(file.path(dir, 'layout.xlsx'), sheet = 1)
layoutZTrans <- read_excel(file.path(dir, 'layout.xlsx'), 
                           sheet = 2,
                           col_types = c("text", "text", "numeric", "text", "text", "text"))

con = dbConnect(SQLite(), dbname = "/Volumes/G-DRIVE mobile SSD R-Series/ZTrans/sorting_wui.sqlite")
######################################################################
###  Create nonhistoric assessment data
#    Need 3 tables
#    1) Main table in assessor database
#    2) Building table


col_namesMain <- layoutZAsmt[layoutZAsmt$TableName == 'utMain', 'FieldName']
View(col_namesBldg) <- layoutZAsmt[layoutZAsmt$TableName == 'utBuilding', 'FieldName']
col_namesBldgA <- layoutZAsmt[layoutZAsmt$TableName == 'utBuildingAreas', 'FieldName']
col_namesPropInfo <- layoutZAsmt[layoutZAsmt$TableName == 'utAdditionalPropertyAddress', 'FieldName']

col_namesProp <- layoutZTrans[layoutZTrans$TableName == 'utPropertyInfo', 'FieldName']
col_namesMainTr <- layoutZTrans[layoutZTrans$TableName == 'utMain', 'FieldName']

# Through painful trial and error, this data is too large for memory (even a single column). Instead, we will use an outside method with a c-based peeper function that
# counts lines in external files

###############################################################################
#   Load PropertyInfo table for later merge

col_namesMainTrInt = c(1:5, 7,19:20, 38:40, 41, 25, 60)
col_namesPropTrInt = c(1,64, 65, 45, 47)

ztrans = here("Raw_data/zillow/ZTrans/Main.txt")



toobig = fread(ztrans,
select = col_namesMainTrInt,
sep = '|',
header = FALSE,
stringsAsFactors = FALSE,             
quote = "",
col.names = t(col_namesMainTr[col_namesMainTrInt,]$FieldName)
)

dbWriteTable(con, "Main", toobig)

rm(toobig)


zprop = here("Raw_data/zillow/ZTrans/PropertyInfo.txt")

toobig2 = fread(zprop,
select = col_namesPropTrInt,
sep = '|',
header = FALSE,
stringsAsFactors = FALSE,             
quote = "",
col.names = t(col_namesProp[col_namesPropTrInt,]$FieldName)
)

dbWriteTable(con, "propInfo", toobig2, overwrite = TRUE)
rm(toobig2)
sqft <- fread(file.path(dir, "ZAsmt/BuildingAreas.txt"),                    
              sep = '|',
              header = FALSE,
              stringsAsFactors = FALSE,                          
              quote = "",                                
              col.names = t(col_namesBldgA)
)

dbWriteTable(con, "sqft", sqft)
rm(sqft)

bldg_colint = c(1, 2, 3, 6, 9, 15, 16, 17, 18:27, 30:31, 36, 41)
bldg = fread(here('Raw_data/zillow/Zasmt/Building.txt'),
             select = bldg_colint,
             sep = '|',
             header = FALSE,
             stringsAsFactors = FALSE,             
             quote = "",
             col.names = t(col_namesBldg[bldg_colint,]$FieldName))

dbWriteTable(con, "bldg", bldg, overwrite = TRUE)
rm(bldg)

basecolsint = c(1,2,5,6,7,10,20:30,32,33,37,39,40, 52 , 80:84, 86, 87, 89)

base_dt = fread(here("Raw_data/zillow/Zasmt/Main.txt"),
                select= basecolsint,
                sep = '|',
                header = FALSE,
                stringsAsFactors = FALSE,             
                quote = "",
                col.names = t(col_namesMain[basecolsint,]$FieldName)
)


dbWriteTable(con, "mainAssmt", base_dt, overwrite = TRUE)
rm(base_dt)



chunkdfaster = function(skipnum, 
                        datapath = here("Raw_data/zillow/Historical/06/ZAsmtMain.txt"), 
                        keepcols = basecolsint,
                        colnames = t(col_namesMain[basecolsint,]$FieldName),
                                     tblname = "MainAssmtHist"){
  base_dt_hist = fread(datapath,
                     skip = skipnum,
                     nrows = 1000000,
                     select= keepcols,
                     sep = '|',
                     header = FALSE,
                     stringsAsFactors = FALSE,             
                     quote = "",
                     col.names = colnames)

dbWriteTable(con, tblname, base_dt_hist, append = TRUE)

}

dp = here("Raw_data/zillow/Historical/06/ZAsmtMain.txt")
numlines = wc(dp)

skipnumbers = seq(from = 1000000, to = numlines - 1000000, by = 1000000) + 1

pblapply(skipnumbers, chunkdfaster)
Main_db = tbl(con, "MainAssmt")
countylist = Main_db %>% dplyr::select(County) %>% collect() %>% unique()

mergerfcn = function(county, connect){
  print(paste0("Beginning write process for: ", county))
  collectedtrans = tbl(connect, "Main") #%>% filter(County == county)
  propinfo = tbl(connect, "propInfo")
  fulltrans = left_join(x = collectedtrans, y = propinfo, by = "TransId")
  collectedassmt = tbl(connect, "MainAssmt")
  bldg_tab = tbl(connect, "bldg")
  sqft_tab = tbl(connect, "sqft")
  tempassmt = left_join(x = collectedassmt, y = bldg_tab, by = 'RowID')
  fullassmt = left_join(x = tempassmt, y = sqft_tab, by = c("RowID", "BuildingOrImprovementNumber"))
  prod = left_join(fulltrans, fullassmt, by = "ImportParcelID")
  dbWriteTable(con, 'Modern')
  prod_out = prod %>% collect()
  filename = paste0("data_",county,".csv")
  fwrite(prod,paste0("/Volumes/G-DRIVE mobile SSD R-Series/county_data/", filename))
  print(paste0("Wrote data for ", filename))
}

dp = "/Volumes/G-DRIVE mobile SSD R-Series/Historical/ZAsmtBuilding.txt"
numlinesbldg = wc(dp)

skipnumbers = seq(from = 2000, to = numlinesbldg - 1000000, by = 1000000) + 1

pblapply(skipnumbers, chunkdfaster, 
         datapath = "/Volumes/G-DRIVE mobile SSD R-Series/Historical/ZAsmtBuilding.txt", 
         keepcols = bldg_colint,
         colnames = t(col_namesBldg[bldg_colint,]$FieldName),
         tblname = "BldgHist")

dp = "/Volumes/G-DRIVE mobile SSD R-Series/Historical/ZAsmtBuildingAreas.txt"
numlinesbldgA = wc(dp)

skipnumbers = seq(from = 2000, to = numlinesbldgA - 1000000, by = 1000000) + 1

pblapply(skipnumbers, chunkdfaster, 
         datapath = "/Volumes/G-DRIVE mobile SSD R-Series/Historical/ZAsmtBuildingAreas.txt", 
         keepcols = NULL,
         colnames = t(col_namesBldgA),
         tblname = "sqftHist")

collectedata = tbl(con, sql("SELECT * FROM Main AS a LEFT JOIN propInfo as b ON a.TransID = b.TransID"))
collectedata %>% head()

tab1 = tbl(con, "mainAssmt")
tab2 = tbl(con, "bldg")
ta