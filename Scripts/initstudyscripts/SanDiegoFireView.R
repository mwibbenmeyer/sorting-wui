### Script to add WUI, fire distance and fire potential to san diego data

library(pacman)
p_load(raster, sf, here, readxl, chunkR, tidyverse, broom, future, inline, pbmcapply, data.table, fst, LaF, bigreadr, RSQLite, DBI, magrittr, rgdal, gdalUtils, lubridate, fixest)


#set a directory. Here keeps this clean
dir <- here("/Raw_data/")#zillow")


#  Pull in layout information
layoutZAsmt <- read_excel(file.path(dir, 'layout.xlsx'), sheet = 1)
layoutZTrans <- read_excel(file.path(dir, 'layout.xlsx'), 
                           sheet = 2,
                           col_types = c("text", "text", "numeric", "text", "text", "text"))

#connect to sqlite database with all project data in it. Eventually I'll make a user-provided data path
dpath = readlines('where does your sqlite database live? Enter fp here: ')
#dpath = "/Volumes/G-DRIVE mobile SSD R-Series/ZTrans/sorting_wui.sqlite"con = dbConnect(SQLite(), dbname = dpath)

#filter lazily to san diego
SanDiegoTransactions1 = tbl(con, 'Main') %>% filter(County == 'SAN DIEGO')

#Temporary table pointer to hold property information
temp = tbl(con, 'propInfo')

#Join prop and main tables
SanDiegoTransactions2 = left_join(SanDiegoTransactions1, temp , by = 'TransId')
rm(temp)

#filter all data with low or null sales prices
SanDiegoTransactions_nonAL = SanDiegoTransactions2 %>% filter(SalesPriceAmount > 1000 & !is.na(SalesPriceAmount)) %>% filter(!is.na(ImportParcelID))

#Attach all *current* assessment data. Once my algortihm can handle the historic building information, this will be replaced with a filter and use that instead.
SanDiegoFullInfo = left_join(SanDiegoTransactions_nonAL, tbl(con, 'AssmtJoined'), by = 'ImportParcelID')

#read in
SanDiegoTransactionDf = SanDiegoFullInfo %>% filter(!is.na(PropertyAddressLongitude)& !is.na(PropertyAddressLatitude)) %>% collect()
SanDiegoTransactionDf %<>% as.data.table()

SanDiegoSF = st_as_sf(SanDiegoTransactionDf, coords = c("PropertyAddressLongitude", "PropertyAddressLatitude"), 
                 crs = 4269, agr = "constant")

#Neighborhood level data
zneighborhoods = read_sf('/Users/connor/Downloads/zillow-neighborhoods/zillow-neighborhoods.shp')
SanDiegoSF= st_join(SanDiegoSF, st_transform(zneighborhoods, st_crs(SanDiegoSF)), join = st_within)
SanDiegoSF %<>% mutate(name = ifelse(is.na(name), 'None', name))
#WUI flag data
test1 = ogr2ogr("/Users/connor/Downloads/ca_wui_cp12.gdb", "wui.shp", "ca_wui_cp12", nlt = "MULTIPOLYGON")

wuiogr = readOGR('wui.shp')

wui_sf = st_as_sf(wuiogr)

rm(wuiogr)

wfp = raster('Raw_data/WFP_Layers/RDS-2015-0047-2/Data/whp_2018_continuous/whp2018_cnt')
wfp2012 = raster('/Users/connor/Downloads/RDS-2015-0045 (1)/Data/wfp_2012_continuous/wfp2012_cnt/w001001.adf')

SanDiegoSFYrs$WFPC = raster::extract(wfp, SanDiegoSFYrs)

SanDiegoSFYrs$WFPC2012 = raster::extract(wfp2012, SanDiegoSFYrs)

wui_sf1 = st_transform(wui_sf, st_crs(SanDiegoSFYrs))
SanDiegoSFYrs = st_join(SanDiegoSFYrs, wui_sf1, join = st_within)
studyhome = st_join(studyhome, wui_sf1, join = st_within)
##Fire aggregation

allfires = st_read('/Users/connor/Downloads/Historic_GeoMAC_Perimeters_All_Years%3A_2000-2018-shp/US_HIST_FIRE_PERIMTRS_2000_2018_DD83.shp')
allfires = st_read(here("Raw_data/Historic_FirePerimetersshp/US_HIST_FIRE_PERIMTRS_2000_2018_DD83.shp"))

fire_studyperiod = allfires %>% filter(fireyear %in% c(2002:2010) & state == "CA") %>% mutate(incidentna = tolower(incidentna))


## IMP! Missing Wilcox fire (approx 100 acres, no shapefile exists. No home damage as a result)
sandiegofires_oct2007 = fire_studyperiod %>% filter(incidentna %in% c('poomacha', 'ammo', 'wilcox', 'harris', 'witch', 'mccoy', 'coronado hills', 'rice'))
sandiegofires_oct2007_singleshape = st_union(sandiegofires_oct2007) 
sandiegofires_2003 = fire_studyperiod %>% filter(fireyear == 2003)
sandiegofires_2003 = st_intersection(st_transform(sandiegofires_2003,st_crs(studyhome)), sandiegosf)


sandiegofires2008 = fire_studyperiod %>% filter(incidentna %in% c('november', 'juliett'))
sandiegofires2006 = fire_studyperiod %>% filter(incidentna %in% c('horse'))


SanDiegoSFYrs = SanDiegoSF %>% mutate(date = lubridate::as_date(RecordingDate)) %>% filter(year(date) %in% c(2000:2015)) %>% filter(PropertyLandUseStndCode %in% c('RR101',  # SFR
                                            'RR999',  # Inferred SFR
                                            'RR102',  # Rural Residence   (includes farm/productive land?)
                                            'RR109'  # Planned Unit Development
                                            ))

SanDiegoSF$dist2007 = st_distance(SanDiegoSF,st_transform(sandiegofires_oct2007_singleshape, st_crs(SanDiegoSF)), by_element = FALSE)
SanDiegoSF$dist2006 = st_distance(SanDiegoSF,st_transform(sandiegofires2006, st_crs(SanDiegoSF)), by_element = FALSE)

#set minimum and maximum range bands for a study area
minband = 1000
maxband = 2000
maxband2006 = 7000


units(minband) <- units(maxband) <- units(maxband2006) <- 'm'

firemask = st_disjoint(SanDiegoSFYrs, st_transform(sandiegofires_oct2007_singleshape, st_crs(SanDiegoSFYrs)))
SanDiegoSFYrs_Masked = SanDiegoSFYrs[as.logical(firemask),]
SanDiefoSFYrs_Masked = SanDiegoSFYrs_Masked[as.logical(st_disjoint(SanDiegoSFYrs, st_transform(st_union(sandiegofires2006), st_crs(SanDiegoSFYrs)))),]

#studyhome = SanDiegoSFYrs_Masked %>% as.data.table() %>% mutate(trt = ifelse((dist2007 > minband & dist2007 < maxband & dist2006 > maxband2006),1,0))
studyhome = SanDiegoSFYrs_Masked %>% mutate(date = lubridate::as_date(RecordingDate)) %>% filter(year(date) %in% c(2000:2015)) %>% mutate(qrtr = quarter(date, with_year = TRUE), Year = year(date), Month = month(date))

#Witch Fire
incidentdatewit = lubridate::as_date(c("2007-10-21"))
incidentyear = 2007

#Cedar Fire
incidentdateced = lubridate::as_date(c("2003-10-25"))

studyhome %<>% mutate(Month = month(date), Year = year(date), Week = week(date), postw = ifelse(date > incidentdatewit, 1, 0), postc = ifelse(date > incidentdateced, 1, 0))

#load local land cover information for better home valuation
#mylocaldata = '/Volumes/G-DRIVE-mobile-SSD-R-Series/NLCD_2006_Land_Cover_L48_20190424/NLCD_2006_Land_Cover_L48_20190424.img'
NLCD2006 = raster('yourpath/to/data') 

#too big for me to keep on my machine, so you need a way to access this file locally or through the in-built package 
#(which really just downloads the file.) Use the .img file in  to access the full dataset.

#takes a while. The NLCD is huge -potentially useful to define an extent first from which you can find subregions.
home_lc_data = raster::extract(NLCD2006, studyhome, buffer = 1000)

tmptab1 = lapply(home_lc_data, FUN = function(x) table(x)/(table(x) %>% sum()))

renamelambda = function(x, named){names(named[[x]]) = paste0('lc',names(named[[x]]))
 return(named[[x]])
	}

tmptab2 = lapply(1:length(tmptab1), FUN = renamelambda, named = something)

tmptab3 <- rbindlist(lapply(tmptab2, function(x) as.data.frame.list(x)), fill=TRUE)

tmptab3[is.na(tmptab3),] = 0

studyhome = cbind(studyhome, tmptab3)

rm(tmptab1, tmptab2, tmptab3)

#-----------------------------------------
#Estimation of models using fixest. By simply replacing WFPC2012 with either WUI_FLAG or vhfhsz in both points, you can see the differences in specification.

formula1 = as.formula(paste0('log(SalesPriceAmount) ~ ', paste0(grep("lc", names(studyhome), value=TRUE), collapse= '+'), '+', 'TotalBedrooms + I(TotalBedrooms^2)+ BuildingAreaSqFt + WFPC2012 + postw:WFPC2012| name + qrtr' ))

model1 = feglm(data = studyhome %>% as.data.table(), formula1)
summary(model1, cluster = 'name')


formula2 = as.formula(paste0('log(SalesPriceAmount) ~ ', paste0(grep("lc", names(studyhome), value=TRUE), collapse= '+'), '+', 'TotalBedrooms + I(TotalBedrooms^2)+ BuildingAreaSqFt + WFPC2012 + postc:WFPC2012| name + qrtr' ))

model2 = feglm(data = studyhome %>% as.data.table(), formula2)
summary(model2, cluster = 'name')

###
#With property fe
studyhomeredset = setDT(studyhome)[, .N, ImportParcelID] %>% filter(N > 1)
studyhome2 = studyhome[studyhome$ImportParcelID %in% studyhomeredset$ImportParcelID,]

formula3 = as.formula(paste0('log(SalesPriceAmount) ~ ', paste0(grep("lc", names(studyhome), value=TRUE), collapse= '+'), '+', 'TotalBedrooms + I(TotalBedrooms^2)+ BuildingAreaSqFt + WFPC2012 + postc:WFPC2012| Year + ImportParcelID' ))

model3 = feglm(data = studyhome2 %>% as.data.table(), formula3)
summary(model3)

## use fixest::etable to produce nice tables.
