#---------------------------------------------------------------------------------------------------
# WORKING DIRECTORY
#---------------------------------------------------------------------------------------------------

## Should be predefined in your Rproj file.

#---------------------------------------------------------------------------------------------------
# PREAMBLE (PACKAGES NEEDED TO RUN SCRIPT)
#---------------------------------------------------------------------------------------------------

devtools::install_github('walkerke/tigris')
library(pacman)



p_load(gmodels,
  gtools,
  RPostgres,
  sqldf,
  readr,
  RODBC,
  ggmap,
  DT,
  rgdal,
  raster,
  phonics,
  UScensus2000tract,
  tmap,
  XML,
  sf,
  sp,
  rgeos,
  spatialEco,
  tigris,
  magrittr,
  rgdal,
  maptools,
  dplyr,
  dbplyr,
  lubridate,
  jsonlite,
  httr,
  ggplot2,
  here,
  stringi,
  stringr,
  data.table)

p_update()
#---------------------------------------------------------------------------------------------------
# LOOP SETUP
#---------------------------------------------------------------------------------------------------

# Set system time for checking performance after running
  start.time <- Sys.time()
  final = data.table()
# Make the database connection for retrieving Zillow data via SQL
  #zillowdbloc = rstudioapi::askForPassword('Where does your zillow database live? Please enter a full filepath.')

  #This is where mine lives
  zillowdbloc = '/Volumes/G-DRIVE-mobile-SSD-R-Series/ZTrans/sorting_wui.sqlite'

  database <- dbConnect(RSQLite::SQLite(), zillowdbloc)
  
#FULL HMDA DATA
  pwd = readline(prompt = "Enter your password for your HMDA database: ")
  user = readline(prompt = "Enter your username for your database. If you didn't set one, use 'postgres' ")
  hmda_database = dbConnect(Postgres(), host = 'localhost', dbname = 'hmda', 
    password = 'sortingproject', user = 'postgres')
  hmdadatabase = hmda_database

  #HMDA 2002-2019
  yrs = c(2002:2019)
  tblist = paste(rep('lar', each = length(yrs)), yrs, sep = '_')
  
  
  #RESET FINAL DATASET OF OBS
  mergestats_all_hmda <- data.frame(
    state = c(),
    year = c(),
    numb_trans = c(),
    numb_trans_loans = c(),
    numb_trans_loans_matched = c()
  )
  mergestats_all_hmda$state = as.factor(mergestats_all_hmda$state)
  mergestats_all_hmda$year = as.numeric(mergestats_all_hmda$year)
  mergestats_all_hmda$numb_trans = as.factor(mergestats_all_hmda$snumb_trans)
  mergestats_all_hmda$numb_trans_loans = as.factor(mergestats_all_hmda$numb_trans_loans)
  mergestats_all_hmda$numb_trans_loans_matched = as.factor(mergestats_all_hmda$numb_trans_loans_matched)
  
  #RESET FINAL DATASET
  final <- data.frame(
    state = c(),
    year = c()
  )
  
  
  # MD - no lender names but can do other matches
  #No loan amounts - VT
  ##Missing loan amount in some years/few loan amount values - GA (2016 - none), WY
  
  #state.id <- c("AL", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",  "ID",
  #             "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS", "MO",
  #            "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", 
  #           "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY") 
  

  
  
  # MD - no lender names but can do other matches
  #No loan amounts - VT
  ##Missing loan amount in some years/few loan amount values - GA (2016 - none), WY
  
  
  #Bring in older hmda data
  #memory.limit(size=5120000000)
  library(haven)

  
  
  state.id <- c("CA") 
  
 
  #MD, MN has no lendername.
 
 
 
  year.id <- c(2002:2019)
  

  # Define list to store elements of final data.table
  #list <- vector("list", length(state.id)*length(year.id))
  
  #---------------------------------------------------------------------------------------------------
  # LOOP
  #---------------------------------------------------------------------------------------------------
  #options(tigris_class = "sf" 
  #options(tigris_use_cache = FALSE)

#2000 tracts used for years 2004-2009
tracts2000 <- tracts('CA', county = NULL, cb = FALSE, year = 2000, refresh = TRUE)

#1990 tracts used through 2003 in HMDA reporting, but uses only less-well-defined tigris definitions, unfortunately  
tracts1990 <- tracts('CA', county = NULL, cb = TRUE, year = 1990, refresh = TRUE)

#2010 tracts used for HMDA data between 2010 and 2016
tracts2010 <- tracts('CA', county = NULL, cb = FALSE, year = 2010, refresh = TRUE)

#2015 tracts are used for 2017 and onwards
tracts2015 <- tracts('CA', county = NULL, cb = FALSE, year = 2015, refresh = TRUE)
    
    
#racts1990$TRACTCE90 <- paste(tracts1990$TRACTBASE,tracts1990$TRACTSUF , sep="")

#storage object for full list of unique respondent id's mapped to lender names for second most important join. This is time consuming to build, but allows for a more intelligent mapping process where lenders identfied out-of-year 
#get results.


buildfromdisk = TRUE
lendername_dictionary = NULL
pinfo = tbl(database, 'propInfo')
#pinfo = tbl(database, 'propTrans')
zillowSQL1_con = tbl(database, 'Main')
#zillowSQL1_con = tbl(database, 'mainAsmt')
maintranstab = tbl(database, 'largetransaction')
#maintranstab = tbl(database, 'mainTrans')

if(buildfromdisk){
  lendername_dictionary = readRDS(here('lendername_dict.dta'))
} else{print('reading in zillow db for speed')
      zilint = left_join(maintranstab, pinfo, by = 'TransId')
      zilint = zilint %>% filter(DataClassStndCode %in% c('D', 'H'))
      zilint1 = left_join(zilint, tbl(database, 'MainAssmt'), by = 'ImportParcelID')
      zilint1 = left_join(zilint1, tbl(database, 'bldg'), by = 'RowID')
      zillowSQLint = zilint1 %>% dplyr::select(SalesPriceAmount, PropertyAddressLatitude, LoanAmount, PropertyAddressLongitude, ImportParcelID, TransId, LenderDBAName, LenderIDStndCode, LenderName, InitialInterestRate, FIPS, RecordingDate, DocumentDate) %>% 
                                              mutate(date = as.Date(RecordingDate)) %>% collect() %>% as.data.table()}



for(i in 1:length(state.id)){
    for (j in 1:length(year.id)){
      print("YEAR and STATE are")
      print(paste(year.id[[j]]," and ", state.id[[i]]), sep = "")
      

      ## Build multi-year lender-respondent-id dictionary

        if(is.null(lendername_dictionary)){
          for (k in 1:length(year.id)) {
      lendername_dictionary = readRDS(lendername_dictionary, file = here('lendername_dict_no2019.dta'))
      print(paste0('starting dictionary build for ',year.id[[k]]))
      yrnum = year.id[k]
      #comb = left_join(zillowSQL1_con, maintranstab, by = 'TransId')

      #If Statement that loanamount is not all zeros
      zilint = left_join(maintranstab, pinfo, by = 'TransId')
      zilint = zilint %>% filter(DataClassStndCode %in% c('D', 'H'))
      zilint1 = left_join(zilint, tbl(database, 'MainAssmt'), by = 'ImportParcelID')
      zilint1 = left_join(zilint1, tbl(database, 'bldg'), by = 'RowID')
      
      zillowSQL = zillowSQLint %>% filter(date == yrnum)
      rm(zilint, zilint1)
      rm(test1)
      # Census tract data

     
      
      
      zillow.main <- zillowSQL[which(zillowSQL$PropertyAddressLatitude != "NA" & !is.na(zillowSQL$PropertyAddressLatitude) &
                                       ((zillowSQL$SalesPriceAmount!= "NA" & !is.na(zillowSQL$SalesPriceAmount) &
                                           zillowSQL$SalesPriceAmount>= 1000) & 
                                          (zillowSQL$LoanAmount > 0 
                                           & zillowSQL$LoanAmount != "NA" & !is.na(zillowSQL$LoanAmount)))) , ]
      
      setkey(zillow.main, NULL)
      zillow.main = unique(zillow.main)      

      
      rm(zillowSQL3)
      rm(zillowSQL1)
      rm(zillowSQL)

      zillow.main$year = year.id[[k]]

      
      
      # Zillow stuff
      
      # Drop rows that are not in the looped year j and rows that don't meet other criteria
      zillow <- zillow.main[which(zillow.main$PropertyAddressLatitude != "NA"), ]
      zillow <- zillow[which(zillow.main$PropertyAddressLongitude != "NA"), ]
      zillow = as.data.table(zillow)
      zillow <- distinct(zillow,ImportParcelID, SalesPriceAmount, RecordingDate, LoanAmount,
                         .keep_all = TRUE)
      zillow2 = zillow
      
      # Make loan amount comparable to HMDA syntax
      zillow2$loan_amount = round(zillow2$LoanAmount/1000)
      
      # Set up coordinates to match with Census Data
      zdf <- as.data.table(zillow2)
      
      
      
      
      #tracts2000 <- california.tract
      # Get Census CRS projection
      census.CRS <- st_crs(tracts2000)

      zdf = st_as_sf(zdf, coords = c("PropertyAddressLongitude", "PropertyAddressLatitude"), agr = "constant", crs = census.CRS)
      
      #proj4string(tracts1990) = CRS("+proj=longlat")
      
      # Transform data to match CRS projection from Census data
      cord.UTM <- st_transform(zdf, census.CRS)
      
      
      tracts1990 <- st_transform(tracts1990, census.CRS)
      
      #tracts2000 is baseline
      
      tracts2010 <- st_transform(tracts2010, census.CRS)

      tracts2015 = st_transform(tracts2015, census.CRS)
      
      tracts1990$TRACTSUF[is.na(tracts1990$TRACTSUF)] <- "00"
      #for 1990 tracts. NA sub for 00

      if(year.id[k] > 2017){
        zillow_tract <- st_join(cord.UTM, tracts2015, join = st_within)
        zillow_tract$id_number = zillow_tract$GEOID
      }
      if(year.id[k] == 2017){
        zillow_tract <- st_join(cord.UTM, tracts2015, join = st_within)
        zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
        # Add decimal to make mergeable to HMDA data
        zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }
      if(year.id[k] %in% c(2012:2016)){
        zillow_tract <- st_join(cord.UTM, tracts2010, join = st_within)
        zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
        # Add decimal to make mergeable to HMDA data
        zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }
      if(year.id[k] %in% c(2003:2011)){
          zillow_tract <- st_join(cord.UTM, tracts2000, join = st_within)
          zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }
      if(year.id[k] <= 2002){
        zillow_tract <- st_join(cord.UTM, tracts1990, join = st_within)
        zillow_tract$id_number <- paste(zillow_tract$CO, zillow_tract$TRACTBASE, '.', zillow_tract$TRACTSUF, sep = '')
      }
      
      
     #testing <- merge(pts.poly2, pts.poly2A, 
      #                 by.x = c("TransId"),
      #                 by.y = c("TransId"))
     #r <- as.data.frame(tracts1990)
     
      #rm(tracts2000)
      #rm(tracts1990)
      rm(coords)
      rm(cord.UTM)
      rm(cord.dec)
      rm(id)

      
      # Filter to unique transactions
      #count.zillow = 
      #setkey(zillow_temp)
      zillow3_filt = setDT(zillow_tract)[, .(n = .N), by = .(id_number,loan_amount)]
      zillow3 = merge(zillow_tract,zillow3_filt, by =c('id_number','loan_amount')) %>% filter(n == 1)
      
      zillow3_dt = zillow3 %>% as.data.table()
      # HMDA

      ## generate name of table from target year
      table_name = paste('lar', year.id[[k]], sep = '_')
      hmda = tbl(hmda_database, in_schema('hmda_public', table_name)) %>% dplyr::filter(state_code == '06' | state_code =='CA') %>% collect()
      hmda = hmda %>% tail(-1)
      hmda = as.data.table(hmda)
      if(year.id[[k]] > 2016){
        hmda$sequence_num = seq.int(nrow(hmda)) %>% as.character()
      }
      hmda$sequence_num = hmda$sequence_num %>% as.character() %>% parse_number()

      if(year.id[[k]] %in% c(year.id[[1]]:2017)){
        hmda %<>% mutate(agency_code = ifelse(is.na(agency_code), '023', agency_code))
        hmda %<>% mutate(respondent_id_prop = str_c(agency_code, respondent_id, sep = ''))
      }
      
      if(year.id[[k]] > 2017){
        hmda$respondent_id_prop = hmda$lei
      }

      # Keep only home purchases and create LoanAmount column to match Zillow's
      
      if(year.id[[k]] > 2017){
        hmda$loan_amount %<>% as.numeric()
        hmda %<>% as.data.table()
        hmda[,LoanAmount := loan_amount]
        hmda[,loan_amount := round(loan_amount/1000)]
      } else{hmda %<>% mutate(loan_amount = as.numeric(loan_amount)) %>% mutate(LoanAmount = loan_amount*1000)}
      
      # Create location identifiers to match Zillow data
      hmda$county_code <- parse_number(hmda$county_code)
      
      hmda$county_code <- paste(formatC(hmda$county_code, width = 3, flag = "0"), sep = "")
      
      hmda$county_code <- as.character(hmda$county_code)
      
      if(year.id[[k]] < 2018){
        hmda$id_number <- paste(hmda$state_code,hmda$county_code, hmda$census_tract, sep = '')
      } else{
        hmda$id_number = hmda$census_tract
      }
      # Drop loans without identifiers
      hmda <- hmda[which(hmda$id_number != 'NA'
                         & !is.na(hmda$LoanAmount)), ]
      
      
      
      
      # Filter to unique transactions
      count.hmda <- setDT(hmda)[, .(n = .N), by = c('id_number', 'loan_amount')]
      
      
      hmda2 <- merge(hmda, count.hmda,
                     by.x = c("id_number", "loan_amount"),
                     by.y = c("id_number", "loan_amount"))
      
      rm(count.hmda)
      
      hmda2 <- hmda2[which(hmda2$n == 1), ] %>% as.data.table()
      
      
      
      # 4) MERGES
      
      # FIRST: simple merge by id_number and LoanAmount
      zillow3_dt[,names(zillow3_dt[,-c('geometry')]) := lapply(.SD, trimws), .SDcols = names(zillow3_dt[,-c('geometry')])]
      hmda2[,names(hmda2) := lapply(.SD, trimws), .SDcols = names(hmda2)]
      hmda2$LoanAmount %<>% as.numeric()
      zillow3$LoanAmount %<>% as.numeric()
      
      
      merge1 <- merge(zillow3, hmda2, 
                      by.x = c("id_number", "LoanAmount"),
                      by.y = c("id_number", "LoanAmount"), .keep_all = TRUE)

      #coord = merge1$geometry
      merge1 %<>%  st_as_sf()
      coord = st_coordinates(merge1)
      merge1 %<>% as.data.table() %>% select(-geometry)
      merge1 = cbind(merge1, coord)

      zillow3$loan_amount = as.numeric(zillow3$loan_amount)
      hmda2$loan_amount = hmda2$loan_amount %>% as.numeric()
      merge12 = merge(zillow3, hmda2, 
                      by.x = c("id_number", "loan_amount"),
                      by.y = c("id_number", "loan_amount"), .keep_all = TRUE)
      #merge1$geometry
      merge12 %<>%  st_as_sf()
      coord = st_coordinates(merge12)
      merge12 %<>% as.data.table() %>% select(-geometry)
      merge12 = cbind(merge12, coord)

      merge1 = rbind(merge1,merge12, fill = TRUE) %>% unique()
      
      # SECOND: merge by respondent_id_prop, LoanAmount, and LenderName
      rm(hmda2)
      # Create database of LenderName using respondent_id_prop
      lendername <- subset(merge1, select = c(LenderName, respondent_id_prop, activity_year)) %>% as.data.table() %>% dplyr::select(LenderName, respondent_id_prop, activity_year)
      lendername_dictionary = rbind(lendername, lendername_dictionary)
      lendername_dictionary = unique(as.data.frame(lendername_dictionary))
      if(!exists('merge1_store')){
        merge1_store = merge1
      }
      if(year.id[[k]] == 2018){
        saveRDS(lendername_dictionary, file = here('lendername_dict_no2019.dta'))
      }
      if(year.id[[k]] == 2019){
        saveRDS(lendername_dictionary, file = here('lendername_dict.dta'))
      }
    }
  } else{
     if(buildfromdisk == FALSE){saveRDS(lendername_dictionary, file = here('lendername_dict.dta'))}
      # Pull in data needed to run code
      yrnum = year.id[[j]]
      # Zillow Transaction data (two parts, plus a merge)
      zillowSQL1_con = tbl(database, 'Main')
      maintranstab = tbl(database, 'largetransaction')
      #comb = left_join(zillowSQL1_con, maintranstab, by = 'TransId')
      zillowSQL1 = maintranstab %>% dplyr::select(TransId, FIPS, RecordingDate, LenderName, SalesPriceAmount, LoanAmount, InitialInterestRate) %>% 
                                              mutate(date = as.Date(RecordingDate))%>% dplyr::filter(date == yrnum) %>% collect()
       
      

    

      #If Statement that loanamount is not all zeros
      pinfo = tbl(database, 'propInfo')
      
      
      #zillowSQL3 = zilint1 %>% dplyr::select(PropertyAddressLatitude, PropertyAddressLongitude, ImportParcelID, TransId, LenderDBAName, LenderIDStndCode) %>% collect() %>% as.data.table()
      rm(zilint, zilint1)
      #rm(test1)
      # Census tract data
      zilint = left_join(maintranstab, pinfo, by = 'TransId')
      zilint = zilint %>% filter(DataClassStndCode %in% c('D', 'H'))
      zilint1 = left_join(zilint, tbl(database, 'MainAssmt'), by = 'ImportParcelID')
      zilint1 = left_join(zilint1, tbl(database, 'bldg'), by = 'RowID')
      zillowSQL = zilint1 %>%  filter(PropertyLandUseStndCode %in% c('RR101',  # SFR
                                            'RR999',  # Inferred SFR
                                           'RR102')) %>%
                                            dplyr::select(PropertyLandUseStndCode,SalesPriceAmount, PropertyAddressLatitude, LoanAmount, PropertyAddressLongitude, ImportParcelID, TransId, RowID, LenderDBAName, LenderIDStndCode, LenderName, InitialInterestRate, FIPS, RecordingDate, DocumentDate) %>% 
                                              mutate(date = as.Date(RecordingDate)) %>% dplyr::filter(date == yrnum) %>% collect() %>% as.data.table()
      
      
      
      zillow.main <- zillowSQL[which(zillowSQL$PropertyAddressLatitude != "NA" & !is.na(zillowSQL$PropertyAddressLatitude) &
                                       ((zillowSQL$SalesPriceAmount!= "NA" & !is.na(zillowSQL$SalesPriceAmount) &
                                           zillowSQL$SalesPriceAmount>= 1000) & 
                                          (zillowSQL$LoanAmount > 0 
                                           & zillowSQL$LoanAmount != "NA" & !is.na(zillowSQL$LoanAmount)))) , ] %>% as.data.table()
      
      setkey(zillow.main, NULL)
      zillow.main = unique(zillow.main)      

      
      rm(zillowSQL3)
      rm(zillowSQL1)
      rm(zillowSQL)

      zillow.main$year = year.id[[j]]

      
      
      # Zillow stuff
      
      # Drop rows that are not in the looped year j and rows that don't meet other criteria
      zillow <- zillow.main[which(zillow.main$PropertyAddressLatitude != "NA"), ]
      zillow <- zillow[which(zillow.main$PropertyAddressLongitude != "NA"), ]
      zillow <- unique(zillow.main, 
                         by = c('ImportParcelID', 'SalesPriceAmount', 'RecordingDate', 'LoanAmount'), 
                         .keep_all = TRUE)
      zillow2 = zillow
      
      # Make loan amount comparable to HMDA syntax
      zillow2$loan_amount = round(zillow2$LoanAmount/1000)
      
      # Set up coordinates to match with Census Data
      zdf <- as.data.table(zillow2)
      
      
      
      
      #tracts2000 <- california.tract
      # Get Census CRS projection
      census.CRS <- st_crs(tracts2000)

      zdf = st_as_sf(zdf, coords = c("PropertyAddressLongitude", "PropertyAddressLatitude"), agr = "constant", crs = census.CRS)
      
      #proj4string(tracts1990) = CRS("+proj=longlat")
      
      # Transform data to match CRS projection from Census data
      cord.UTM <- st_transform(zdf, census.CRS)
      
      
      tracts1990 <- st_transform(tracts1990, census.CRS)
      
      #tracts2000 is baseline
      
      tracts2010 <- st_transform(tracts2010, census.CRS)

      tracts2015 <- st_transform(tracts2015, census.CRS)
      
     tracts1990$TRACTSUF[is.na(tracts1990$TRACTSUF)] <- "00"
      #for 1990 tracts. NA sub for 00
      if(year.id[j] > 2017){
        zillow_tract <- st_join(cord.UTM, tracts2015, join = st_within)
        zillow_tract$id_number = zillow_tract$GEOID
      }
      if(year.id[j] == 2017){
        zillow_tract <- st_join(cord.UTM, tracts2015, join = st_within)
        zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
        # Add decimal to make mergeable to HMDA data
        zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }
      if(year.id[j] %in% c(2012:2016)){
        zillow_tract <- st_join(cord.UTM, tracts2010, join = st_within)
        zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
        # Add decimal to make mergeable to HMDA data
        zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }
      if(year.id[j] %in% c(2003:2012)){
          zillow_tract <- st_join(cord.UTM, tracts2000, join = st_within)
          zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }

      if(year.id[j] <= 2002){
        zillow_tract <- st_join(cord.UTM, tracts1990, join = st_within)
        zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTBASE, '.', zillow_tract$TRACTSUF, sep = '')
      }
      
      
     #testing <- merge(pts.poly2, pts.poly2A, 
      #                 by.x = c("TransId"),
      #                 by.y = c("TransId"))
     #r <- as.data.frame(tracts1990)
     
      #rm(tracts2000)
      #rm(tracts1990)
      rm(coords)
      rm(cord.UTM)
      rm(cord.dec)
      rm(id)
      # Add decimal to make mergeable to HMDA data
      #zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      
      # Filter to unique transactions
      #count.zillow = setDT(zillow_tract)[, .(n = .N), by = c('id_number', 'LoanAmount')]
      
      #zillow3 = setDT(zillow_tract)[, .(n = .N), by = c('id_number', 'loan_amount')] %>% filter(n == 1)
      zillow3_filt = setDT(zillow_tract)[, .(n = .N), by = .(id_number,LoanAmount)]
      zillow3 = merge(zillow_tract,zillow3_filt, by =c('id_number','LoanAmount')) %>% filter(n == 1)
      #zillow3 = merge(zillow3)
      # HMDA

      ## generate name of table from target year

      table_name = paste('lar', year.id[[j]], sep = '_')
      hmda = tbl(hmda_database, in_schema('hmda_public', table_name)) %>% dplyr::filter(state_code == '06' | state_code =='CA') %>% collect()
      if(year.id[j] == 2019) {hmda = hmda %>% tail(-1)}
      hmda = as.data.table(hmda)
      if(year.id[[j]] > 2016){
        hmda$sequence_num = seq.int(nrow(hmda)) %>% as.character()
      }
      hmda$sequence_num %<>% parse_number()
      
      if(year.id[[j]] %in% c(year.id[[1]]:2017)){
        hmda %<>% mutate(agency_code = ifelse(is.na(agency_code), '023', agency_code))
        hmda %<>% mutate(respondent_id_prop= str_c(hmda$agency_code, hmda$respondent_id, sep = ''))
      }
      
      if(year.id[[j]] > 2017){
        hmda %<>% mutate(respondent_id_prop = lei)
      }

      # Keep only home purchases and create LoanAmount column to match Zillow's
      if(year.id[[j]] > 2017){
        hmda %<>% as.data.table()
        hmda$loan_amount %<>% as.numeric()
        hmda = hmda[,LoanAmount := loan_amount]
        hmda = hmda[,loan_amount := round(loan_amount/1000)]
      } else{
        hmda$LoanAmount <- hmda$loan_amount %>% as.numeric() * 1000
      }
      # Create location identifiers to match Zillow data
      hmda$county_code <- parse_number(hmda$county_code)
      
      hmda$county_code <- paste(formatC(hmda$county_code, width = 3, flag = "0"), sep = "")
      
      hmda$county_code <- as.character(hmda$county_code)
       if(year.id[[j]] < 2018){
        hmda$id_number <- paste(hmda$county_code, hmda$census_tract, sep = '')
      } else{
        hmda$id_number = hmda$census_tract
      }
      # Drop loans without identifiers
      hmda <- hmda[which(hmda$id_number != 'NA'
                         & !is.na(hmda$LoanAmount)), ]
      
      
      
      
      # Filter to unique transactions
      count.hmda <- setDT(hmda)[, .(n = .N), by = c('id_number', 'loan_amount')]
      
      
      hmda2 <- merge(hmda, count.hmda,
                     by.x = c("id_number", "loan_amount"),
                     by.y = c("id_number", "loan_amount"))
      
      rm(count.hmda)
      
      hmda2 <- hmda2[which(hmda2$n == 1), ]
      
      
      
      # 4) MERGES
      
      # FIRST: simple merge by id_number and LoanAmount

      zillow3$LoanAmount %<>% as.numeric()
      hmda2$LoanAmount %<>% as.numeric()
      
     merge1 <- merge(zillow3, select(hmda2, c(id_number, LoanAmount, respondent_id_prop, sequence_num)), 
                by.x = c("id_number", "LoanAmount"),
                by.y = c("id_number", "LoanAmount"))

      merge1 %<>%  st_as_sf()
      coord = st_coordinates(merge1)
      merge1 %<>% as.data.table() %>% select(-geometry)
      merge1 = cbind(merge1, coord)

      zillow3$loan_amount = as.numeric(zillow3$loan_amount)
      hmda2$loan_amount = hmda2$loan_amount %>% as.numeric()
      
      merge12 = merge(zillow3, select(hmda2, c(id_number, loan_amount, respondent_id_prop, sequence_num)), 
                      by.x = c("id_number", "loan_amount"),
                      by.y = c("id_number", "loan_amount"))
      
      merge12 %<>% st_as_sf()
      coord = st_coordinates(merge12)
      merge12 %<>% as.data.table() %>% select(-geometry)
      merge12 = cbind(merge12, coord)
      zillow3a = zillow3 %>% mutate(loan_amount = floor(LoanAmount/1000))
      merge13 = merge(zillow3a, select(hmda2, c(id_number, loan_amount, respondent_id_prop, sequence_num)), 
                      by.x = c("id_number", "loan_amount"),
                      by.y = c("id_number", "loan_amount"))

      merge13 %<>% st_as_sf()
      coord = st_coordinates(merge13)
      merge13 %<>% as.data.table() %>% select(-geometry)
      merge13 = cbind(merge13, coord)
      zillow3a = zillow3 %>% mutate(loan_amount = ceiling(LoanAmount/1000))
      
      merge14 = merge(zillow3a, select(hmda2, c(id_number, loan_amount, respondent_id_prop, sequence_num)), 
                      by.x = c("id_number", "loan_amount"),
                      by.y = c("id_number", "loan_amount"))

      merge14 %<>% st_as_sf()
      coord = st_coordinates(merge14)
      merge14 %<>% as.data.table() %>% select(-geometry)
      merge14 = cbind(merge14, coord)

      merge1 = rbind(merge1, merge12, merge13, merge14) %>% unique(by = c('sequence_num'))
      
      # SECOND: merge by respondent_id_prop, LoanAmount, and LenderName
      rm(hmda2)
      merge1_store = merge1

  }

      merge1 = merge1_store %>% as.data.table()
      hmda = hmda %>% as.data.table() %>% setkey(respondent_id_prop)
      lendername = lendername_dictionary %>% dplyr::filter(activity_year == year.id[[j]])
      print('made it to hmda setkey')

      #build hashmap
      dictlist = as.list(lendername$LenderName)
      names(dictlist) = lendername$respondent_id_prop
      lendername.dict = list2env(dictlist, hash = TRUE)
      
      #dictionary querying helper-function
      qry = function(item, dict) {
        if(is.null(dict[[item]])) {return(NA)} else{return(dict[[item]])}}
      
      mappedvals = lapply(hmda$respondent_id_prop, FUN = qry, dict = lendername.dict)
      hmda$LenderName = do.call(rbind, mappedvals)[,1]
      #hmda[setkey(lendername[, .(LenderName, respondent_id_prop)],respondent_id_prop), on = 'respondent_id_prop', LenderName := i.LenderName]
      hmda_sameyr = hmda %>% filter(!is.na(LenderName))

      `%nin%` = Negate(`%in%`)

      if(year.id[[j]] + 1 <= max(year.id) + 1){
        lendername = lendername_dictionary %>% filter(activity_year == year.id[[j]] + 1 & respondent_id_prop %nin% hmda_sameyr$respondent_id_prop) 
        hmda_nextyr = hmda %>% filter(is.na(LenderName))
        print('made it to hmda nxtyr setkey')

        #make new hashmap
        dictlist = as.list(lendername$LenderName)
        names(dictlist) = lendername$respondent_id_prop
        lendername.dict = list2env(dictlist, hash = TRUE)
        mappedvals = lapply(hmda_nextyr$respondent_id_prop, FUN = qry, dict = lendername.dict)
        hmda_nextyr$LenderName = do.call(rbind, mappedvals)[,1]
        hmda_nextyr = hmda_nextyr %>% filter(!is.na(LenderName))
      } else{hmda_nextyr = NULL}

      if(year.id[[j]] - 1 >= min(year.id) - 1 & year.id[[j]] != 2018){
        lendername = lendername_dictionary %>% filter(activity_year == year.id[[j]] - 1 & respondent_id_prop %nin% hmda_sameyr$respondent_id_prop)
        if(exists('hmda_nextyr')){lendername %<>% filter(activity_year == year.id[[j]] - 1 & respondent_id_prop %nin% hmda_nextyr$respondent_id_prop)} 
        hmda_prevyr = hmda %>% filter(is.na(LenderName))

        #make new hashmap
        dictlist = as.list(lendername$LenderName)
        names(dictlist) = lendername$respondent_id_prop
        lendername.dict = list2env(dictlist, hash = TRUE)
        mappedvals = lapply(hmda_prevyr$respondent_id_prop, FUN = qry, dict = lendername.dict)
        hmda_prevyr$LenderName = do.call(rbind, mappedvals)[,1]
        hmda_prevyr = hmda_prevyr %>% filter(!is.na(LenderName))
      } else{hmda_prevyr = NULL}
      # The merge by id_number, LoanAmount, and LenderName

      hmda_temp = rbind(hmda_nextyr, hmda_prevyr, hmda_sameyr)

      #hmda_temp[,names(hmda_temp) := lapply(.SD, trimws), .SDcols = names(hmda_temp)]

      hmda3 <- hmda_temp[which(hmda_temp$id_number != 'NA' & !is.na(hmda_temp$id_number) & 
                              hmda_temp$LoanAmount != 'NA' & !is.na(hmda_temp$LoanAmount) &
                              hmda_temp$LenderName != "NA" & !is.na(hmda_temp$LenderName) & (hmda_temp$state_code == '06'|hmda_temp$state_code == 'CA') & hmda_temp$county_code != 'NA' & hmda_temp$census_tract != 'NA' & !is.na(hmda_temp$census_tract)), ]
      hmda3$loan_amount %<>% as.numeric()
      zillow4<- setDT(zillow_tract)[, .(n = .N), by = c('id_number', 'loan_amount', 'LenderName')] %>% filter(n == 1) %>% filter(!is.na(LenderName))
      zillow4$loan_amount %<>% as.numeric()
      merge2 <- merge(zillow4, select(hmda3, c(id_number, loan_amount, LenderName, sequence_num, respondent_id_prop)),
                      by.x = c("id_number", "loan_amount", "LenderName"),
                      by.y = c("id_number", "loan_amount", "LenderName"))
      
      # THIRD: same as last but with "fuzzy" names
      
      # Get "fuzzy" names for HMDA and Zillow
      hmda3a = hmda3 %>% mutate(LenderName2 = mra_encode(stri_replace_all_fixed(LenderName, ' ', '')))
      hmda3a$LenderName2 <- refinedSoundex(hmda3a$LenderName2, maxCodeLen = 4L)
      hmda3a = hmda3a[which(hmda3a$id_number != 'NA' & !is.na(hmda3a$id_number) &
                              hmda3a$LoanAmount != 'NA' & !is.na(hmda3a$LoanAmount) &
                              hmda3a$LenderName2 != "NA" & !is.na(hmda3a$LenderName2)) , ]
      hmda3a$loan_amount %<>% as.numeric()
      #hmda3a$LenderName <- NULL
      zillow2 = zillow_tract %>% mutate(LenderName2 = mra_encode(stri_replace_all_fixed(LenderName, ' ', '')))
      zillow2$LenderName2 <- refinedSoundex(zillow2$LenderName2,  maxCodeLen = 4L)
      
      #zillow2$LenderName <- NULL
      
      # Do the other stuff
      count.hmda.merge3 <- setDT(hmda3a)[, .(n = .N), by = c('id_number', 'loan_amount', 'LenderName2')]
      count.hmda.merge3$loan_amount %<>% as.numeric()

      hmda4 <- merge(hmda3a, count.hmda.merge3, 
                     by = c("id_number", "loan_amount", "LenderName2"))

      hmda4 = hmda4[which(hmda3$n == 1), ]

      hmda4 = hmda4[which(hmda4$id_number != 'NA' & !is.na(hmda4$id_number) &
                              hmda4$LoanAmount != 'NA' & !is.na(hmda4$LoanAmount) &
                              hmda4$LenderName2 != "NA" & !is.na(hmda4$LenderName2)), ]%>% mutate(loan_amount = as.numeric(loan_amount))
      
      rm(hmda3a)
      rm(hmda3)
      
      rm(count.hmda.merge3)
      
      count.zillow.merge3 <- setDT(zillow2)[, .(n = .N), by = c('id_number', 'LoanAmount', 'LenderName2', 'loan_amount')]
      
      zillow5 <- merge(zillow2, count.zillow.merge3,
                       by.x = c("id_number", "loan_amount", "LenderName2"),
                       by.y = c("id_number", "loan_amount", "LenderName2"))
      
      zillow5 <- zillow5[which(zillow5$n == 1), ] %>% mutate(loan_amount = as.numeric(loan_amount))
      zillow5$loan_amount %<>% as.numeric()
      hmda4$loan_amount %<>% as.numeric()
      merge3 <- merge(zillow5, select(hmda4, c(id_number, loan_amount, LenderName2, sequence_num, respondent_id_prop)) ,
                      by.x = c("id_number", "loan_amount", "LenderName2"),
                      by.y = c("id_number", "loan_amount", "LenderName2"))
      
      merge3$LenderName <- merge3$LenderName2
      
      merge3$LenderName2 <- NULL
      
      merge3$freq.y <- merge3$freq.x <- merge2$freq.y <- merge2$freq.x <- merge1$freq.y <- merge1$freq.x <- NULL
      
      # 5) The final merge
      
      merge1$match <- 1
      merge2$match <- 2
      merge3$match <- 3

      merge1$LoanAmount %<>% as.numeric()
      merge2$LoanAmount %<>% as.numeric()
      merge3$LoanAmount %<>% as.numeric()
      merge1$loan_amount %<>% as.numeric()
      merge2$loan_amount %<>% as.numeric()
      merge3$loan_amount %<>% as.numeric()
      
      #have_I_broken <- readline(prompt="Have I broken? ")
      final.merge <- smartbind(merge1, merge2, merge3)
      
      
      
      #rm(count.zillow)
      #rm(count.zillow.merge2)
      #rm(count.zillow.merge3)
      
      #rm(hmda4)
      #rm(hmda)
      #rm(pts.poly)
      #rm(pts.poly2)
      #rm(merge1)
      #rm(merge2)
      #rm(merge3)
      #rm(zillow3)
      #rm(zillow4)
      #rm(zillow5)
      
      # Filter out repeated values
      final.merge <- distinct(final.merge, id_number, loan_amount, LenderName, sequence_num, .keep_all = TRUE)
      
      final.merge <- as.data.table(final.merge)
      final.merge$COUNTYFP10 <- final.merge$FIPS <- final.merge$id_number <- NULL
      #final.merge$action_taken_name <- final.merge$TRACTCE00 <- final.merge$STATEFP00 <- NULL
      # Match to pre-defined list to merge all states and years into one data table
      #list[[i]][[j]] <- final.merge
      
      
      # NEED TO FIGURE OUT WHY THIS DATASET IS NOT RECORDING STATE
      numbtrans <- sum(with(zillow, year.id[j] == year))
      numbtransloans <- sum(with(zillow2, year.id[j] == year))
      numbtransloansmatch <- sum(with(final.merge, year.id[j] == year))
      
      mergestats_all_hmda2 <- data.frame(
        state = c('CA'),
        year = c(year.id[[j]]),
        numb_trans = c(numbtrans),
        numb_trans_loans = c(numbtransloans ),
        numb_trans_loans_matched = c(nrow(final.merge))
        
      )
      print(mergestats_all_hmda2)
      mergestats_all_hmda <- rbind(mergestats_all_hmda, mergestats_all_hmda2)
      
      
      
      final <- smartbind(final, final.merge)
      #list <- do.call("rbind", list)
      #final <- dplyr::bind_rows(list)
      final$STATEFP00 <- final$COUNTYFP00 <- final$TRACTCE00  <- NULL
      
      final$STATEFP <- final$COUNTYFP <- final$TRACTCE90  <- NULL
      final$COUNTY <- final$TRACT <- final$n.x <- final$n.y <- NULL
      rm(list=setdiff(ls(), c("lendername_dictionary", 'final.merge', 'final', 'mergestats_all_hmda', 'database', 'hmda_database', 'hmdadatabase', 'pinfo', 
      'maintranstab', 'tracts1990', 'tracts2000', 'tracts2010', 'tracts2015', 'zillowSQL1_con', 'year.id', 'j', 'i', 'state.id', 'buildfromdisk')))
      }
      }

      
  mergestats_hmda <- data.table(mergestats_all_hmda)
  write.dta(mergestats_hmda, here('InfoMerge/mergestats.dta'))
  final <- data.frame(final,stringsAsFactors = FALSE)
  #final$RecordingDate2 = as.character(final$RecordingDate, "%Y/%m/%d")
  #final$rate_spread <- final$freq.x <-  final$freq.y <- NULL
  write_dta(final, here('intermediate/final_hmdazil.dta'))

  
  
  
  


