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
  data.table,
  here,
  stringi)

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
  pwd = rstudioapi::askForPassword("Enter your password for your HMDA database:")
  user = rstudioapi::askForPassword("Enter your username for your database. If you didn't set one, use 'postgres'")
  hmda_database = dbConnect(Postgres(), host = 'localhost', dbname = 'hmda', 
    password = pwd, user = user)
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
    
tracts2000 <- tracts('CA', county = NULL, cb = TRUE, year = 2000, refresh = TRUE)

#1990 tracts used through 2003 in HMDA reporting  
tracts1990 <- tracts('CA', county = NULL, cb = TRUE, year = 1990, refresh = TRUE)
    
tracts2010 <- tracts('CA', county = NULL, cb = TRUE, year = 2010, refresh = TRUE)
    
    
#racts1990$TRACTCE90 <- paste(tracts1990$TRACTBASE,tracts1990$TRACTSUF , sep="")

#storage object for full list of unique respondent id's mapped to lender names for second most important join. This is time consuming to build, but allows for a more intelligent mapping process where lenders identfied out-of-year 
#get results.

lendername_dictionary = NULL
pinfo = tbl(database, 'propInfo')
#pinfo = tbl(database, 'propTrans')
zillowSQL1_con = tbl(database, 'Main')
#zillowSQL1_con = tbl(database, 'mainAsmt')
maintranstab = tbl(database, 'largetransaction')
#maintranstab = tbl(database, 'mainTrans')

for(i in 1:length(state.id)){
    for (j in 1:length(year.id)){
      print("YEAR and STATE are")
      print(paste(year.id[[j]]," and ", state.id[[i]]), sep = "")
      

      ## Build multi-year lender-respondent-id dictionary

        if(is.null(lendername_dictionary)){
          for (k in 1:length(year.id)) {
      print(paste0('starting dictionary build for ',year.id[[k]]))
      yrnum = year.id[k]
      #comb = left_join(zillowSQL1_con, maintranstab, by = 'TransId')
      zillowSQL1 = maintranstab %>% dplyr::select(TransId, FIPS, RecordingDate, LenderName, SalesPriceAmount, LoanAmount, InitialInterestRate) %>% 
                                              mutate(date = as.Date(RecordingDate))%>% dplyr::filter(date == yrnum) %>% collect()
       
      

    

      #If Statement that loanamount is not all zeros
      zilint = left_join(maintranstab, pinfo, by = 'TransId')
      zilint = zilint %>% filter(DataClassStndCode %in% c('D', 'H'))
      zilint1 = left_join(zilint, tbl(database, 'MainAssmt'), by = 'ImportParcelID')
      zilint1 = left_join(zilint1, tbl(database, 'bldg'), by = 'RowID')
      zilint1 = zilint1 %>% filter(PropertyLandUseStndCode %in% c('RR101',  # SFR
                                            'RR999',  # Inferred SFR
                                           'RR102')  # Rural Residence   (includes farm/productive land?)
             )
      
      zillowSQL3 = zilint1 %>% dplyr::select(PropertyAddressLatitude, PropertyAddressLongitude, ImportParcelID, TransId, LenderDBAName, LenderIDStndCode) %>% collect() %>% as.data.table()
      rm(zilint, zilint1)
      rm(test1)
      # Census tract data

      zillowSQL <- merge(zillowSQL1, zillowSQL3, by.x = c("TransId"),
                         by.y = c("TransId") , allow.cartesian = FALSE, all.x = TRUE)
      
      
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
      
      tracts1990$TRACTSUF[is.na(tracts1990$TRACTSUF)] <- "00"
      #for 1990 tracts. NA sub for 00
      if(year.id[k] > 2017){
        zillow_tract <- st_join(cord.UTM, tracts2010, join = st_within)
        zillow_tract$id_number = paste0(zillow_tract$COUNTY, zillow_tract$TRACT)
      }
      if(year.id[k] %in% c(2012:2017)){
        zillow_tract <- st_join(cord.UTM, tracts2010, join = st_within)
        zillow_tract$id_number <- paste(zillow_tract$COUNTY, zillow_tract$TRACT, sep = '')
        # Add decimal to make mergeable to HMDA data
        zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }
      if(year.id[k] %in% c(2004:2012)){
          zillow_tract <- st_join(cord.UTM, tracts2000, join = st_within)
          zillow_tract$id_number <- paste(zillow_tract$COUNTY, zillow_tract$TRACT, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }

      if(year.id[k] <= 2003){
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

      
      # Filter to unique transactions
      #count.zillow = 
      #setkey(zillow_temp)
      zillow3_filt = setDT(zillow_tract)[, .(n = .N), by = .(id_number,LoanAmount)]
      zillow3 = merge(zillow_tract,zillow3_filt, by =c('id_number','LoanAmount')) %>% filter(n == 1)
      
      zillow3_dt = zillow3 %>% as.data.table()
      # HMDA

      ## generate name of table from target year

      table_name = paste('lar', year.id[[k]], sep = '_')
      hmda = tbl(hmda_database, in_schema('hmda_public',table_name)) %>% collect()
      
      if(year.id[[k]] %in% c(year.id[[1]]:2017)){
        hmda$respondent_id_prop = stri_join(hmda$agency_code, hmda$respondent_id, sep = '')
      }
      
      if(year.id[[k]] > 2017){
        hmda$respondent_id_prop = hmda$lei
      }

      # Keep only home purchases and create LoanAmount column to match Zillow's
      hmda$LoanAmount <- hmda$loan_amount %>% as.numeric() * 1000
      
      # Create location identifiers to match Zillow data
      hmda$county_code <- parse_number(hmda$county_code)
      
      hmda$county_code <- paste(formatC(hmda$county_code, width = 3, flag = "0"), sep = "")
      
      hmda$county_code <- as.character(hmda$county_code)
      
      hmda$id_number <- paste(hmda$county_code, hmda$census_tract, sep = '')
      
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
      merge1 %<>%mutate(loan_amount = loan_amount.x) %>% select(-c('loan_amount.x', 'loan_amount.y')) %>% st_as_sf()
      coord = st_coordinates(merge1)
      merge1 %<>% as.data.table() %>% select(-geometry)
      merge1 = cbind(merge1, coord)

      zillow3$loan_amount = as.numeric(zillow3$loan_amount)
      hmda2$loan_amount = hmda2$loan_amount %>% as.numeric()
      merge12 = merge(zillow3, hmda2, 
                      by.x = c("id_number", "loan_amount"),
                      by.y = c("id_number", "loan_amount"))
      #merge1$geometry
      merge12 %<>% mutate(LoanAmount = LoanAmount.x) %>% select(-c('LoanAmount.x', 'LoanAmount.y')) %>% st_as_sf()
      coord = st_coordinates(merge12)
      merge12 %<>% as.data.table() %>% select(-geometry)
      merge12 = cbind(merge12, coord)

      p_load(sf)
      merge1 = rbind(merge1,merge12) %>% unique()
      
      # SECOND: merge by respondent_id_prop, LoanAmount, and LenderName
      rm(hmda2)
      # Create database of LenderName using respondent_id_prop
      lendername <- subset(merge1, select = c(LenderName, respondent_id_prop, activity_year)) %>% as.data.table() %>% dplyr::select(LenderName, respondent_id_prop, activity_year)
      lendername_dictionary = rbind(lendername, lendername_dictionary)
      lendername_dictionary = unique(lendername_dictionary)
      if(!exists('merge1_store')){
        merge1_store = merge1
      } 
    }
  } else{
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
      zilint = left_join(maintranstab, pinfo, by = 'TransId')
      zilint = zilint %>% filter(DataClassStndCode %in% c('D', 'H'))
      zilint1 = left_join(zilint, tbl(database, 'MainAssmt'), by = 'ImportParcelID')
      zilint1 = left_join(zilint1, tbl(database, 'bldg'), by = 'RowID')
      zilint1 = zilint1 %>% filter(PropertyLandUseStndCode %in% c('RR101',  # SFR
                                            'RR999',  # Inferred SFR
                                           'RR102',  # Rural Residence   (includes farm/productive land?)
                                            'RR104',  # Townhouse
                                            'RR105',  # Cluster Home
                                            'RR106',  # Condominium
                                            'RR107',  # Cooperative
                                            'RR108',  # Row House
                                            'RR109',  # Planned Unit Development
                                            'RR113',  # Bungalow
                                            'RR116',  # Patio Home
                                            'RR119',  # Garden Home
                                            'RR120'), # Landominium
             )
      
      zillowSQL3 = zilint1 %>% dplyr::select(PropertyAddressLatitude, PropertyAddressLongitude, ImportParcelID, TransId, LenderDBAName, LenderIDStndCode) %>% collect() %>% as.data.table()
      rm(zilint, zilint1)
      rm(test1)
      # Census tract data

      zillowSQL <- merge(zillowSQL1, zillowSQL3, by.x = c("TransId"),
                         by.y = c("TransId") , allow.cartesian = FALSE, all.x = TRUE)
      
      
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
      
     tracts1990$TRACTSUF[is.na(tracts1990$TRACTSUF)] <- "00"
      #for 1990 tracts. NA sub for 00
      if(year.id[k] > 2017){
        zillow_tract <- st_join(cord.UTM, tracts2010, join = st_within)
        zillow_tract$id_number = paste0(zillow_tract$COUNTY, zillow_tract$TRACT)
      }
      if(year.id[j] %in% c(2012:2017)){
        zillow_tract <- st_join(cord.UTM, tracts2010, join = st_within)
        zillow_tract$id_number <- paste(zillow_tract$COUNTY, zillow_tract$TRACT, sep = '')
        # Add decimal to make mergeable to HMDA data
        zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }
      if(year.id[j] %in% c(2004:2012)){
          zillow_tract <- st_join(cord.UTM, tracts2000, join = st_within)
          zillow_tract$id_number <- paste(zillow_tract$COUNTY, zillow_tract$TRACT, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
      }

      if(year.id[j] <= 2003){
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
      hmda = tbl(hmda_database, in_schema('hmda_public',table_name)) %>% collect() %>% as.data.table()
      
      if(year.id[[j]] %in% c(year.id[[1]]:2017)){
        hmda$respondent_id_prop = stri_join(hmda$agency_code, hmda$respondent_id, sep = '')
      }
      
      if(year.id[[j]] > 2017){
        hmda$respondent_id_prop = hmda$lei
      }

      # Keep only home purchases and create LoanAmount column to match Zillow's
      hmda$LoanAmount <- hmda$loan_amount %>% as.numeric() * 1000
      
      # Create location identifiers to match Zillow data
      hmda$county_code <- parse_number(hmda$county_code)
      
      hmda$county_code <- paste(formatC(hmda$county_code, width = 3, flag = "0"), sep = "")
      
      hmda$county_code <- as.character(hmda$county_code)
      
      hmda$id_number <- paste(hmda$county_code, hmda$census_tract, sep = '')
      
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
      
     merge1 <- merge(zillow3, hmda2, 
                by.x = c("id_number", "LoanAmount"),
                by.y = c("id_number", "LoanAmount"))

      merge1 %<>% mutate(loan_amount = loan_amount.x) %>% select(-c('loan_amount.x', 'loan_amount.y')) %>% st_as_sf()
      coord = st_coordinates(merge1)
      merge1 %<>% as.data.table() %>% select(-geometry)
      merge1 = cbind(merge1, coord)

      zillow3$loan_amount = as.numeric(zillow3$loan_amount)
      hmda2$loan_amount = hmda2$loan_amount %>% as.numeric()
      
      merge12 = merge(zillow3, hmda2, 
                      by.x = c("id_number", "loan_amount"),
                      by.y = c("id_number", "loan_amount"))
      
      merge12 %<>% mutate(LoanAmount = LoanAmount.x) %>% select(-c('LoanAmount.x', 'LoanAmount.y')) %>% st_as_sf()
      coord = st_coordinates(merge12)
      merge12 %<>% as.data.table() %>% select(-geometry)
      merge12 = cbind(merge12, coord)
      merge1 = rbind(merge1, merge12) %>% unique()
      
      # SECOND: merge by respondent_id_prop, LoanAmount, and LenderName
      rm(hmda2)
      merge1_store = merge1

  }

      merge1 = merge1_store %>% as.data.table()
      hmda = hmda %>% as.data.table() %>% setkey(respondent_id_prop)
      lendername = lendername_dictionary %>% filter(activity_year == year.id[[j]])
      hmda[setkey(lendername[,.(LenderName, respondent_id_prop)],respondent_id_prop), on = 'respondent_id_prop', LenderName := i.LenderName]
      hmda_sameyr = hmda %>% filter(!is.na(LenderName))

      `%nin%` = Negate(`%in%`)

      if(year.id[[j]] + 1 <= max(year.id) + 1 & year.id[[j]] != 2018){
        lendername = lendername_dictionary %>% filter(activity_year == year.id[[j]] + 1 & respondent_id_prop %nin% hmda_sameyr$respondent_id_prop) 
        hmda_nextyr = hmda %>% filter(is.na(LenderName))
        hmda_nextyr[setkey(lendername[,.(LenderName, respondent_id_prop)],respondent_id_prop), on = 'respondent_id_prop', LenderName := i.LenderName]
      } else{hmda_nextyr = NULL}

      if(year.id[[j]] - 1 >= min(year.id) - 1){
        lendername = lendername_dictionary %>% filter(activity_year == year.id[[j]] - 1 & respondent_id_prop %nin% hmda_sameyr$respondent_id_prop)
        if(exists('hmda_nextyr')){lendername %<>% filter(activity_year == year.id[[j]] - 1 & respondent_id_prop %nin% hmda_nextyr$respondent_id_prop)} 
        hmda_prevyr = hmda %>% filter(is.na(LenderName))
        hmda_prevyr[setkey(lendername[,.(LenderName, respondent_id_prop)],respondent_id_prop), on = 'respondent_id_prop', LenderName := i.LenderName]
      } else{hmda_prevyr = NULL}
      # The merge by id_number, LoanAmount, and LenderName

      hmda_temp = rbind(hmda_nextyr, hmda_prevyr, hmda_sameyr)

      #hmda_temp[,names(hmda_temp) := lapply(.SD, trimws), .SDcols = names(hmda_temp)]

      hmda3 <- hmda_temp[which(hmda_temp$id_number != 'NA' & !is.na(hmda_temp$id_number) & 
                              hmda_temp$LoanAmount != 'NA' & !is.na(hmda_temp$LoanAmount) &
                              hmda_temp$LenderName != "NA" & !is.na(hmda_temp$LenderName) & state_code == '06' & county_code != 'NA' & census_tract != 'NA' & !is.na(census_tract)), ]
      
      zillow4<- setDT(zillow_tract)[, .(n = .N), by = c('id_number', 'loan_amount', 'LenderName')] %>% filter(n == 1)
      
      merge2 <- merge(zillow4, hmda3,
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
      
      merge3 <- merge(zillow5, hmda4,
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
      final.merge <- distinct(final.merge, 
                              ImportParcelID, TransId, SalesPriceAmount, 
                              RecordingDate, LoanAmount, .keep_all = TRUE)
      
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
        numb_trans_loans_matched = c(numbtransloansmatch )
        
      )
      
      mergestats_all_hmda <- rbind(mergestats_all_hmda, mergestats_all_hmda2)
      
      
      
      final <- smartbind(final, final.merge)
      #list <- do.call("rbind", list)
      #final <- dplyr::bind_rows(list)
      final$STATEFP00 <- final$COUNTYFP00 <- final$TRACTCE00  <- NULL
      
      final$STATEFP <- final$COUNTYFP <- final$TRACTCE90  <- NULL
      final$COUNTY <- final$TRACT <- final$n.x <- final$n.y <- NULL
      rm(hmda_nextyr)
      }
      }

      
  mergestats_hmda <- data.table(mergestats_all_hmda)
  write.dta(mergestats_hmda, here('InfoMerge/mergestats.dta'))
  final <- data.frame(final,stringsAsFactors = FALSE)
  #final$RecordingDate2 = as.character(final$RecordingDate, "%Y/%m/%d")
  #final$rate_spread <- final$freq.x <-  final$freq.y <- NULL
  write_dta(final, here('intermediate/final_hmdazil.dta'))

  
  
  
  


