#---------------------------------------------------------------------------------------------------
# WORKING DIRECTORY
#---------------------------------------------------------------------------------------------------

## Should be predefined in your Rproj file.

#---------------------------------------------------------------------------------------------------
# PREAMBLE (PACKAGES NEEDED TO RUN SCRIPT)
#---------------------------------------------------------------------------------------------------
#setwd('/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/')
devtools::install_github('walkerke/tigris')
library(pacman)
by_county = FALSE
yrdelta = 1

## still lean on this
`%nin%` <- Negate(`%in%`)


### Lala. Matt and Margaret provided variables of interest, and these were the ones identified.
hmda_cols = c("lei","state_code","census_tract","county_code","census_tract_number","activity_year","respondent_id",
              "agency_code","property_value","occupancy",
              "loan_amount","applicant_sex",
              "co_applicant_sex", "income", 'loan_type', 'loan_purpose',
              "applicant_ethnicity", "co_applicant_ethnicity",
              "applicant_race_1", "applicant_race_2", "co_applicant_race_1",
              "co_applicant_race_1", "co_applicant_race_2", "occupancy_type",
              "derived_dwelling_category","derived_ethnicity","derived_race",
              "derived_sex", "combined_loan_to_value_ratio","occupancy_type",
              "debt_to_income_ratio","applicant_ethnicity_1","applicant_ethnicity_2",
              "co_applicant_ethnicity_1", "co_applicant_ethnicity_2",
              "applicant_ethnicity_observed", "co_applicant_ethnicity_observed",
              "applicant_race_observed", "co_applicant_race_observed","applicant_sex_observed",
              "co_applicant_sex_observed", "applicant_age", "co_applicant_age",
              "owner_occupancy_name", "owner_occupancy",
              "applicant_ethnicity_name", "co_applicant_ethnicity_name", "applicant_race_name_1",
              "co_applicant_race_name_1", "applicant_race_name_2", "co_applicant_race_name_2", "applicant_sex_name",
              "co_applicant_sex_name", "applicant_income_000s", 'action_taken'
)

## allows a storing of a named list
lendername_dictionary = c()

#### this package list is inefficient and excessive. I have so many in here because asks can vastly change the basic structure of how you
#### attack this problem.
p_load(tigris,
       logr,
       optparse,
       gmodels,
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
       #UScensus2000tract,
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
       lubridate,
       jsonlite,
       httr,
       ggplot2,
       here,
       stringi,
       stringr,
       data.table,
       RSQLite,
       fuzzyjoin,
       pbapply,
       tidycensus,
       dplyr,
       dbplyr)

p_load(dplyr)

### basic options for command line runs.
option_list = list(
  make_option(c("-s", "--start_year"), type="integer", default=2007, 
              help="year to begin loop of merge", metavar="integer"),
    make_option(c("-e", "--end_year"), type="integer", default=2021, 
              help="year to end loop of merge", metavar="integer"),
  make_option(c('-u', '--urban'), type = "character", default='',
              help="whether to filter to large counties only, pass TRUE or FALSE or nothing", 
              metavar = "character")
)

logdir = paste0('log_file_merge_', Sys.Date(), '.log')
tmp <- file.path(tempdir(), logdir)

# Open log
lf <- log_open(tmp)

opt_parser = OptionParser(option_list=option_list)
opt = parse_args(opt_parser)

start_year = opt$start_year
end_year = opt$end_year

init_string = paste(c('Beginning merge running from', as.character(start_year), 'to', as.character(end_year)), collapse = ' ')
log_print(init_string)

#p_update()
#---------------------------------------------------------------------------------------------------
# LOOP SETUP
#---------------------------------------------------------------------------------------------------

# Set system time for checking performance after running
start.time <- Sys.time()
final = data.table()
# Make the database connection for retrieving Zillow data via SQL
#zillowdbloc = rstudioapi::askForPassword('Where does your zillow database live? Please enter a full filepath.')

#This is where mine lives
#zillowdbloc = '/media/connor/T7/database_ztrax/sortingwui_ztrax'
zillowdbloc = '/home/connor/Desktop/rfffiles/wui-afri/sortingwui_ztrax'

  # '/media/connor/T7/database_ztrax/sortingwui_ztrax'
  
  #'/Volumes/T7/database_ztrax/sortingwui_ztrax'

database <- dbConnect(RSQLite::SQLite(), zillowdbloc)

#FULL HMDA DATA
#pwd = readline(prompt = "Enter your password for your HMDA database: ")
#user = readline(prompt = "Enter your username for your database. If you didn't set one, use 'postgres' ")
hmda_database = dbConnect(Postgres(), host = 'localhost', dbname = 'hmda',password="fire-sorting", user = 'connor', 
                          options="-c search_path=hmda_public")
hmdadatabase = hmda_database

#ts data to map 2018 & 2019 rid to 2017 and earlier - we don't really use this, but available if match rates fall to very low levels
#tsdata = read_csv('/2018_public_panel_csv.csv')
#tsdict = tsdata %>% dplyr::select(arid_2017, lei)
#tsdict$respondent_id = paste0(substr(tsdict$arid_2017, 1, 1), str_pad(substr(tsdict$arid_2017, 2, 10), 10, pad = '0'))
#tsdict = tsdict %>% dplyr::select(respondent_id, lei)

#HMDA 2002-2019
yrs = c(start_year:end_year)
tblist = paste(rep('lar', each = length(yrs)), yrs, sep = '_')


#RESET FINAL DATASET OF OBS
mergestats_all_hmda <- data.frame(
  state = c(),
  county = c(),
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


#Bring in older hmda data
#memory.limit(size=5120000000)
library(haven)



state.id <- c("CA") 


year.id <- c(start_year:end_year)


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

#2020 tracts begin to be used for 2022
tracts2022 <- tracts('CA', county = NULL, cb = FALSE, year = 2020, refresh = TRUE)


#querying helper function
qry = function(item, dict) {
  if(is.null(dict[[item]])) {return(NA)} else{return(dict[[item]])}}
merge_fin = data.table()
#storage object for full list of unique respondent id's mapped to lender names for second most important join. This is time consuming to build, but allows for a more intelligent mapping process where lenders identfied out-of-year 
#get results.

##### PARAMETER - IF TRUE, WILL MATCH USING CROSS-YEAR LOAN IDS. 
buildfromdisk = FALSE

#lendername_dictionary = NULL
#pinfo = tbl(database, 'propInfo')
#pinfo = tbl(database, 'propTrans')
#zillowSQL1_con = tbl(database, 'mainAssmt')
#zillowSQL1_con = tbl(database, 'mainAsmt')
#maintranstab = tbl(database, 'transMain')
#maintranstab = tbl(database, 'largetransaction')

#######
####### Extension to build a dictionary of matches ACROSS years to find matches in different regions 
#######
if(buildfromdisk){
  lendername_dictionary = readRDS('/lendername_dict.rds')
} else{print('reading in zillow db for speed')
  # zilint = left_join(maintranstab, pinfo %>% dplyr::select(TransId, ImportParcelID), by = 'TransId')
  # zilint = zilint %>% filter(DataClassStndCode %in% c('D', 'H', 'M'))
  # zilint1 = left_join(zilint %>% dplyr::select(-FIPS), tbl(database, 'mainAssmt'), by = 'ImportParcelID')
  # zilint1 = left_join(zilint1, tbl(database, 'bldgAssmt'), by = 'RowID')
  # zillowSQLint = zilint1  %>% collect() %>% as.data.table()  %>% mutate(date = as_date(RecordingDate))
  
  zillowSQLint = zillowSQLint = tbl(database, 'ztrax_view')
  
}

print('starting read from remote disk')

zillowSQLint= tbl(database, 'ztrax_view')

if(opt$urban == 'TRUE'){
  print('Filtering only to counties that ARE Los Angeles or San Diego')
  zillowSQLint %<>% filter(County %in% c("LOS ANGELES", "SAN DIEGO"))
  cnty_codes = c("06073","06037")
  bycounty = FALSE
  
} else if(opt$urban == 'FALSE'){
  print('Filtering only to counties that are NOT Los Angeles or San Diego')
  zillowSQLint %<>% filter(County %nin% c("LOS ANGELES", "SAN DIEGO"))
  cnty_codes = setdiff((tracts2010 %>% 
                         mutate(county_code = paste0('06', COUNTYFP10)) %>% 
                         select(county_code) %>% unique())$county_code, c("06073","06037"))
  bycounty = FALSE

} else if(opt$urban == ''){
  print('Not filtering on size of county for merge, may run into out of memory issues')
  # cnty_codes = (tracts2010 %>% 
  #                        mutate(county_code = paste0('06', COUNTYFP10)) %>% 
  #                        select(county_code) %>% unique())$county_code
  bycounty = FALSE
} else if(opt$urban == 'by_county'){
  print('Looping by county code')
  cnty_codes_full = (tracts2010 %>% 
                  mutate(county_code = paste0('06', COUNTYFP10)) %>% 
                  select(county_code) %>% unique())$county_code %>% unique()
  bycounty = TRUE
  
} else{
  print("I don't recognize this option-cancelling operation")
  quit('no')
  
}
bycounty=FALSE

######
merge_dat_out = data.table()
if(bycounty == FALSE){
  for(i in 1:length(state.id)){
    j =1
    lei_flag = FALSE
    
    if(year.id[[j]] %in% c(2018,2019,2020,2021)){
      lei_flag = TRUE
    }
    
    if(lei_flag){
      print('setting respondent id to be consistent with 2002-2017')
      panname = paste0('panel_', year.id[[j]])
      hmda_pan = tbl(hmdadatabase, in_schema('hmda_public', panname)) %>% filter(id_2017 != '-1') %>% collect()
      hmda_pan = hmda_pan %>% mutate(respondent_id_prop2 = paste0(substr(agency_code, 1, 1), str_pad(id_2017, 10, pad = '0')))
      tsdict = hmda_pan %>% dplyr::select(respondent_id_prop2, lei)
    }
    
    ## Build multi-year lender-respondent-id dictionary
    
    for (k in 1:length(year.id)){
        print("YEAR and STATE are")
        print(paste(year.id[[k]]," and ", state.id[[i]]), sep = "")
      
        #lendername_dictionary = readRDS(lendername_dictionary, file = '/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/lendername_dict_no2019.dta')
        print(paste0('starting dictionary build for ',year.id[[k]]))
        yrnum = year.id[k]
        if(yrnum >= 2018){
          post2018 = TRUE
        } else{
          post2018 = FALSE
        }
        
        #If Statement that loanamount is not all zeros
        #zilint = left_join(maintranstab, pinfo, by = 'TransId')
        #zilint = maintranstab %>% filter(DataClassStndCode %in% c('D', 'H'))
        #zilint1 = left_join(zilint, tbl(database, 'mainAssmt'), by = 'ImportParcelID')
        #zilint1 = left_join(zilint1, tbl(database, 'bldg'), by = 'RowID')
        
        zillowSQL = zillowSQLint  %>%
           collect() %>%#add back in for raw file data
          mutate(date2 = as_date(RecordingDate), date = ifelse(is.na(DocumentDate), date2, as_date(DocumentDate))) %>% 
          filter((str_detect(AssessmentLandUseStndCode, "RR")|str_detect(AssessmentLandUseStndCode, "AG"))&(str_detect(AssessmentLandUseStndCode, "VL", negate = TRUE))) # either a residence, or a ag/rural residence, but not classified as vacant.
        
        
        zillowSQL = zillowSQL %>% mutate(date = as_date(date), yr = year(date), semester = semester(date, with_year = TRUE)) %>% filter(yr == yrnum) %>% group_by(TransId) %>% mutate(idcount = n()) %>% filter(idcount <= 1)
        #zillowSQL = merge(zillowSQL, zillowSQLint, by = intersect(colnames(zillowSQL), colnames(zillowSQLint)), all.x = T, all.y = F) ## for merge logic from RFF source file
        
        setDT(zillowSQL)[, c('LoanAmount','SalesPriceAmount','PropertyAddressLatitude', 'PropertyAddressLongitude') := nafill(.SD, "locf"), 
                         by = .(ImportParcelID, semester), .SDcols = c('LoanAmount','SalesPriceAmount', 'PropertyAddressLatitude', 'PropertyAddressLongitude')] #character fill not supported by dt methods - use tidyr instead
        
        zillowSQL %<>% group_by(TransId) %>% tidyr::fill(LenderName, .direction = "updown") %>% ungroup()
        zillowSQL = unique(zillowSQL, by = c('LoanAmount','SalesPriceAmount', 'ImportParcelID', 'quarter'))
        merge_dat_1 = zillowSQL %>% filter(SalesPriceAmount >= 1000 & !is.na(PropertyAddressLatitude)) %>% group_by(County) %>% summarize(raw_count = n())
        
        #date = ifelse(is.na(date), date2, date)) %>%
          #filter(lubridate::year(date) == 2020)
        zillow.main <- zillowSQL[which(zillowSQL$PropertyAddressLatitude != "NA" & !is.na(zillowSQL$PropertyAddressLongitude) &
                                         ((zillowSQL$SalesPriceAmount!= "NA" & !is.na(zillowSQL$SalesPriceAmount) &
                                             zillowSQL$SalesPriceAmount>= 1000) & 
                                            (zillowSQL$LoanAmount > 0 
                                             & zillowSQL$LoanAmount != "NA" & !is.na(zillowSQL$LoanAmount)))) , ]
        
        log_print(paste0(nrow(zillow.main), ' rows in zillow.main after filtering to valid loan/sale values. Does this seem right for all of California?'))
        print('past zillow.main (line 368)')
        rm(zillowSQL)
        ### collect data on filtering down to total loan-transaction pairs
        merge_dat_2 = zillow.main %>% group_by(County) %>% summarize(filtered_count = n())
        
        setkey(zillow.main, NULL)
        zillow.main = unique(zillow.main)      
        
        
        #rm(zillowSQL3)
        #rm(zillowSQL1)
        #rm(zillowSQL)
        
        zillow.main$year = year.id[[k]]
        
        
        
        # Zillow stuff
        
        # Drop rows that are not in the looped year j and rows that don't meet other criteria
        zillow <- zillow.main[which(zillow.main$PropertyAddressLatitude != "NA"), ]
        zillow <- zillow[which(zillow$PropertyAddressLongitude != "NA"), ]
        zillow = as.data.table(zillow)
        print('past zillow (line 391)')
        rm(zillow.main)
        propvalues = tbl(database,'assmtValue') %>% dplyr::select(RowID, TotalAssessedValue) %>% collect() %>% as.data.table()
        zillow <- merge(setDT(zillow),propvalues)
        zillow <- distinct(zillow,ImportParcelID, SalesPriceAmount, RecordingDate, LoanAmount, TransId, .keep_all = TRUE)
        zillow2 = zillow
        print('past zillow2')
        
        # Make loan amount comparable to HMDA syntax
        if(yrnum < 2018){
          zillow2$loan_amount = round(zillow2$LoanAmount/1000)
        } else{
          
          #in 2019 and on, data for loans begins to be reported in mid 10000s increments. This means we need to
          #match loan amounts on the 5000 dollar mark that falls at the midpoint in the 10000 dollar range.
          
          # So - equal to (ceiling(loan/10k) + floor(loan/10k))/2 * 10, or (ceiling(loan/10k) + floor(loan/10k))*5
          zillow2$loan_amount = (floor(zillow2$LoanAmount/10000))*10 + 5
          #for boundary cases
          zillow2 %<>% mutate(loan_amount = ifelse(loan_amount%%10 == 0, loan_amount + 5, loan_amount)) 
        }
        #zillow2 = zillow2 %>% mutate(LoanAmount = loan_amount*1000)
        
        # Set up coordinates to match with Census Data
        #zdf <- as.data.table(zillow2)
        print('past zdf (line 416)')
        #@rm(zillow2)
        
        
        #tracts2000 <- california.tract
        # Get Census CRS projection
        if(yrnum %in% c(2003:2011)){
          tract = tracts2000
        }
        if(yrnum %in% c(2012:2016)){
          tract = tracts2010
        }
        if(yrnum %in% c(2017:2021)){
          tract = tracts2015
        }
        else{
          tract = tracts2020
        }
        
        census.CRS <- st_crs(tract)
        ## SAN BERNADINO, SAN DIEGO, FRESNO, SAN MATEO, SOLANO, YOLO, SUTTER
        counties_with_wgs84 = c("SAN BERNADINO", "SAN DIEGO", "FRESNO", "SAN MATEO", "SOLANO", "YOLO", "SUTTER")
        zdf_1 = st_as_sf(zillow2 %>% filter(County %nin% counties_with_wgs84), coords = c("PropertyAddressLongitude", "PropertyAddressLatitude"), agr = "constant", crs = 4267) %>% st_transform(crs = census.CRS)
        zdf_2 = st_as_sf(zillow2 %>% filter(County %in% counties_with_wgs84), coords = c("PropertyAddressLongitude", "PropertyAddressLatitude"), agr = "constant", crs = 4326)%>% st_transform(crs = census.CRS)
        zdf = bind_rows(zdf_1, zdf_2)
        print('past zdf (line 435)')
        #proj4string(tracts1990) = CRS("+proj=longlat")
        
        # Transform data to match CRS projection from Census data
        cord.UTM <- st_transform(zdf, census.CRS)
        print('past cord.utm (line 440)')
        rm(zdf)
        # tracts1990 <- st_transform(tracts1990, census.CRS)
        
        #tracts2000 is baseline
        
        tracts2010 <- st_transform(tracts2010, census.CRS)
        
        tracts2015 = st_transform(tracts2015, census.CRS)
        
        #tracts1990$TRACTSUF[is.na(tracts1990$TRACTSUF)] <- "00"
        #for 1990 tracts. NA sub for 00
        gc(reset = TRUE)
        log_print('past tract transform (line 453)')
        if(year.id[k] > 2017){
          #tracts_yr = st_join(cord.UTM, tracts2015, join = st_within)
          zillow_tract <- st_join(cord.UTM, tracts2015, join = st_within)
          #rm(cord.UTM)
          
          
          zillow_tract$id_number = zillow_tract$GEOID
        }
        if(year.id[k] == 2017){
          zillow_tract <- st_join(cord.UTM, tracts2015, join = st_within)
          #rm(cord.UTM)
          
          zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
          zillow_tract %<>% mutate(id_number=paste0('06',zillow_tract$id_number))
          
        }
        if(year.id[k] %in% c(2012:2016)){
          zillow_tract <- st_join(cord.UTM, tracts2010, join = st_within)
          #rm(cord.UTM)
          
          #zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$GEOID10)
          #zillow_tract %<>% mutate(id_number=paste0('06',zillow_tract$id_number))
          
        }
        if(year.id[k] %in% c(2003:2012)){
          zillow_tract <- st_join(cord.UTM, tracts2000, join = st_within)
          #rm(cord.UTM)
          zillow_tract$id_number <- paste(zillow_tract$COUNTYFP00, zillow_tract$TRACTCE00, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
          zillow_tract %<>% mutate(id_number=paste0('06', zillow_tract$id_number))
          
          #zillow_tract %<>% mutate(id_number=paste0('06', zillow_tract$id_number))
        }
        if(year.id[k] <= 2002){
          zillow_tract <- st_join(cord.UTM, tracts1990, join = st_within)
          zillow_tract$id_number <- paste(zillow_tract$CO, zillow_tract$TRACTBASE, '.', zillow_tract$TRACTSUF, sep = '')
        }
        #rm(cord.UTM)
        print('past tract/ztrax merge (line 497)')
        
        #testing <- merge(pts.poly2, pts.poly2A, 
        #                 by.x = c("TransId"),
        #                 by.y = c("TransId"))
        #r <- as.data.frame(tracts1990)
        
        #rm(tracts2000)
        #rm(tracts1990)
        #rm(coords)
        #rm(cord.UTM)
        #rm(cord.dec)
        #rm(id)
        
        
        # Filter to unique transactions
        #count.zillow = 
        #setkey(zillow_temp)
        
        ######## For merge 1
        zillow3_filt = setDT(zillow_tract)[, .(n = .N), by = .(id_number,loan_amount)]
        zillow3 = merge(zillow_tract,zillow3_filt, by =c('id_number','loan_amount')) %>% filter(n == 1)
        
        ####### for merge 1.5
        if(post2018){
          zillow_tract = zillow_tract %>% mutate(property_val_merge = floor(TotalAssessedValue/10000)*1000 + 5)
          zillow3.5_filt = setDT(zillow_tract)[, .(n = .N), by = .(id_number,loan_amount, property_val_merge)]
          zillow3.5 = merge(zillow_tract,zillow3.5_filt, by =c('id_number','loan_amount', 'property_val_merge')) %>% filter(n == 1)
        }
        
        rm(zillow3_filt, zillow3.5_filt)
        
        zillow3_dt = zillow3 %>% as.data.table()
        log_print('past final zillow transform (line 530)')
        gc(reset = TRUE)
        
        ######################## HMDA DATA PROCESSING #######################
        
        ## generate name of table from target year
        table_name = paste('lar',as.character(yrnum), sep = '_')
        if(yrnum == 2017) { ### in 2017 hmda updates format signif. 
          hmda = tbl(hmda_database, table_name) %>% dplyr::filter(state_code == 'CA'|state_code == "06") %>% collect()
          #hmda %<>% mutate(loan_amount = loan_amount_000s)
        } else if(yrnum < 2017){
          hmda = tbl(hmda_database, table_name) %>% dplyr::filter(state_code == '06' | state_code =='CA') %>% collect()
        } else{
          hmda = tbl(hmda_database, table_name) %>% dplyr::filter(state_code == '06' | state_code =='CA') %>% 
            collect()
          hmda %<>% mutate(respondent_id = lei)
          #hmda %<>% mutate(loan_amount = as.integer(loan_amount)/1000)
          #hmda %<>% filter(denial_reason_1=='10') %>% filter(loan_purpose %in% c('1','5'))
        }
        print('past first hmda read in (line 549)')
        ## store full table for future years of data
        #hmda_store = hmda
        hmda = hmda %>% dplyr::select(any_of(hmda_cols))
        hmda_base = hmda
        
        hmda$activity_year = year.id[[j]]
        #hmda = hmda %>% tail(-1)
        hmda = as.data.table(hmda)
        hmda$sequence_num = hmda$sequence_num %>% as.character() %>% parse_number()
        
        if(year.id[[k]] %in% c(2000:2017)){
          hmda %<>% mutate(agency_code = ifelse(is.na(agency_code), '023', agency_code))
          hmda %<>% mutate(respondent_id = str_c(agency_code, respondent_id, sep = ''))
        }
        
        # Keep only home purchases and create LoanAmount column to match Zillow's
        
        if(post2018){
          hmda$loan_amount %<>% as.numeric()
          hmda[,LoanAmount := loan_amount]
          hmda %<>% mutate(property_val_merge = as.numeric(property_value)/1000)
          hmda %<>% mutate(respondent_id = lei,
                           agency_code = '')
          hmda %<>% mutate(loan_amount = as.numeric(loan_amount)/1000)
          
          
          hmda %<>% filter(action_taken %in% c(1)|action_taken %in% c('1')) %>% 
            filter(loan_purpose %in% c(1,3,31,13)|loan_purpose %in% c('1','3', '31', '13')) 
          #filters to only approved loans intended for refinance (for new home purchase)
          #or for home purchase (directly) - no contingency clause
          # action taken: 1: orignator file
          # action taken 6: purchaser file - hopefully limiting only to 1s should prevent 
          # overly many duplicates.
          
          
        } else{hmda %<>% mutate(loan_amount = as.numeric(loan_amount)) %>% mutate(LoanAmount = loan_amount*1000)}
        print('past first hmda transform (586)')
        # Create location identifiers to match Zillow data
        
        if('loan_purpose' %in% (hmda %>% colnames())){
          #hmda[,loan_purpose := as.numeric(loan_purpose)]
          #hmda = hmda %>% filter(loan_purpose %in% c(1,5,3))
        }
        
        hmda$county_code <- parse_number(as.character(hmda$county_code))
        
        hmda$county_code <- paste(formatC(hmda$county_code, width = 3, flag = "0"), sep = "")
        
        hmda$county_code <- as.character(hmda$county_code)
        
        if(year.id[[k]] <= 2017){
          hmda$id_number <- paste(hmda$state_code,hmda$county_code, hmda$census_tract, sep = '')
        } else if (year.id[[k]] == 2) { #deprecated code for 2017 call using non-ultimate data
          hmda$county_tract_number = as.character(hmda$census_tract_number)
          hmda$census_tract_number = sapply(hmda$census_tract_number, function(x){
            str1 = paste(c(str_pad(str_split_fixed(x,'[.]', n = 2)[1,1], width = 4, pad = '0', side = 'left'), '.'), collapse = '')
            str2 = paste(c(str_pad(str_split_fixed(x,'[.]', n = 2)[1,2], width = 2, pad = '0', side = 'right')), collapse ='')
            #if(str2 == 'NA') {
            #  str2 = '00'
            #}
            str = paste(c(str1, str2), collapse = '')
            return(str)
          })
          hmda$id_number <- paste('06',hmda$county_code, hmda$census_tract_number, sep = '')
        }  else {
          hmda$id_number = hmda$census_tract
        }
        
        ## need to drop at 2018 to make sure our data is consistent
        if(year.id[[k]] == 2018){
          #fwrite(lendername_dictionary, "lendername_dictionary_wuiafri_2022.csv")
          #lendername_dictionary = data.table()
        }
        log_print(paste0(c('we found ,',nrow(hmda), ' obs in the "hmda" file')))
        # Drop loans without identifiers
        hmda <- hmda[which(hmda$id_number != 'NA'
                           & !is.na(hmda$loan_amount)), ]
        hmda %<>% filter(id_number %in% zillow_tract$id_number)
        log_print(((hmda %>% head(15))$id_number))
        #hmda_store = hmda
        
        
        hmda = hmda %>% unique()
        print(nrow(hmda))
        
        
        #### for merge 1
        # Filter to unique transactions
        count.hmda <- setDT(hmda)[, .(n = .N), by = c('id_number', 'loan_amount')] %>% unique()
        
        
        hmda2 <- merge(hmda, count.hmda,
                       by.x = c("id_number", "loan_amount"),
                       by.y = c("id_number", "loan_amount"))
        
        #### for merge 1.5
        if(post2018){
          # Filter to unique transactions
          
          hmda[,property_value := as.numeric(property_value)]
          count.hmda <- setDT(hmda %>% filter(!is.na(property_value)))[, .(n = .N), by = c('id_number', 'loan_amount', 'property_val_merge')]
        
          
          hmda2.5 <- merge(hmda %>% filter(!is.na(property_val_merge)), count.hmda,
                       by.x = c("id_number", "loan_amount", 'property_val_merge'),
                       by.y = c("id_number", "loan_amount", 'property_val_merge'))
        
          print('past hmda2, line 995')
          rm(count.hmda)
        }
        
        hmda2 <- hmda2[which(hmda2$n == 1), ] %>% as.data.table()
        if(post2018){hmda2.5 <- hmda2.5[which(hmda2.5$n == 1), ] %>% as.data.table()}
        
        
        print(paste0(nrow(hmda2), ' number of obs possible to match in hmda data for match 1'))
        if(post2018){print(paste0(nrow(hmda2.5),  ' number of obs possible to match in hmda data for match 1.5'))}
        
        # 4) MERGES
        
        # FIRST: simple merge by id_number and LoanAmount
        zillow3_dt = zillow3 %>% as.data.table()
        zillow3_dt[,names(zillow3_dt[,-c('geometry')]) := lapply(.SD, trimws), .SDcols = names(zillow3_dt[,-c('geometry')])]
        hmda2[,names(hmda2) := lapply(.SD, trimws), .SDcols = names(hmda2)]
        hmda2$loan_amount %<>% as.numeric()
        #zillow3 = zillow3_dt
        rm(zillow3_dt)
        zillow3$LoanAmount %<>% as.numeric()
        zillow3$loan_amount %<>% as.numeric()
        zillow3 = zillow3 %>% st_drop_geometry()
        
        if(post2018){
          zillow3.5_dt = zillow3.5 %>% as.data.table()
          zillow3.5_dt[,names(zillow3.5_dt[,-c('geometry')]) := lapply(.SD, trimws), .SDcols = names(zillow3.5_dt[,-c('geometry')])]
          #zillow3.5 = zillow3.5_dt
          rm(zillow3.5_dt)
          hmda2.5[,names(hmda2.5) := lapply(.SD, trimws), .SDcols = names(hmda2.5)]
          hmda2.5$loan_amount %<>% as.numeric()
          zillow3.5$LoanAmount %<>% as.numeric()
          zillow3.5$loan_amount %<>% as.numeric()
          zillow3.5 = zillow3.5 %>% st_drop_geometry()
          
        }
        
        merge_dat_3 = zillow3 %>% group_by(County) %>% summarize(viable_fst_rnd_count = n())
        
        log_print((zillow3 %>% head(100))$loan_amount)
        log_print((hmda2 %>% head(100))$loan_amount)
        
        merge1 <- merge(zillow3, hmda2, 
                        by.x = c("id_number", "loan_amount"),
                        by.y = c("id_number", "loan_amount"), .keep_all = TRUE)
        
        merge_dat_4 = merge1 %>% group_by(County) %>% summarize(match_type_one = n())
        log_print(merge_dat_4 %>% head(5))
        print('merge_dat_4 - line(1018)')
        
        print(merge_dat_4)
        
        if(post2018){
          hmda2.5[,property_val_merge := as.numeric(property_val_merge)]
          
          
          hmda2.5 = hmda2.5[!is.na(property_val_merge)]
          zillow3.5[,property_val_merge := as.numeric(property_val_merge)]
          zillow3.5 = zillow3.5[!is.na(property_val_merge)]
          
          merge1.5 <- setDT(zillow3.5 %>% 
                              mutate(loan_amount = as.character(loan_amount)))[hmda2.5%>% 
                                                                                 mutate(loan_amount = as.character(loan_amount)), 
                                                                               on = .(id_number, loan_amount, property_val_merge), 
                                                                               roll = "nearest"]
          
          
          merge1.5$merge = 1.5
          
          merge1.5 %<>% mutate(property_value = as.numeric(property_value)) %>% mutate(valuation_diff = abs(property_value - TotalAssessedValue))
          
          merge1.5 <- merge1.5 %>% filter(valuation_diff < 10000)
          
          merge1 %<>%
            mutate(across(everything(), as.character))
          merge1.5 %<>%
            mutate(across(everything(), as.character))
          
          merge1 = rbindlist(list(merge1 %>% mutate(loan_amount = as.numeric(loan_amount)), (merge1.5 %>% mutate(loan_amount = as.numeric(loan_amount)))), fill = TRUE)
        }
        
        log_print(merge1 %>% nrow())
        
        
        lendername <- subset(merge1,select = c(LenderName, respondent_id)) %>% as.data.table() %>% mutate(activity_year = yrnum)
        fwrite(lendername, paste0('lendername_dictionary_wuiafri_2022_', yrnum,'_','ALL', '.csv'))
        
        #coord = merge1$geometry
        #merge1 %<>% st_as_sf()
        #coord = st_coordinates(merge1)
        #merge1 %<>% as.data.table() %>% dplyr::select(-geometry)
        #merge1 = cbind(merge1, coord)
        
        zillow3$loan_amount = as.numeric(zillow3$loan_amount)
        hmda2$loan_amount = hmda2$loan_amount %>% as.numeric()

        
        merge1 = merge1 %>% select_if(Negate(is.list)) %>% unique()
        rm(zillow3, hmda2)
        # SECOND: merge by respondent_id_prop, LoanAmount, and LenderName
        #rm(hmda2)
        # Create database of LenderName using respondent_id_prop
        
        lendername <- subset(merge1,select = c(LenderName, respondent_id)) %>% as.data.table() %>% mutate(activity_year = yrnum)
        
        lendername = unique(lendername)
        fwrite(merge1, paste0('merge_type_1_', yrnum, '.csv'))
        
        
        if(post2018){
          tryCatch(
            {
             ## read in lendernames from later datasets - lei is a longitudinal id, so 
             ## can capture name-lei id from multiple years
              
              lendername_dictionary = fread("/home/connor/lendername_dictionary_wuiafri_2022_2021_.csv")
              

            },
            error=function(cond) {
              message("Have you run the project a second time (may not be a true error) - could not find 2021 lendername dictionary")
              message("Here's the original error message:")
              message(cond)
              # 
              return(lendername)
            }
          )
        }
        
        log_print('right before lendername dict merge, (786)')
        lendername_dictionary = rbind(lendername, lendername_dictionary)
        lendername_dictionary = unique(as.data.frame(lendername_dictionary))
        fwrite(lendername_dictionary, paste0('lendername_dictionary_wuiafri_2022_', yrnum,'_','ALL', '.csv'))
        
        log_print('lendername_dictionary - (line 791)')
        hmda = hmda %>% unique()
        log_print('line (793)')
        lendername[!duplicated(lendername[,c('respondent_id')])]
        hmda3 = setDT(unique(hmda))[as.data.table(lendername), LenderName := i.LenderName, on = c(respondent_id = 'respondent_id')]
        #hmda3$id_number <- paste(hmda3$state_code,hmda3$county_code, hmda3$census_tract, sep = '')
        
        hmda3_count =  setDT(hmda3)[, .(n = .N), by = .(id_number,loan_amount, LenderName)] %>% filter(n == 1)
        hmda3[hmda3_count, n := i.n, on = c(id_number = 'id_number',loan_amount = 'loan_amount', LenderName = 'LenderName')]
        hmda3 = hmda3 %>% mutate(LoanAmount = as.numeric(loan_amount))
        log_print("hmda3 - with names")
        log_print(hmda3 %>% head(3))
        log_print("Right before merge zillow3_count")
        zillow_tract %<>% select(-any_of(c('geometry'))) %>% st_drop_geometry()
        log_print(head(zillow_tract))
        zillow3_count = setDT(zillow_tract)[, .(n = .N), by = .(id_number,loan_amount, LenderName)] %>% filter(n == 1) %>% unique()
        zillow3 = unique(zillow_tract)[setDT(zillow3_count%>% select(-any_of(c('geometry')))), n := i.n, on = c(id_number = 'id_number',loan_amount = 'loan_amount', LenderName = 'LenderName')] %>% filter(n == 1)
        
        log_print(zillow3 %>% head(4))
        zillow3 = zillow3 %>% select(-any_of(c("geometry")))
        zillow3 = unique(zillow3)
        hmda3 = unique(hmda3)
        log_print("Right before merge 21")
        merge21 = merge(zillow3, hmda3, 
                        by.x = c("id_number", "loan_amount", "LenderName"),
                        by.y = c("id_number", "loan_amount", "LenderName"), all.x = FALSE, all.y = FALSE)
        
        ## find how many properties match on type 2 data.
        merge_dat_5 = merge21 %>% group_by(County) %>% summarize(match_type_two = n())
        
        ## search adjacent years
        if(yrnum <= 2017){
          yrs = c(max(c((yrnum-yrdelta),2007)):min(c((yrnum+yrdelta), 2017)))
        } else{
          yrs = c(max(c((yrnum-yrdelta),2018)):min(c((yrnum+yrdelta), 2021)))
        }
        lnd_list = list()
        lnd_list = lapply(yrs, function(yr) fread(paste0('merge_type_1_', yr, '.csv')))
        log_print((lnd_list[1] %>% colnames())[102])
        log_print((lnd_list[length(lnd_list)] %>% colnames())[102])
        
        log_print('lnd_full (826)')
        
        ## this is a variation of a type two merge, and captures data matches that we've seen before in other regions
        lnd_full = lnd_list %>% rbindlist(fill = TRUE, use.names = TRUE) %>% select(any_of(c('respondent_id', 'LenderName'))) %>% unique()
        lnd_full[!duplicated(lnd_full[,respondent_id])]
        log_print(head(lnd_full, 4))
        log_print(head(hmda, 4))
        hmda3.2 = setDT(hmda)[as.data.table(lnd_full), LenderName := i.LenderName, on = c(respondent_id = 'respondent_id')]
        log_print("Right before merge 22")
        merge22 = merge(zillow3, hmda3.2,
                        by.x = c("id_number", "loan_amount", "LenderName"),
                        by.y = c("id_number", "LoanAmount", "LenderName"), all.x = FALSE, all.y = FALSE)

        fwrite(merge22, paste0('merge_type_22_2_',yrnum,'.csv'))
        
        
        #### third: merge by soundex approximate name match
        
        rm(zillow3, zillow3_count, hmda3)
        hmda45 = hmda_base #%>% filter(respondent_id %nin% c(merge1$respondent_id, merge21$respondent_id))
        
        if(year.id[[k]] <= 2017){
          hmda45$id_number <- paste(hmda45$state_code,hmda45$county_code, hmda45$census_tract, sep = '')
        } else if (year.id[[k]] == 2) {
          hmda45$census_tract_number = as.character(hmda45$census_tract_number)
          hmda45$census_tract_number = sapply(hmda45$census_tract_number, function(x){
            str1 = paste(c(str_pad(str_split_fixed(x,'[.]', n = 2)[1,1], width = 4, pad = '0', side = 'left'), '.'), collapse = '')
            str2 = paste(c(str_pad(str_split_fixed(x,'[.]', n = 2)[1,2], width = 2, pad = '0', side = 'right')), collapse ='')
            #if(str2 == 'NA') {
            #  str2 = '00'
            #}
            str = paste(c(str1, str2), collapse = '')
            return(str)
          })
          hmda45$id_number <- paste('06',hmda45$county_code, hmda45$census_tract_number, sep = '')
        }  else {
          hmda45$id_number = hmda45$census_tract
        }
        log_print("made it to line 848")
        
        ## now, soundex merge
        hmda45 %<>% mutate(respondent_id = paste0(agency_code, respondent_id))
        hmda45 = hmda45 %>% unique()
        setDT(hmda45)[as.data.table(lendername_dictionary), LenderName := i.LenderName, on = c(respondent_id = 'respondent_id')]
        hmda45 = hmda45 %>% filter(!is.na(LenderName))

        zillow45 = zillow_tract %>% filter(TransId %nin% c(merge1$TransId, merge21$TransId)) %>% filter(!(is.na(LenderName)))

        merge_dat_6 = zillow45 %>% group_by(County) %>% summarize(viable_thrd_round_count = n())
        log_print("made it to line 857")
        log_print(merge_dat_6)

        log_print("about to begin the thing that crashes memory (nope)")
        print(hmda45 %>% nrow())
        z5chunked = split(dplyr::select(zillow45, c(id_number, loan_amount, LenderName, TransId, County)), seq(nrow(zillow45)) %/% 10000)
        merge3 <- pblapply(z5chunked, FUN = function(x) {
          x_out = x %>% mutate(loan_amount = as.integer(loan_amount))%>%
            stringdist_inner_join(y =dplyr::select(hmda45 %>% mutate(loan_amount = as.integer(loan_amount)), c(id_number, loan_amount, LenderName, respondent_id)),
                                  by = c("id_number", "loan_amount", "LenderName"), max_dist = c(0,0,2),
                                  method = "soundex")

          return(x_out)
        }) %>% rbindlist()
        
        #rm(hmda,hmda2,hmda3,hmda_base)
        
        print(nrow(hmda45))
        
        hmda_chunked = split(hmda45,seq(nrow(hmda45)) %/% 5000)
        # 
        z5chunked = split(dplyr::select(zillow45, c(id_number, loan_amount, LenderName, TransId, County)), seq(nrow(zillow45)) %/% 5000)
        merge3 <- lapply(hmda_chunked, FUN = function(x) {
        x_out = stringdist_inner_join(x = x, y =dplyr::select(zillow45, c(id_number, loan_amount, LenderName, TransId, County)), by = c("id_number", "loan_amount", "LenderName"), max_dist = c(0,0,1),
                                 method = "soundex")
        
         return(x_out)
        }) %>% rbindlist()
        rm(hmda_chunked)
        # 
        merge3 = unique(zillow45)%>%stringdist_inner_join(y =unique(dplyr::select(hmda45, c(id_number, loan_amount, LenderName, respondent_id))),
                                                  by = c("id_number", "loan_amount", "LenderName"),
                                                max_dist = c(0,0,2), method = "soundex")
        merge_dat_7 = merge3 %>% group_by(County) %>% summarize(thrd_round_count = n())
        log_print('merge 3 looks like.... (line 905)')
        log_print(merge3)
        #end_merge$prod_year = yrnum
        #### Recursive merge to recover stats - since moved to an item-by-item version
        # merge_dat_out = merge(merge(merge(merge(merge(merge_dat_1, 
        
        #                                  merge_dat_2),
        #                            merge_dat_3), merge_dat_4), merge_dat_5), merge_dat_6)
        fwrite(merge_dat_1, paste0('file_1_summary_', yrnum,'_2',  '.csv'))
        fwrite(merge_dat_2, paste0('file_2_summary_', yrnum,'_2',  '.csv'))
        fwrite(merge_dat_3, paste0('file_3_summary_', yrnum,'_2',  '.csv'))
        fwrite(merge_dat_4, paste0('file_4_summary_', yrnum,'_2', '.csv'))
        fwrite(merge_dat_5, paste0('file_5_summary_', yrnum,'_2', '.csv'))
        fwrite(merge_dat_6, paste0('file_6_summary_', yrnum,'_2', '.csv'))
        fwrite(merge_dat_7, paste0('file_7_summary_', yrnum,'_',  '.csv'))
        
        fwrite(merge1, paste0('merge_type_1_', yrnum, '.csv'))
        fwrite(merge21, paste0('merge_type_2_', yrnum, '.csv'))
        fwrite(merge22, paste0('merge_type_22_', yrnum, '.csv'))
        fwrite(merge3, paste0('merge_type_3_', yrnum,'.csv'))
        
        fwrite(lendername_dictionary, paste0('lendername_dictionary_wuiafri_2022_', yrnum,'_','ALL', '.csv'))
        
        
        
        #merge_dat_out$year = yrnum
        
        #merge_fin = merge(merge_fin, merge_dat_out)
        print(paste0('Finishing year ', yrnum, ' in county ALL'))
    }
        
        
        ## remove big objects
        rm(merge1, merge21, merge3, merge_dat_1, merge_dat_2, merge_dat_3,
           merge_dat_4, merge_dat_5, merge_dat_6, hmda45, hmda_base,hmda_store, hmda3, hmda2,lendername,
           zillow3,zillow3_dt,zillowSQL,zillow.main, zdf, zillow2, zillow45)
        
        gc(reset = TRUE)
        
    }
  }

#}

######## looping by county
data(fips_codes)
if(bycounty){
  for(i in 1:length(state.id)){
    #for(fips_code in cnty_codes){
    #for (j in 1:length(year.id)){
    j =1
    lei_flag = FALSE
    
    if(year.id[[j]] %in% c(2018,2019,2020,2021)){
      lei_flag = TRUE
    }
    
    if(lei_flag){
      print('setting respondent id to be consistent with 2002-2017')
      panname = paste0('panel_', year.id[[j]])
      #hmda_pan = tbl(hmdadatabase, in_schema('hmda_public', panname)) %>% filter(id_2017 != '-1') %>% collect()
      #hmda_pan = hmda_pan %>% mutate(respondent_id_prop2 = paste0(substr(agency_code, 1, 1), str_pad(id_2017, 10, pad = '0')))
      #tsdict = hmda_pan %>% dplyr::select(respondent_id_prop2, lei)
    }
    
    ## Build multi-year lender-respondent-id dictionary

    for (k in 1:length(year.id)){
      print("YEAR and STATE are")
      print(paste(year.id[[k]]," and ", state.id[[i]]), sep = "")
      for(fips in cnty_codes_full){
        county_name = setDT(fips_codes)[county_code == substring(fips,3,6) & state_code == substring(fips,1,2)]$county %>% unique()
        cnty_codes = c(fips)
        county_name = toupper(str_remove(county_name, " County"))
        print("YEAR and STATE and COUNTY are")
        print(paste(year.id[[k]]," and ", state.id[[i]], " and ", county_name), sep = "")
        #lendername_dictionary = readRDS(lendername_dictionary, file = '/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/lendername_dict_no2019.dta')
        print(paste0('starting dictionary build for ',year.id[[k]], ' in county: ', county_name))
        yrnum = year.id[k]
        if(yrnum >= 2018){
          post2018 = TRUE
        } else{
          post2018 = FALSE
        }
        #comb = left_join(zillowSQL1_con, maintranstab, by = 'TransId')
        
        #If Statement that loanamount is not all zeros
        #zilint = left_join(maintranstab, pinfo, by = 'TransId')
        #zilint = maintranstab %>% filter(DataClassStndCode %in% c('D', 'H'))
        #zilint1 = left_join(zilint, tbl(database, 'mainAssmt'), by = 'ImportParcelID')
        #zilint1 = left_join(zilint1, tbl(database, 'bldg'), by = 'RowID')
        
        zillowSQL = zillowSQLint %>% filter(County == county_name) %>% collect() %>% 
          mutate(date = as_date(RecordingDate)) %>% mutate(date2 = as_date(RecordingDate)) %>% 
          filter(year(date) == yrnum)#date = ifelse(is.na(date), date2, date)) %>%
        print(paste0('found: ', nrow(zillowSQL), ' given the baseline data in county name.'))
        #filter(lubridate::year(date) == 2020)
        #rm(zilint, zilint1)
        #rm(test1)
        # Census tract data
        
        ### collect data on filtering down to total loan-transaction pairs
        merge_dat_1 = zillowSQL %>% group_by(County) %>% summarize(raw_count = n())
        
        gc(reset = TRUE)
        
        
        
        
        
        zillow.main <- zillowSQL[which(zillowSQL$PropertyAddressLatitude != "NA" & !is.na(zillowSQL$PropertyAddressLatitude) &
                                         ((zillowSQL$SalesPriceAmount!= "NA" & !is.na(zillowSQL$SalesPriceAmount) &
                                             zillowSQL$SalesPriceAmount>= 1000) & 
                                            (zillowSQL$LoanAmount > 0 
                                             & zillowSQL$LoanAmount != "NA" & !is.na(zillowSQL$LoanAmount)))) , ]
        print('past zillow.main (line 762)')
        rm(zillowSQL)
        ### collect data on filtering down to total loan-transaction pairs
        merge_dat_2 = zillow.main %>% group_by(County) %>% summarize(filtered_count = n())
        
        setkey(zillow.main, NULL)
        zillow.main = unique(zillow.main)      
        
        
        #rm(zillowSQL3)
        #rm(zillowSQL1)
        #rm(zillowSQL)
        
        zillow.main$year = year.id[[k]]
        
        
        
        # Zillow stuff
        
        # Drop rows that are not in the looped year j and rows that don't meet other criteria
        zillow <- zillow.main[which(zillow.main$PropertyAddressLatitude != "NA"), ]
        zillow <- zillow[which(zillow.main$PropertyAddressLongitude != "NA"), ]
        zillow = as.data.table(zillow)
        print('past zillow (line 785)')
        rm(zillow.main)
        #zillow <- distinct(zillow,ImportParcelID, SalesPriceAmount, RecordingDate, LoanAmount, TransId, .keep_all = TRUE)
        zillow2 = zillow
        print('past zillow2')
        
        # Make loan amount comparable to HMDA syntax
        if(yrnum < 2019){
          zillow2$loan_amount = round(zillow2$LoanAmount/1000)
        } else{
          
          #in 2019 and on, data for loans begins to be reported in mid 10000s increments. This means we need to
          #match loan amounts on the 5000 dollar mark that falls at the midpoint in the 10000 dollar range.
          
          # So - equal to (ceiling(loan/10k) + floor(loan/10k))/2 * 10, or (ceiling(loan/10k) + floor(loan/10k))*5
          zillow2$loan_amount = (ceiling(zillow2$LoanAmount/10000) + floor(zillow2$LoanAmount/10000))*5
          
        }
        #zillow2 = zillow2 %>% mutate(LoanAmount = loan_amount*1000)
        
        # Set up coordinates to match with Census Data
        #zdf <- as.data.table(zillow2)
        print('past zdf (line 807)')
        #@rm(zillow2)
        
        
        #tracts2000 <- california.tract
        # Get Census CRS projection
        if(yrnum %in% c(2003:2012)){
          tract = tracts2000
        }
        if(yrnum %in% c(2012:2016)){
          tract = tracts2010
        }
        if(yrnum >= 2017){
          tract = tracts2015
        }
        
        census.CRS <- st_crs(tract)
        
        zdf = st_as_sf(zillow2, coords = c("PropertyAddressLongitude", "PropertyAddressLatitude"), agr = "constant", crs = census.CRS)
        print('past zdf (line 826)')
        #proj4string(tracts1990) = CRS("+proj=longlat")
        
        # Transform data to match CRS projection from Census data
        cord.UTM <- st_transform(zdf, census.CRS)
        print('past cord.utm (line 831)')
        rm(zdf)
        # tracts1990 <- st_transform(tracts1990, census.CRS)
        
        #tracts2000 is baseline
        
        tracts2010 <- st_transform(tracts2010, census.CRS)
        
        tracts2015 = st_transform(tracts2015, census.CRS)
        
        #tracts1990$TRACTSUF[is.na(tracts1990$TRACTSUF)] <- "00"
        #for 1990 tracts. NA sub for 00
        gc(reset = TRUE)
        print('past tract transform (line 843)')
        if(year.id[k] > 2017){
          #tracts_yr = st_join(cord.UTM, tracts2015, join = st_within)
          zillow_tract <- st_join(cord.UTM, tracts2015, join = st_within)
          rm(cord.UTM)
          
          
          zillow_tract$id_number = zillow_tract$GEOID
        }
        if(year.id[k] == 2017){
          zillow_tract <- st_join(cord.UTM, tracts2015, join = st_within)
          rm(cord.UTM)
          
          zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
          zillow_tract %<>% mutate(id_number=paste0('06',zillow_tract$id_number))
          
        }
        if(year.id[k] %in% c(2012:2016)){
          zillow_tract <- st_join(cord.UTM, tracts2010, join = st_within)
          rm(cord.UTM)
          
          #zillow_tract$id_number <- paste(zillow_tract$COUNTYFP, zillow_tract$TRACTCE, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$GEOID10)
          #zillow_tract %<>% mutate(id_number=paste0('06',zillow_tract$id_number))
          
        }
        if(year.id[k] %in% c(2003:2012)){
          zillow_tract <- st_join(cord.UTM, tracts2000, join = st_within)
          rm(cord.UTM)
          zillow_tract$id_number <- paste(zillow_tract$COUNTYFP00, zillow_tract$TRACTCE00, sep = '')
          # Add decimal to make mergeable to HMDA data
          zillow_tract$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", zillow_tract$id_number)
          zillow_tract %<>% mutate(id_number=paste0('06', zillow_tract$id_number))
          
          #zillow_tract %<>% mutate(id_number=paste0('06', zillow_tract$id_number))
        }
        if(year.id[k] <= 2002){
          zillow_tract <- st_join(cord.UTM, tracts1990, join = st_within)
          zillow_tract$id_number <- paste(zillow_tract$CO, zillow_tract$TRACTBASE, '.', zillow_tract$TRACTSUF, sep = '')
        }
        #rm(cord.UTM)
        print('past tract/ztrax merge (line 876)')
        
        ## need to find those obs. that are unique on our req.
        if(yrnum > 2018){
          zillow3_filt = setDT(zillow_tract)[, .(n = .N), by = .(id_number,loan_amount, property_val_merge)]
          zillow3 = merge(zillow_tract,zillow3_filt, by =c('id_number','loan_amount', 'property_val_merge')) %>% filter(n == 1)
        } else{
          zillow3_filt = setDT(zillow_tract)[, .(n = .N), by = .(id_number,loan_amount)]
          zillow3 = merge(zillow_tract,zillow3_filt, by =c('id_number','loan_amount')) %>% filter(n == 1)
        }
        
        rm(zillow3_filt)
        
        print('past final zillow transform (line 898)')
        gc(reset = TRUE)
        
        ######################## HMDA DATA PROCESSING #######################
        
        ## generate name of table from target year
        table_name = paste('lar',as.character(yrnum), sep = '_')
        if(yrnum == 2017) { ### in 2017 hmda updates format signif. 
          hmda = tbl(hmda_database, table_name) %>% dplyr::filter(state_code == 06) %>% collect()
          hmda %<>% mutate(loan_amount = loan_amount_000s)
        } else if(yrnum < 2017){
          hmda = tbl(hmda_database, table_name) %>% dplyr::filter(state_code == '06' | state_code =='CA') %>% collect()
        } else{
          hmda = tbl(hmda_database, table_name) %>% dplyr::filter(state_code == '06' | state_code =='CA') %>% 
            filter(county_code %in% cnty_codes) %>% collect()
          hmda %<>% mutate(respondent_id = lei)
          #hmda %<>% mutate(loan_amount = as.integer(loan_amount)/1000)
          hmda %<>% filter(denial_reason_1=='10') %>% filter(loan_purpose %in% c('1','5'))
        }
        print('past first hmda read in (line 916)')
        ## store full table for future years of data
        #hmda_store = hmda
        hmda_base = hmda
        
        hmda$activity_year = year.id[[j]]
        #hmda = hmda %>% tail(-1)
        hmda = as.data.table(hmda)
        if(year.id[[k]] > 2016){
          #hmda$sequence_num = seq.int(nrow(hmda)) %>% as.character()
        }
        #hmda$sequence_num = hmda$sequence_num %>% as.character() %>% parse_number()
        
        if(year.id[[k]] %in% c(2000:2017)){
          hmda %<>% mutate(agency_code = ifelse(is.na(agency_code), '023', agency_code))
          hmda %<>% mutate(respondent_id = str_c(agency_code, respondent_id, sep = ''))
        }
        
        # Keep only home purchases and create LoanAmount column to match Zillow's
        
        if(year.id[[k]] > 2017){
          hmda$loan_amount %<>% as.numeric()
          hmda %<>% as.data.table()
          hmda[,LoanAmount := loan_amount]
          hmda[,loan_amount := round(loan_amount/1000)]
          hmda %<>% mutate(respondent_id = lei,
                           agency_code = ''
          )
          
        } else{hmda %<>% mutate(loan_amount = as.numeric(loan_amount)) %>% mutate(LoanAmount = loan_amount*1000)}
        log_print('past first hmda transform (946)')
        # Create location identifiers to match Zillow data
        
        hmda$county_code <- parse_number(as.character(hmda$county_code))
        
        hmda$county_code <- paste(formatC(hmda$county_code, width = 3, flag = "0"), sep = "")
        
        hmda$county_code <- as.character(hmda$county_code)
        
        if(year.id[[k]] < 2017){
          hmda$id_number <- paste(hmda$state_code,hmda$county_code, hmda$census_tract, sep = '')
        } else if (year.id[[k]] == 2017) {
          hmda$county_tract_number = as.character(hmda$census_tract_number)
          hmda$census_tract_number = sapply(hmda$census_tract_number, function(x){
            str1 = paste(c(str_pad(str_split_fixed(x,'[.]', n = 2)[1,1], width = 4, pad = '0', side = 'left'), '.'), collapse = '')
            str2 = paste(c(str_pad(str_split_fixed(x,'[.]', n = 2)[1,2], width = 2, pad = '0', side = 'right')), collapse ='')
            #if(str2 == 'NA') {
            #  str2 = '00'
            #}
            str = paste(c(str1, str2), collapse = '')
            return(str)
          })
          hmda$id_number <- paste('06',hmda$county_code, hmda$census_tract_number, sep = '')
        }  else {
          hmda$id_number = hmda$census_tract
        }
        
        ## need to drop at 2018 to make sure our data is consistent
        if(year.id[[k]] == 2018){
          #fwrite(lendername_dictionary, "lendername_dictionary_wuiafri_2022.csv")
          #lendername_dictionary = data.table()
        }
        
        # Drop loans without identifiers
        hmda <- hmda[which(hmda$id_number != 'NA'
                           & !is.na(hmda$LoanAmount)), ]
        hmda %<>% filter(id_number %in% zillow_tract$id_number)
        
        #hmda_store = hmda
        
        
        
        # Filter to unique transactions
        count.hmda <- setDT(hmda)[, .(n = .N), by = c('id_number', 'loan_amount')]
        
        
        hmda2 <- merge(hmda, count.hmda,
                       by.x = c("id_number", "loan_amount"),
                       by.y = c("id_number", "loan_amount"))
        print('past hmda2, line 995')
        rm(count.hmda)
        
        hmda2 <- hmda2[which(hmda2$n == 1), ] %>% as.data.table()
        
        # 4) MERGES
        
        # FIRST: simple merge by id_number and LoanAmount
        zillow3_dt[,names(zillow3_dt[,-c('geometry')]) := lapply(.SD, trimws), .SDcols = names(zillow3_dt[,-c('geometry')])]
        hmda2[,names(hmda2) := lapply(.SD, trimws), .SDcols = names(hmda2)]
        hmda2$loan_amount %<>% as.numeric()
        zillow3$LoanAmount %<>% as.numeric()
        zillow3$loan_amount %<>% as.numeric()
        
        rm(zillow3_dt)
        
        merge_dat_3 = zillow3 %>% group_by(County) %>% summarize(viable_fst_rnd_count = n())
        
        merge1 <- merge(zillow3, hmda2, 
                        by.x = c("id_number", "loan_amount"),
                        by.y = c("id_number", "loan_amount"), .keep_all = TRUE)
        
        merge1$merge = 1
        fwrite(merge1, paste0('merge_type_1_', yrnum, '.csv'))
        if(yrnum >= 2018){
          merge1.5 <- setDT(zillow3 %>% 
                            mutate(loan_amount = as.character(loan_amount)))[hmda2%>% 
                                                                                         mutate(loan_amount = as.character(loan_amount)), 
                                                                             on = .(id_number, loan_amount, property_val_match), 
                                                                             roll = "nearest"]
        
        
          merge1.5$merge = 1.5
        
          merge1 = rbindlist(list(merge1, merge1.5 %>% mutate(loan_amount = as.numeric(loan_amount))), fill = TRUE)
          fwrite(merge1, paste0('merge_type_1_', yrnum, '.csv'))
        }
        
        merge_dat_4 = merge1 %>% group_by(County) %>% summarize(match_type_one = n())
        print('merge_dat_4 - line(1018)')
        
        #coord = merge1$geometry
        merge1 %<>%  st_as_sf()
        coord = st_coordinates(merge1)
        merge1 %<>% as.data.table() %>% dplyr::select(-geometry)
        merge1 = cbind(merge1, coord)
        
        zillow3$loan_amount = as.numeric(zillow3$loan_amount)
        hmda2$loan_amount = hmda2$loan_amount %>% as.numeric()
        merge12 = merge(zillow3, hmda2, 
                        by.x = c("id_number", "loan_amount"),
                        by.y = c("id_number", "loan_amount"), .keep_all = TRUE)
        #merge1$geometry
        merge12 %<>%  st_as_sf()
        coord = st_coordinates(merge12)
        merge12 %<>% as.data.table() %>% dplyr::select(-geometry)
        merge12 = cbind(merge12, coord)
        
        merge1 = rbind(merge1,merge12, fill = TRUE) %>% unique()
        rm(zillow3, hmda2)
        # SECOND: merge by respondent_id_prop, LoanAmount, and LenderName
        #rm(hmda2)
        # Create database of LenderName using respondent_id_prop
        
        lendername <- subset(merge1,select = c(LenderName, respondent_id)) %>% as.data.table() %>% mutate(activity_year = yrnum)
        lendername_dictionary = rbind(lendername, lendername_dictionary)
        lendername_dictionary = unique(as.data.frame(lendername_dictionary))
        fwrite(lendername_dictionary, paste0('lendername_dictionary_wuiafri_2022_', yrnum,'_',county_name, '.csv'))
        
        print('lendername_dictionary - (line 1046)')
        hmda3 = setDT(hmda)[as.data.table(lendername_dictionary), LenderName := i.LenderName, on = c(respondent_id = 'respondent_id')]
        #hmda3$id_number <- paste(hmda3$state_code,hmda3$county_code, hmda3$census_tract, sep = '')
        
        hmda3_count =  setDT(hmda3)[, .(n = .N), by = .(id_number,loan_amount, LenderName)] %>% filter(n == 1)
        hmda3[hmda3_count, n := i.n, on = c(id_number = 'id_number',loan_amount = 'loan_amount', LenderName = 'LenderName')]
        hmda3 = hmda3 %>% mutate(LoanAmount = as.numeric(loan_amount))
        
        zillow3_count = setDT(zillow_tract)[, .(n = .N), by = .(id_number,loan_amount, LenderName)] %>% filter(n == 1)
        zillow3 = zillow_tract[zillow3_count, n := i.n, on = c(id_number = 'id_number',loan_amount = 'loan_amount', LenderName = 'LenderName')] %>% filter(n == 1)
        merge21 = merge(zillow3, hmda3, 
                        by.x = c("id_number", "loan_amount", "LenderName"),
                        by.y = c("id_number", "LoanAmount", "LenderName"), .keep_all = TRUE)
        
        ## find how many properties match on type 2 data.
        merge_dat_5 = merge21 %>% group_by(County) %>% summarize(match_type_two = n())
        
        ## search adjacent years
        yrs = c((yrnum-yrdelta):(yrnum+yrdelta))
        lnd_list = list()
        for(yr in yrs){
          lnd = fread(paste0('lendername_dictionary_wuiafri_2022_', yrnum,'', '.csv'))
          append(lnd_list, lnd)
        }
        lnd_full = lnd_list %>% rbindlist() %>% unique()
        hmda3.2 = setDT(hmda)[as.data.table(lnd_full), LenderName := i.LenderName, on = c(respondent_id = 'respondent_id')]
        
        merge22 = merge(zillow3, hmda3.2, 
                        by.x = c("id_number", "loan_amount", "LenderName"),
                        by.y = c("id_number", "LoanAmount", "LenderName"), .keep_all = TRUE)
        
        #### third: merge by soundex approximate name match
        
        rm(zillow3, zillow3_count, hmda3)
        hmda45 = hmda_base #%>% filter(respondent_id %nin% c(merge1$respondent_id, merge21$respondent_id))
        
        if(year.id[[k]] <= 2017){
          hmda45$id_number <- paste(hmda45$state_code,hmda45$county_code, hmda45$census_tract, sep = '')
        } else if (year.id[[k]] == 20) {
          hmda45$census_tract_number = as.character(hmda45$census_tract_number)
          hmda45$census_tract_number = sapply(hmda45$census_tract_number, function(x){
            str1 = paste(c(str_pad(str_split_fixed(x,'[.]', n = 2)[1,1], width = 4, pad = '0', side = 'left'), '.'), collapse = '')
            str2 = paste(c(str_pad(str_split_fixed(x,'[.]', n = 2)[1,2], width = 2, pad = '0', side = 'right')), collapse ='')
            #if(str2 == 'NA') {
            #  str2 = '00'
            #}
            str = paste(c(str1, str2), collapse = '')
            return(str)
          })
          hmda45$id_number <- paste('06',hmda45$county_code, hmda45$census_tract_number, sep = '')
        }  else {
          hmda45$id_number = hmda45$census_tract
        }
        
        hmda45 = hmda45 %>% unique()
        #hmda45 = hmda45 %>% mutate(respondent_id = paste0(agency_code, respondent_id))
        
        setDT(hmda45)[as.data.table(lendername_dictionary), LenderName := i.LenderName, on = c(respondent_id = 'respondent_id')]
        hmda45 = hmda45 %>% filter(!is.na(LenderName))
        zillow45 = zillow_tract %>% filter(TransId %nin% c(merge1$TransId, merge21$TransId)) %>% filter(!(is.na(LenderName)))
        
        merge_dat_6 = zillow45 %>% group_by(County) %>% summarize(viable_thrd_round_count = n())
        
        log_print("about to begin the thing that generally crashes memory")
        z5chunked = split(dplyr::select(zillow45, c(id_number, loan_amount, LenderName, TransId, County)), seq(nrow(zillow45)) %/% 5000)
        merge3 <- pblapply(z5chunked, FUN = function(x) {
         x_out = x %>% mutate(loan_amount = as.integer(loan_amount))%>%
            stringdist_inner_join(y =dplyr::select(hmda45 %>% mutate(loan_amount = as.integer(loan_amount)), c(id_number, loan_amount, LenderName, respondent_id)), by = c("id_number", "loan_amount", "LenderName"), max_dist = c(0,0,1),
                                 method = "soundex")

         return(x_out)
          }) %>% rbindlist()
        merge_dat_7 = merge3 %>% group_by(County) %>% summarize(thrd_round_count = n())
        # CONCATENATE
        # #end_merge = list(merge1, merge21, merge3) %>% rbindlist()
        #end_merge$prod_year = yrnum
        #### Recursive merge to recover stats
        # merge_dat_out = merge(merge(merge(merge(merge(merge_dat_1, 
        
        #                                  merge_dat_2),
        #                            merge_dat_3), merge_dat_4), merge_dat_5), merge_dat_6)
        fwrite(merge_dat_1, paste0('file_1_summary_2_', yrnum,'_',county_name,  '.csv'))
        fwrite(merge_dat_2, paste0('file_2_summary_2_', yrnum,'_',county_name,  '.csv'))
        fwrite(merge_dat_3, paste0('file_3_summary_2_', yrnum,'_',county_name,  '.csv'))
        fwrite(merge_dat_4, paste0('file_4_summary_2_', yrnum,'_',county_name,  '.csv'))
        fwrite(merge_dat_5, paste0('file_5_summary_2_', yrnum,'_',county_name,  '.csv'))
        fwrite(merge_dat_6, paste0('file_6_summary_2_', yrnum,'_',county_name,  '.csv'))
        fwrite(merge_dat_7, paste0('file_7_summary_2_', yrnum,'_',county_name,  '.csv'))
        
        fwrite(merge1, paste0('merge_type_1_2_', yrnum,'_',county_name, '.csv'))
        fwrite(merge21, paste0('merge_type_2_2_', yrnum,'_',county_name, '.csv'))
        fwrite(merge22, paste0('merge_type_2_2.2_', yrnum,'_',county_name, '.csv'))
        fwrite(merge3, paste0('merge_type_3_2_', yrnum,'_',county_name,'.csv'))
        
        fwrite(lendername_dictionary, paste0('lendername_dictionary_wuiafri_2022_', yrnum,'_',fips,'', '.csv'))
        
        log_print(paste0('Finishing year ', yrnum, ' in county ', county_name))
        
        
        
        ## remove big objects
        rm(merge1, merge21, merge3, merge_dat_1, merge_dat_2, merge_dat_3,
           merge_dat_4, merge_dat_5, merge_dat_6, hmda45, hmda_base,hmda_store, hmda3, hmda2,lendername,
           zillow3,zillow3_dt,zillowSQL,zillow.main, zdf, zillow2, zillow45)
        
        gc(reset = TRUE)
      
    }
  }
}
}
dbDisconnect(hmda_database)
dbDisconnect(database)