#-----------------------------------------
# Script to process data - extract extra columns from zillow and hmda
# To include - racial/income/demo characteristics of borrower
#			   geographic lat-long (zillow sourced)
# 			   factor associated with fire risk (wfhz) -- extracted using point value in wfhz, in 2016 and 2018
#				Also include - wui designation
#							 - % land cover (lc) in 1km buffer zone around home
#							 - point elevation of property
#			   census tract/block (hmda)
#			   date of certification (zillow)
#-----------------------------------------

# REQ - prerun that has produced the hmda database, the zillow database and the 
#		collected identifier dataset (.Rds file)

#PREAMBLE
#-----------------------------------------

#install.packages('pacman')

library(pacman)

#install.packages('tigris')

p_load(gmodels,
	raster,
	tidyverse,
  gtools,
  RPostgres,
  sqldf,
  RODBC,
  DT,
  rgdal,
  UScensus2000tract,
  sf,
  sp,
  rgeos,
  spatialEco,
  tigris,
  dbplyr,
  data.table,
  here,
  stringi,
  elevatr,
  haven,
  rgdal,
  gdalUtils)

# Dataload & Pruning
#-----------------------------------------

  zillowdbloc <- '/Volumes/G-DRIVE-mobile-SSD-R-Series/ZTrans/sorting_wui.sqlite'
  ztrax_database <- dbConnect(RSQLite::SQLite(), zillowdbloc)
  hmda_database <- dbConnect(Postgres(), host = 'localhost', dbname = 'hmda', 
    password = '', user = 'postgres')
  
  ca_blocks = tigris::block_groups(state = 'CA')

  #to maintain same-race-categories across years - restrict to 2005-2017 sales
  yrs = c(2005:2016)
  tblist = paste(rep('lar', each = length(yrs)), yrs, sep = '_')

  out_dat = readRDS('/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/intermediate/final_hmdazil.dta') %>% 
  select(ImportParcelID, TransId, respondent_id_prop, sequence_num, 
 	InitialInterestRate, RecordingDate, loan_amount, SalesPriceAmount, 
 	PropertyLandUseStndCode, year)

  #grab spatial data in R
  #Wui read-in
  print('!!!!!!!!!!!!!!!!!BIG WARNING SECTION!!!!!!!!!!!!!!!!!!!!!!!!!!')
  #wui = ogr2ogr("/Volumes/G-DRIVE-mobile-SSD-R-Series/ca_wui_cp12.gdb", "wui.shp", "ca_wui_cp12", nlt = "MULTIPOLYGON")
  wuiogr = readOGR('wui.shp')

  #Wui convert
  wui_sf = st_as_sf(wuiogr)


  #wildfire potential files - 2012 and 2018
  wfp2012 = raster("/Volumes/G-DRIVE-mobile-SSD-R-Series/RDS-2015-0045/Data/wfp_2012_continuous/wfp2012_cnt")
  wfp2018 =raster("/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/Raw_data/WFP_Layers/RDS-2015-0047-2/Data/whp_2018_continuous/whp2018_cnt")
  #NLCD2006 = raster('/Volumes/G-DRIVE-mobile-SSD-R-Series/ZTrans/NLCD_2006_Land_Cover_L48_20190424/NLCD_2006_Land_Cover_L48_20190424.img')

  out_df = data.table()

  prop_type_select = c('RR101',  # SFR
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
  'RR120')
  #Loop to find relevant variables from HMDA and Zillow and return year-wise data
  #-----------------------------------------
  for(yr in yrs){
  print(paste0('Starting data for year ', yr))
  out_dat_tmp = out_dat %>% filter(year == yr)
  
  print(paste0('found ', nrow(out_dat_tmp), ' in year ', yr))
  if(yr < 2009){
    padcharacter = ' '
  } else{
    padcharacter = '0'
  }
  
  print('loading hmda database')
  tablename = paste('lar', yr, sep =  '_')
  hmda_data = tbl(hmda_database,  in_schema('hmda_public',tablename))
  
  #grab IDs for matching
  hmda_id = out_dat_tmp %>% select(sequence_num, respondent_id_prop, TransId, ImportParcelID) %>% 
    mutate(sequence_num = str_pad(as.character(sequence_num), 7, padcharacter, side = 'left'))
  
  ztrax_id = out_dat_tmp %>% select(sequence_num, respondent_id_prop,ImportParcelID, 
                                    PropertyLandUseStndCode, SalesPriceAmount, loan_amount, RecordingDate, 
                                    InitialInterestRate, TransId) %>% mutate(sequence_num = str_pad(as.character(sequence_num), 7, padcharacter, side = 'left'))
  
  hmda_data %<>% mutate(agency_code = ifelse(is.na(agency_code), '023', agency_code))%>% 
    mutate(respondent_id_prop = str_c(agency_code, respondent_id, sep = ''))
  
  #merge to main hmda result
  print(paste0('beginning merge with hmda database for year ', yr))
  hmda_out = merge(hmda_id, hmda_data %>% select(respondent_id_prop, sequence_num, applicant_race_1, 
                                                 income, applicant_ethnicity, census_tract, county_code, state_code,msa, rate_spread, applicant_sex), 
                   by = c('sequence_num', 'respondent_id_prop'))
  
  print(paste0('hmda_out found ', nrow(hmda_out), ' in year ', yr))
  
  #merge to transaction result - no useful data in this table...yet
  #ztrax_out1 = merge(ztrax_id %>% select(TransId), tbl(ztrax_database, "largetransaction") %>% select(TransId,County) %>% unique(), by = c('TransId'))
  
  #merge to property characteristic result
  print('beginning z-trax merge')
  
  ztrax_out2 = merge(ztrax_id %>% distinct(), tbl(ztrax_database, 'AssmtJoined') %>% 
                       select(ImportParcelID, PropertyLandUseStndCode, PropertyAddressLatitude, PropertyAddressLongitude, YearBuilt, BuildingAreaSqFt, PropertyCity, 
                              TotalBedrooms, TotalCalculatedBathCount)%>% filter(PropertyLandUseStndCode %in% prop_type_select) %>% distinct(), 
                     by = c('ImportParcelID', 'PropertyLandUseStndCode')) %>% distinct(sequence_num, respondent_id_prop, .keep_all = T)
  
  print(paste0('ztrax_out2 found ', nrow(ztrax_out2), ' in year ', yr)) 
  
  print('merging ztrax and hmda')
  
  #print(paste0('ztrax_hmda found after joining to ORIGINAL join: ', nrow(ztrax_hmda), ' in year ', yr)) 
  
  ztrax_hmda = merge(ztrax_out2, hmda_out,
                     by = c('sequence_num', 'respondent_id_prop', 'ImportParcelID', 'TransId')) %>% distinct(sequence_num, respondent_id_prop, .keep_all = T)
  print(paste0('ztrax_hmda found ', nrow(ztrax_hmda), ' in year ', yr)) 
  
  print(paste0('Found ', nrow(ztrax_hmda), ' matches in the HMDA/Ztrax dataset (post merge)'))
  
  print('converting to sf')
  ztrax_hmda_sf = st_as_sf(ztrax_hmda%>% filter(!is.na(PropertyAddressLongitude)& !is.na(PropertyAddressLatitude)), 
                           coords = c("PropertyAddressLongitude", "PropertyAddressLatitude"), 
                           crs = 4269, agr = "constant") %>% unique()
  
  #set crs to match ztrax
  print('extracting spatial results')
  
  #extract wildfire risk - 2018 raster (continuous)
  ztrax_hmda_sf$WFPC_risk2018 = raster::extract(wfp2018, ztrax_hmda_sf)
  
  #extract wildfire risk - 2012 raster (continuous)
  ztrax_hmda_sf$WFPC_risk2012 = raster::extract(wfp2012, ztrax_hmda_sf)
  
  print('extracting elevation')
  ztrax_hmda_sf$elevation = get_elev_point(ztrax_hmda_sf %>% select(geometry), src = c("aws")) #200 minutes to gather
  wui_sf1 = st_transform(wui_sf, st_crs(ztrax_hmda_sf))
  ca_blocks = st_transform(ca_blocks, st_crs(ztrax_hmda_sf))
  
  ztrax_hmda_sf = st_join(ztrax_hmda_sf, wui_sf1, join = st_within)
  ztrax_hmda_sf = st_join(ztrax_hmda_sf, ca_blocks, join = st_within)
  
  ztrax_hmda_sf %<>% mutate(wuiflag00 = ifelse(!grepl(WUICLASS00, pattern = 'NoVeg', fixed = TRUE), 1, 0), 
                            wuiflag10 = ifelse(!grepl(WUICLASS10, pattern = 'NoVeg', fixed = TRUE),1,0)) %>% select_if(!grepl(names(.), pattern = '2010')|!grepl(names(.), pattern = '2000')|!grepl(names(.), pattern = '1990'))
  
  #find percentage of different land cover in 2006 at each homesite
  print('producing land cover kilometer raster -- skipped, takes too long')
  
  #ztrax_lc_dat = raster::extract(NLCD2006, ztrax_hmda_sf, buffer = 200)
  #tmptab1 = lapply(ztrax_lc_dat, FUN = function(x) table(x)/(table(x) %>% sum()))
  #renamelambda = function(x, named){names(named[[x]]) = paste0('lc',names(named[[x]]))
  #	return(named[[x]])
  #}
  #tmptab2 = lapply(1:length(tmptab1), FUN = renamelambda, named = tmptab1)
  #tmptab3 <- rbindlist(lapply(tmptab2, function(x) as.data.frame.list(x)), fill=TRUE)
  #tmptab3[is.na(tmptab3),] = 0
  
  #extract point elevation
  
  
  #bind land cover information to ztrax
  print('rbind then start new year')
  out_df = rbindlist(list(out_df, ztrax_hmda_sf %>% select(-geometry) %>% data.table()))


}

out_df = out_df %>% select(-(elevation.elev_units:BLKGRPCE)) %>% select(-(NAMELSAD:INTPTLON)) %>% 
select(-geometry) %>% mutate(elevation = elevation.elevation, census_tract_blocks = GEOID) %>% 
select(-elevation.elevation, -GEOID)

fwrite(out_df, 'ztrax_hmda_data_CA.csv')
write_dta(out_df %>% data.frame(), 'ztrax_hmda_data_CA_2.dta')








