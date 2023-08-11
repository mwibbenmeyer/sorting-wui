### match to original dataframe

library(pacman)

p_load(sf, data.table, tidyverse, haven, tigris)

zillowdbloc = '/home/connor/Desktop/rfffiles/wui-afri/sortingwui_ztrax'
# '/media/connor/T7/database_ztrax/sortingwui_ztrax'
#'/Volumes/T7/database_ztrax/sortingwui_ztrax'
database <- dbConnect(RSQLite::SQLite(), zillowdbloc)
database %>% dbListTables()


tracts = tigris::tracts(state = "CA")

merge_source = haven::read_dta("/home/connor/Desktop/rfffiles/wui-afri/hmda_ztrax_merge_330.dta")

## narrow down product data
columns_of_interest = c("TransId","TransId_origin", "TransId_first", "ImportParcelID", "PropertyZip",
                        "SalesPriceAmount","date_purchase", 
                        "ImportParcelID_origin", "PropertyZip_origin", "BuyerIndividualFullName", "SellerNonIndividualName", "BuyerIndividualFullName_first",
                        "date_origin_sale", "date_orig", "SalesPriceAmount_first",'daydelta','Days_Difference') ### change date_orig to date_origin_buy and date_purchase to date_buy

## name matches - 3 types, 1 is of the most direct matches, followed by 2 and 3
name_match_1 = (fread('seller_name_match_direct.csv') %>% mutate(daydelta = as.numeric(difftime(date_origin_sale, 
                                                                                                          date_purchase, units = "days"))))[,columns_of_interest, with = F] %>% mutate(name_match_confidence_rank= 1)
name_match_2 = fread('seller_name_match_indirect.csv')[,columns_of_interest, with = F] %>% mutate(name_match_confidence_rank = 2)
name_match_3 = (fread('seller_name_match_indirect2.csv') %>% mutate(daydelta = as.numeric(difftime(date_origin_sale, 
                                                                                                  date_purchase, units = "days"))))[,columns_of_interest, with = F] %>% mutate(name_match_confidence_rank = 3)
## create unified product, drop any duplicated transactions, prioritizing higher quality ones
name_match = rbindlist(list(name_match_1,name_match_2, name_match_3)) %>% dplyr::distinct(TransId, .keep_all = TRUE)


## explicitly get data from tables on origin homes, origin buy/sales
name_match = left_join(name_match, tbl(database, 'propInfo') %>%
                         dplyr::select(TransId,PropertyAddressLatitude,PropertyAddressLongitude,ImportParcelID)%>% filter(!is.na(PropertyAddressLongitude))  %>% collect()%>% unique(), join_by(ImportParcelID == ImportParcelID, TransId == TransId), suffix = c('', '_from_database'))

name_match = left_join(name_match, tbl(database, 'propInfo') %>%
                             dplyr::select(TransId,PropertyAddressLatitude,PropertyAddressLongitude,ImportParcelID)%>% filter(!is.na(PropertyAddressLongitude)) %>%collect()%>% unique(), join_by(ImportParcelID_origin == ImportParcelID, TransId_origin == TransId), suffix = c('', '_origin'))
name_match = left_join(name_match, tbl(database, 'transMain') %>%
                            dplyr::select(TransId,SalesPriceAmount) %>% collect()%>% unique(), join_by(TransId_origin == TransId), suffix = c('', '_origin'))
name_match = left_join(name_match, tbl(database, 'ztrax_view') %>%
                         dplyr::select(TransId,AssessmentLandUseStndCode,ImportParcelID) %>% collect()%>% unique(), join_by(TransId == TransId, ImportParcelID == ImportParcelID), suffix = c('', '_from_database'))
name_match = left_join(name_match, tbl(database, 'ztrax_view') %>%
                         dplyr::select(TransId,AssessmentLandUseStndCode,ImportParcelID) %>% collect()%>% unique(), join_by(TransId_origin == TransId,ImportParcelID_origin == ImportParcelID), suffix = c('', '_origin'))

name_match = name_match %>% mutate(date_origin_buy = date_orig, date_buy = date_purchase)

## add lat/lons for _origin, as well as prop use stnd. code $AssessmentLandUseStndCode
## add a column that says importparcelid in/not in original dataset.

## find tract each purchase belongs in
name_match = name_match %>% st_as_sf(coords = c("PropertyAddressLongitude_origin", "PropertyAddressLatitude_origin"), agr = "constant", crs = 4267, remove = FALSE)
name_match = st_join(name_match %>% st_transform(st_crs(tracts)), tracts %>% mutate(census_tract_origin = GEOID) %>% select(census_tract_origin), st_within)


## reattach all original data
merge_source_out = merge(merge_source, name_match %>% st_drop_geometry(), by.x = 'TransId',by.y = 'TransId', all.x = T, all.y = F, suffix = c("", "_from_database"))%>% st_drop_geometry()
merge_source_out = merge_source_out %>% mutate(new_origin_property_flag = !(ImportParcelID_origin %in% unique(merge_source_out$ImportParcelID))) %>% mutate(new_origin_property_flag = ifelse(is.na(ImportParcelID_origin), FALSE, new_origin_property_flag))

## write data object to disk
merge_source_out %>% fwrite('raw_name_match_data.csv')
## move to new object to prevent overly destructive behavior
merge_source_out2 =merge_source_out %>% arrange(name_match_confidence_rank)%>% dplyr::distinct(TransId, .keep_all = TRUE) 
cols_out = c('TransId_origin', 'TransId_first','ImportParcelID_origin',"date_origin_buy","date_origin_sale","date_buy",
             "PropertyZip_origin","SalesPriceAmount_origin","SalesPriceAmount_first","AssessmentLandUseStndCode_origin",
             "BuyerIndividualFullName","BuyerIndividualFullName_first", "SellerNonIndividualName",
             "name_match_confidence_rank",'daydelta','Day_Difference',"new_origin_property_flag","PropertyAddressLatitude_origin","PropertyAddressLongitude_origin",colnames(merge_source))

## filter down to only relevant columns
merge_source_out2 = merge_source_out2 %>% distinct(TransId) %>% select(any_of(cols_out))
colnames(merge_source_out2) = merge_source_out2 %>% colnames() ## %>% tolower() if we want to set to lower

## write to file
merge_source_out2 %>% haven::write_dta("/home/connor/Desktop/rfffiles/wui-afri/hmda_ztrax_merge_330_withnames_clean.dta")
