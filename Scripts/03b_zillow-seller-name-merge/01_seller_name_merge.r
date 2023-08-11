library(pacman)

p_load(readxl)

p_load(tigris,
logr,
optparse,
gmodels,
gtools,
RPostgres,
sqldf,
readr, 
dplyr,
data.table)

###
#what is the source ids we want information on, it lives ->>>
source_data_path <- "/home/connor/Desktop/rfffiles/wui-afri/hmda_ztrax_merge_330.dta"

## set up logs
logdir = paste0('log_file_name_match_', Sys.Date(), '.log')
tmp <- file.path(tempdir(), logdir)
# Open log
lf <- log_open(tmp)
start.time <- Sys.time()

logr::log_print('###############################################################')
logr::log_print('###############################################################')

logr::log_print(paste0('BEGINNING A NEW SELLERNAME MATCH RUN ########## RUN AT: ', start.time))
opt_parser = OptionParser(option_list=option_list)


# Set system time for checking performance after running

# Make the database connection for retrieving Zillow data via SQL
#zillowdbloc = rstudioapi::askForPassword('Where does your zillow database live? Please enter a full filepath.')
#This is where mine lives
#zillowdbloc = '/media/connor/T7/database_ztrax/sortingwui_ztrax'
zillowdbloc = 'wui-afri/sortingwui_ztrax'
raw_ztrax_text_file_location = '/wui-afri/ztrans/' # if necessary - fullpath lives /home/connor/Desktop/rfffiles

# other locations - 
# '/media/connor/T7/database_ztrax/sortingwui_ztrax'
#'/Volumes/T7/database_ztrax/sortingwui_ztrax'

database <- dbConnect(RSQLite::SQLite(), zillowdbloc)

## you'll notice - we don't have names tables... this means we have to work from raw text, which is not good
database %>% dbListTables()

## we need to start by building columns
layoutZTrans = read_excel('/home/connor/Desktop/Raw_data/Layout.xlsx', sheet = 2, col_types = c("text", "text","numeric", "text", "text", "text"))

## sellers and buyers are in separate tables
colnames_sellername = layoutZTrans[layoutZTrans$TableName == 'utSellerName', 'FieldName']
colnames_buyername = layoutZTrans[layoutZTrans$TableName == 'utBuyerName', 'FieldName']

## read tables - this call appears to be consistent with the less-efficient read.txt call
seller_name = fread("SellerName.txt", sep = '|', header = FALSE, stringsAsFactors = FALSE, quote = "", col.names = colnames_sellername$FieldName)
buyer_name = fread("/home/connor/Desktop/rfffiles/wui-afri/BuyerName.txt", sep = '|', header = FALSE, stringsAsFactors = FALSE, quote = "", col.names = colnames_buyername$FieldName)

## we need a source dataset to start with. Primarily, a set of names linked to transactions and past transactions. REALLY however, we just need the source IDs and the
## rest can be built from scratch
merge_source = haven::read_dta(source_data_path)
merge_source_ids = merge_source %>% dplyr::select(TransId) %>% unique()


## subset to target data, find direct matches on names. We know what names are present in our data, more or less, so we can decrease our pain by
## cutting out names not relevant to the search, at least for the direct match.
buyer_name_subset = buyer_name %>% filter(TransId %in% merge_source_ids$TransId)
seller_name_direct_match = merge(buyer_name_subset %>% filter(BuyerIndividualFullName != ""), seller_name, by.x = "BuyerIndividualFullName", by.y = "SellerIndividualFullName", all.x = TRUE, all.y = FALSE, suffixes = c("", "_origin"))


#### BEGIN MERGE TYPE 1 ####

## attach baseline comparables. Primary issue here - every single transaction is MANY-TO-MANY, and correct matches can be either, MANY-TO-ONE, ONE-TO-MANY, or also MANY-TO-MANY
seller_name_direct_match = merge(seller_name_direct_match, tbl(database, 'transMain') %>%
                                   dplyr::select(TransId,RecordingDate,DocumentDate) %>% 
                                   mutate(RecordingDate = ifelse(is.na(RecordingDate), DocumentDate, RecordingDate)), by.x = 'TransId', by.y = 'TransId', suffixes = c("", "_from_database"))

seller_name_direct_match = merge(seller_name_direct_match, tbl(database, 'propInfo') %>%
                                   dplyr::select(TransId,PropertyZip,ImportParcelID) %>% collect(), by.x = 'TransId', by.y = 'TransId', suffixes = c("", "_from_database"))

## have to remove self matches. This lowers match rate, but is critically important to the procedure as often the most recent transaction is when the buyer
## sells their own house.
seller_name_direct_match = seller_name_direct_match %>% filter(!is.na(TransId_origin))
seller_name_direct_match = seller_name_direct_match %>% filter(TransId_origin != TransId)

## we now need to loop through and get characteristics on sales of the original property. 
seller_name_direct_match = merge(seller_name_direct_match, tbl(database, 'propInfo') %>%
                                   dplyr::select(TransId,PropertyZip,PropertyAddressLatitude,PropertyAddressLongitude,ImportParcelID) %>% collect()%>% filter(!is.na(PropertyAddressLongitude)), by.x = 'TransId_origin', by.y = 'TransId', suffixes = c("", "_origin"))

seller_name_direct_match = merge(seller_name_direct_match, tbl(database, 'transMain') %>%
                                   dplyr::select(TransId,SalesPriceAmount,RecordingDate,DocumentDate) %>%
                                   mutate(RecordingDate = ifelse(is.na(RecordingDate), DocumentDate, RecordingDate))%>%  filter(!is.na(SalesPriceAmount)), by.x = 'TransId_origin', by.y = 'TransId', suffixes = c("", "_origin"))



## get data on property chars
seller_name_direct_match = merge(seller_name_direct_match, tbl(database, 'ztrax_view') %>%
                                   dplyr::select(TransId,AssessmentLandUseStndCode), by.x = 'TransId_origin', by.y = 'TransId', suffixes = c("", "_origin"))



## now we need to repeat steps above, but in reverse. Tricky part here is to ensure your initial match 

## as before, but instead of using source ids, we need to use the matched ids we found through round 1 of the process
buyer_name_origin = buyer_name %>% filter(TransId %in% unique(seller_name_direct_match$TransId_origin))

## attach to transaction data. Luckily we already have all information from assessment since it relates to original house
buyer_name_origin = merge(buyer_name_origin, tbl(database, 'transMain') %>%
                                   dplyr::select(TransId,SalesPriceAmount,RecordingDate,DocumentDate) %>%
                                   mutate(date_orig = ifelse(is.na(RecordingDate), lubridate::as_date(DocumentDate), lubridate::as_date(RecordingDate)))%>%  filter(!is.na(SalesPriceAmount)), by.x = 'TransId', by.y = 'TransId')
buyer_name_origin = merge(buyer_name_origin, tbl(database, 'propInfo') %>%
                                   dplyr::select(TransId,PropertyZip,ImportParcelID) %>% collect(), by.x = 'TransId', by.y = 'TransId')

#### COULD keep sellerind. full name, but instead can just drop and merge this way

## logic - because we are using this to create a duration rather than for an exact parcel match, we actually need to force these transactions to occur
## prior to either other recorded event. That means we need joins based on inequalities, which necessitates either a data.table op or a left_join. I went
## with dplyr solution as it reads much more 'directly'

## to keep consistent with target, we use our major seller-name product and match to it from our newly created buyer_name object.
# data path seller_name_data <- buyer_of_property_name_data

seller_name_direct_match = left_join(seller_name_direct_match %>% 
                                       mutate(RecordingDate = lubridate::as_date(RecordingDate),RecordingDate_origin = lubridate::as_date(RecordingDate_origin)), 
                                     buyer_name_origin %>% filter(BuyerIndividualFullName != "") %>% 
                                       mutate(date_orig = as_date(date_orig)), 
                                     join_by(RecordingDate > date_orig, RecordingDate_origin > date_orig, ImportParcelID_origin==ImportParcelID), suffix = c("", "_first"))


## remove sales of the house they just bought
seller_name_direct_match = seller_name_direct_match %>% filter(ImportParcelID != ImportParcelID_origin)
seller_name_direct_match = seller_name_direct_match %>% filter(TransId != TransId_origin)


## filter out any sales outside of a ~30 month window. We do this here, because it allows for Lala to scope later by date, but keeps
## data standards intact.
seller_name_direct_match %<>% mutate(date_origin_sale = lubridate::as_date(RecordingDate_origin))
seller_name_direct_match %<>% mutate(date_purchase = lubridate::as_date(RecordingDate))

seller_name_direct_match = seller_name_direct_match[ , .SD[which.min(abs(date_origin_sale - date_purchase))], by = TransId] %>%mutate(Days_Difference = abs(as.numeric(difftime(date_origin_sale, 
                                                                                                                                                           date_purchase, units = "days")))) %>% filter(Days_Difference <= 1000)
## this allows
fwrite(seller_name_direct_match,'seller_name_match_direct.csv')

# for diagnostics if something goes wrong
## seller_name_direct_match = vroom::vroom('seller_name_match_direct.csv') ## 120x speedup from fread. Usually not necessary.

direct_match_trans = seller_name_direct_match$TransId %>% unique()
buyer_name_matches = unique(seller_name_direct_match$BuyerIndividualFullName)

logr::log_print(paste0('Out of ',nrow(merge_source_ids), ' TransIds we searched for, we found ', nrow(seller_name_direct_match), ' type 1 matches'))


rm(seller_name_direct_match)


#### BEGIN MERGE TYPE 2 ####

# path is identical to type 1, except we need to construct a name field to match on

buyer_name_subset_2 = buyer_name %>% filter(TransId %in% (merge_source_ids$TransId %>% as.numeric() %>% unique())) %>% filter(TransId %nin% unique(direct_match_trans))

buyer_name_subset_2 = buyer_name_subset_2  %>% mutate(BuyerFirstLast = paste0(word(BuyerIndividualFullName, 1),' ', BuyerLastName)) %>% mutate(fst_filtervar = paste0(BuyerFirstLast, TransId)) %>% 
  dplyr::filter(BuyerIndividualFullName %nin% unique(buyer_name_matches))

## build a 'sellerfirstlast' field to match on. Hwew it's first-last
seller_name = seller_name %>% filter(SellerIndividualFullName != '') %>% dplyr::filter(SellerIndividualFullName %nin% unique(buyer_name_matches)) %>% 
  mutate(SellerFirstLast = paste0(word(SellerIndividualFullName, 1),' ', SellerLastName)) %>% tidyr::unite(fst_filtervar, c("SellerFirstLast", "TransId"), remove = FALSE) %>% filter(!(fst_filtervar %in% buyer_name_subset_2$fst_filtervar %>% unique()))


## from here down, it's mostly the same logic as above, except a few more catches to avoid duplication
seller_name_match = merge(buyer_name_subset_2 %>% filter(BuyerIndividualFullName != ""), seller_name, by.x = "BuyerFirstLast", by.y = "SellerFirstLast", all.x = TRUE, all.y = FALSE, suffixes = c("", "_origin"), allow.cartesian = TRUE)

seller_name_match = seller_name_match %>% filter(!is.na(TransId_origin))
seller_name_match = seller_name_match %>% filter(TransId_origin != TransId)
seller_name_match = seller_name_match %>% distinct(BuyerFirstLast, TransId, .keep_all = TRUE)


seller_name_match = merge(seller_name_match, tbl(database, 'transMain') %>%
                            dplyr::select(TransId,RecordingDate,DocumentDate) %>% 
                            mutate(RecordingDate = ifelse(is.na(RecordingDate), DocumentDate, RecordingDate)),by.x = 'TransId', by.y = 'TransId', suffixes = c("", "_from_database"))

seller_name_match = merge(seller_name_match, tbl(database, 'propInfo') %>%
                                   dplyr::select(TransId,PropertyZip,PropertyAddressLatitude,PropertyAddressLongitude,ImportParcelID) %>% collect()%>% filter(!is.na(PropertyAddressLongitude)), 
                          by.x = 'TransId', by.y = 'TransId', suffixes = c("", "_from_database"))



seller_name_match = merge(seller_name_match, tbl(database, 'propInfo') %>%
                            dplyr::select(TransId,PropertyZip,PropertyAddressLatitude,PropertyAddressLongitude,ImportParcelID) %>% collect()%>% filter(!is.na(PropertyAddressLongitude)), 
                          by.x = 'TransId_origin', by.y = 'TransId', suffixes = c("", "_origin"))
seller_name_match = merge(seller_name_match, tbl(database, 'transMain') %>%
                            dplyr::select(TransId,SalesPriceAmount,RecordingDate,DocumentDate)  %>% 
                            mutate(RecordingDate = ifelse(is.na(RecordingDate), DocumentDate, RecordingDate))%>% filter(!is.na(SalesPriceAmount)), 
                          by.x = 'TransId_origin', by.y = 'TransId', suffixes = c("", "_origin"))

###
buyer_name_origin = buyer_name %>% filter(TransId %in% unique(seller_name_match$TransId_origin)) %>% distinct(TransId, BuyerIndividualFullName, .keep_all = TRUE)
buyer_name_origin = merge(buyer_name_origin, tbl(database, 'transMain') %>%
                            dplyr::select(TransId,SalesPriceAmount,RecordingDate,DocumentDate) %>%
                            mutate(date_orig = ifelse(is.na(RecordingDate), lubridate::as_date(DocumentDate), lubridate::as_date(RecordingDate)))%>%  filter(!is.na(SalesPriceAmount)), by.x = 'TransId', by.y = 'TransId')
buyer_name_origin = merge(buyer_name_origin, tbl(database, 'propInfo') %>%
                            dplyr::select(TransId,PropertyZip,ImportParcelID) %>% collect(), by.x = 'TransId', by.y = 'TransId')

#### COULD keep sellerind. full name, but instead can just drop and merge this way
seller_name_match = left_join(seller_name_match %>% mutate(RecordingDate = lubridate::as_date(RecordingDate),RecordingDate_origin = lubridate::as_date(RecordingDate_origin)), 
                                     buyer_name_origin %>% filter(BuyerIndividualFullName != "") %>% mutate(date_orig = as_date(date_orig)), 
                                     join_by(RecordingDate > date_orig, RecordingDate_origin > date_orig, ImportParcelID_origin==ImportParcelID), suffix = c("", "_first"))%>% distinct(TransId_first,TransId,TransId_origin, .keep_all = TRUE)

## remove sales of the house they just bought
seller_name_match = seller_name_match %>% filter(ImportParcelID != ImportParcelID_origin)

seller_name_match %<>% mutate(date_origin_sale = lubridate::as_date(RecordingDate_origin))
seller_name_match %<>% mutate(date_purchase = lubridate::as_date(RecordingDate))

seller_name_match = seller_name_match[ , .SD[which.min(abs(date_origin_sale - date_purchase))], by = TransId] %>%mutate(daydelta = as.numeric(difftime(date_origin_sale, 
                                                                                                                                                      date_purchase, units = "days")), Days_Difference = abs(as.numeric(difftime(date_origin_sale, 
                                                                                                                                                                                date_purchase, units = "days")))) %>% filter(Days_Difference <= 1000 & Days_Difference != 0)

fwrite(seller_name_match, 'seller_name_match_indirect.csv')
# seller_name_match = vroom::vroom('seller_name_match_indirect.csv') for checking
slr_transid1 = seller_name_match$TransId
logr::log_print('##### MATCH 2 #####')

logr::log_print('We see that there is this much overlap in match two with one:')
logr::log_print(paste0('There are, ',sum(slr_transid1 %in% direct_match_trans), ' TransIds in both sets'))
logr::log_print('For our data (match 2), we see that there are this many identified transactions that occurred on the same day we see a purchase (likely errors):')
logr::log_print(paste0('There are, ',sum(seller_name_match$Days_Difference == 0), ' TransIds in both sets'))
logr::log_print(paste0('Out of ',nrow(merge_source_ids), ' TransIds we searched for, we found ', nrow(seller_name_match), ' type 2 matches'))


rm(seller_name_match)

#### BEGIN MERGE TYPE 3 ####

# path is identical to type 1/2, except we need to construct a name field to match on. Stick with firstlast format for ease of later merging.

buyer_name_subset_2 = buyer_name %>% filter(TransId %in% (merge_source_ids$TransId %>% as.numeric() %>% unique())) %>% filter(TransId %nin% direct_match_trans)%>% filter(TransId %nin% slr_transid1)
buyer_name_subset_2 = buyer_name_subset_2  %>% mutate(BuyerFirstLast = paste0(substr(word(BuyerIndividualFullName, 1), 1, 1),' ' ,str_sub(word(BuyerIndividualFullName, 2), start=1, end=1),' ', BuyerLastName))
seller_name_match = seller_name %>% filter(SellerIndividualFullName != '') %>% filter(SellerIndividualFullName %nin% buyer_name_matches) %>% 
  mutate(SellerFirstLast = paste0(str_sub(word(SellerIndividualFullName, 1), start=1, end=1),' ',str_sub(word(SellerIndividualFullName, 2), start=1, end=1),' ', SellerLastName))

rm(seller_name, buyer_name) ## can sometimes save you in memory overflows

seller_name_match = merge(buyer_name_subset_2 %>% filter(BuyerIndividualFullName != ""), seller_name_match, by.x = "BuyerFirstLast", by.y = "SellerFirstLast", all.x = TRUE, all.y = FALSE, suffixes = c("", "_origin"), allow.cartesian = TRUE)

seller_name_match = seller_name_match %>% filter(!is.na(TransId_origin))
seller_name_match = seller_name_match %>% filter(TransId_origin != TransId)
seller_name_match = seller_name_match %>% distinct(BuyerFirstLast, TransId, .keep_all = TRUE)


seller_name_match = merge(seller_name_match, tbl(database, 'transMain') %>%
                            dplyr::select(TransId,RecordingDate,DocumentDate) %>%mutate(RecordingDate = ifelse(is.na(RecordingDate), DocumentDate, RecordingDate)) %>% collect(), by.x = 'TransId', by.y = 'TransId', suffixes = c("", "_from_database"))

seller_name_match = merge(seller_name_match, tbl(database, 'propInfo') %>%
                            dplyr::select(TransId,PropertyZip,ImportParcelID) %>% collect(), by.x = 'TransId', by.y = 'TransId', suffixes = c("", "_from_database"))

seller_name_match = seller_name_match %>% filter(!is.na(TransId_origin))
seller_name_match = merge(seller_name_match, tbl(database, 'propInfo') %>%
                            dplyr::select(TransId,PropertyZip,PropertyAddressLatitude,PropertyAddressLongitude,ImportParcelID) %>% collect()%>% filter(!is.na(PropertyAddressLongitude)), by.x = 'TransId_origin', by.y = 'TransId', suffixes = c("", "_origin"))

seller_name_match = merge(seller_name_match, tbl(database, 'transMain') %>%
                            dplyr::select(TransId,SalesPriceAmount,RecordingDate,DocumentDate) %>% filter(!is.na(SalesPriceAmount)) %>% collect(), by.x = 'TransId_origin', by.y = 'TransId', suffixes = c("", "_origin"))



###
buyer_name_origin = buyer_name %>% filter(TransId %in% unique(seller_name_match$TransId_origin))
buyer_name_origin = merge(buyer_name_origin, tbl(database, 'transMain') %>%
                            dplyr::select(TransId,SalesPriceAmount,RecordingDate,DocumentDate) %>%
                            mutate(date_orig = ifelse(is.na(RecordingDate), lubridate::as_date(DocumentDate), lubridate::as_date(RecordingDate)))%>%  filter(!is.na(SalesPriceAmount)), by.x = 'TransId', by.y = 'TransId')
buyer_name_origin = merge(buyer_name_origin, tbl(database, 'propInfo') %>%
                            dplyr::select(TransId,PropertyZip,ImportParcelID) %>% collect(), by.x = 'TransId', by.y = 'TransId')
## notice - reversed by.x and by.y here. Deliberate, to match onto seller name dataset.

#### COULD keep sellerind. full name, but instead can just drop and merge this way
seller_name_match = left_join(seller_name_match %>% mutate(RecordingDate = lubridate::as_date(RecordingDate),RecordingDate_origin = lubridate::as_date(RecordingDate_origin)), 
                              buyer_name_origin %>% filter(BuyerIndividualFullName != "") %>% mutate(date_orig = as_date(date_orig)), 
                              join_by(RecordingDate > date_orig, RecordingDate_origin > date_orig, ImportParcelID_origin==ImportParcelID), suffix = c("", "_first")) %>%  distinct(TransId, TransId_origin, TransId_first, .keep_all = TRUE)

## remove sales of the house they just bought
seller_name_match = seller_name_match %>% filter(ImportParcelID != ImportParcelID_origin)

seller_name_match %<>% mutate(date_origin_sale = lubridate::as_date(RecordingDate_origin))
seller_name_match %<>% mutate(date_purchase = lubridate::as_date(RecordingDate))

seller_name_match = seller_name_match[ , .SD[which.min(abs(date_origin_sale - date_purchase))], by = TransId] %>%mutate(daydelta = as.numeric(difftime(date_origin_sale, 
                                                                                                                                            date_purchase, units = "days")), Days_Difference = abs(as.numeric(difftime(date_origin_sale, 
                                                                                                                                                                  date_purchase, units = "days")))) %>% filter(Days_Difference <= 1000)


## remove sales of the house they just bought
seller_name_match = seller_name_match %>% filter(ImportParcelID != ImportParcelID_origin & TransId != TransId_origin)

seller_name_match %<>% mutate(date_origin_sale = lubridate::as_date(RecordingDate_origin))
seller_name_match %<>% mutate(date_purchase = lubridate::as_date(RecordingDate))

seller_name_match = setDT(seller_name_match)[ , .SD[which.min(abs(date_origin_sale - date_purchase))], by = TransId] %>%mutate(Days_Difference = abs(as.numeric(difftime(date_origin_sale, 
                                                                                                                                                                  date_purchase, units = "days")))) %>% filter(Days_Difference <= 1000)
seller_name_match = seller_name_match %>% filter(ImportParcelID != ImportParcelID_origin)
seller_name_match = seller_name_match %>% filter(TransId != TransId_origin)


fwrite(seller_name_match, 'seller_name_match_indirect2.csv')

## logs for checks
logr::log_print('##### MATCH 3 #####')
logr::log_print('We see that there is this much overlap in match three with two:')
logr::log_print(paste0('There are, ',sum(seller_name_match$TransId %in% slr_transid1), ' TransIds occurring in both sets'))
logr::log_print('We see that there is this much overlap in match three with one:')
logr::log_print(paste0('There are, ',sum(seller_name_match$TransId %in% direct_match_trans), ' TransIds occurring in both sets'))
logr::log_print('For our data (match 3), we see that there are this many identified transactions that occurred on the same day we see a purchase (likely errors):')
logr::log_print(paste0('There are, ',sum(seller_name_match$Days_Difference == 0), ' TransIds in both sets'))
logr::log_print(paste0('Out of ',nrow(merge_source_ids), ' TransIds we searched for, we found ', nrow(seller_name_match), 'type 3 matches'))
logr::log_print('-------------------------------------------------------------------------')
end.time <- Sys.time()
logr::log_print(paste0('ENDING A NEW SELLERNAME MATCH RUN ########## RUN AT: ', end.time))

