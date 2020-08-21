#---------------------------------------------------------------------------------------------------
# WORKING DIRECTORY
#---------------------------------------------------------------------------------------------------

#setwd("C:/HMDA_R_Files/")
wd <- "C:/HMDA_R_Files/"

#---------------------------------------------------------------------------------------------------
# PREAMBLE (PACKAGES NEEDED TO RUN SCRIPT)
#---------------------------------------------------------------------------------------------------

devtools::install_github('walkerke/tigris')
library(gmodels)
library(readr)
library(RODBC)
library(tidyverse)
library(ggmap)
library(DT)
library(knitr)
library(rgdal)
library(raster)
library(phonics)
library(UScensus2000tract)
library(tmap)
library(XML)
library(sf)
library(sp)
library(rgeos)
library(spatialEco)
library(tigris)
library(magrittr)
library(rgdal)
library(maptools)
#library(plyr)
library(dplyr)
library(lubridate)
library(base)
library(jsonlite)
library(httr)
library(magrittr)
library(ggplot2)
library(phonics)
library(data.table)
library(gtools)
#---------------------------------------------------------------------------------------------------
# LOOP SETUP
#---------------------------------------------------------------------------------------------------

# Set system time for checking performance after running
  start.time <- Sys.time()

# Make the database connection for retrieving Zillow data via SQL
  database <- odbcConnect("sql_test")
  
  
  
  
  
  
  # HMDA data
  
  
  hmda2 <- readRDS("C:/HMDA_Raw/intermediate_merges/hmda_groups_1_to_2.rds")
  hmda3 <- readRDS("C:/HMDA_Raw/intermediate_merges/hmda_group_3.rds")
  hmda3 <-hmda3[ c("action_taken_name", "agency_code")] 
  hmda4 <- readRDS("C:/HMDA_Raw/intermediate_merges/hmda_group_4.rds")
  hmda4 <-hmda4[ c( "as_of_year")] 
  
  hmda5 <- readRDS("C:/HMDA_Raw/intermediate_merges/hmda_group_5.rds")
  hmda5 <-hmda5[ c("denial_reason_1")] 
  
  #hmda6 <- readRDS("C:/HMDA_Raw/intermediate_merges/hmda_group_6.rds")
  
  
  hmda7 <- readRDS("C:/HMDA_Raw/intermediate_merges/hmda_group_7.rds")
  hmda7 <- hmda7[c("lien_status" , "loan_purpose")]
  #hmda8 <- readRDS("C:/HMDA_Raw/intermediate_merges/hmda_group_8.rds")
  #hmda9 <- readRDS("C:/HMDA_Raw/intermediate_merges/hmda_group_9.rds")
  hmda10 <- readRDS("C:/HMDA_Raw/intermediate_merges/hmda_group_10.rds")
  hmda10 <-hmda10[ c("state_abbr")] 
  hmda_final <- cbind(hmda2 , hmda3, hmda4, hmda5, hmda7, hmda10)
  rm(hmda2)
  rm(hmda3)
  rm(hmda4)
  rm(hmda5)
  rm(hmda7)
  rm(hmda10)
  write.csv(hmda_final, file = "hmda_final.csv")
  save(hmda_final, file = "C:/HMDA_R_Files/hmda_final.RData")
  
  
  
#FULL HMDA DATA
 
  #HMDA 2007-2016
  load("C:/HMDA_R_Files/hmda_final.RData")
  hmda_final07_16 <- as.data.frame(hmda_final)
  hmda_final07_16$rate_spread <- hmda_final07_16$loan_purpose_name <- hmda_final07_16$action_taken_name <- NULL
  
  #HMDA 1995-2001
  load("C:/HMDA_Raw/hmda_final1995_2001.RData")
  hmda_final95_01 <- as.data.frame(hmda_final)
  hmda_final95_01$rate_spread <- hmda_final95_01$loan_purpose_name <- hmda_final95_01$action_taken_name <- NULL
  
  #HMDA 2002-2006
  ##bad for some reason
  hmda_final <- read_dta("C:/Users/billinsb/Dropbox/BBGL_Sealevels/Data/HMDA/hmda2002_2006.dta") 
  
  #load("C:/HMDA_Raw/hmda_final2002_2006.RData")
  hmda_final02_06 <- as.data.frame(hmda_final)
  hmda_final02_06$rate_spread <- hmda_final02_06$loan_purpose_name <- hmda_final02_06$action_taken_name <- NULL
  
  state_code <- hmda_final95_01[c("state_code", "state_abbr") ]
  state_code <- distinct(state_code)
  hmda_final02_06 <- merge(hmda_final02_06, state_code, by.x = c("state_code"),
                           by.y = c("state_code") , allow.cartesian = FALSE, all.x = TRUE)
  
  ##need state code abbreviations
  
  rm(hmda_final)
  
  
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
  memory.limit(size=5120000000)
  library(haven)

  
  
  state.id <- c("AL", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA",  "ID",
              "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN" , "MS", "MO",
            "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", 
         "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY") 
  
 
  #MD, MN has no lendername.
 
 
 
  year.id <- c(1995:2016)
  

  # Define list to store elements of final data.table
  list <- vector("list", length(state.id)*length(year.id))
  
  #---------------------------------------------------------------------------------------------------
  # LOOP
  #---------------------------------------------------------------------------------------------------


  tigris_cache_dir("C:/HMDA_R_Files/")
  readRenviron('~/.Renviron')
  options(tigris_use_cache = TRUE)
  #options(tigris_class = "sf" )
  library(sqldf)
  #options(tigris_use_cache = FALSE)
  
  for (i in 1:length(state.id)) {
    
      tracts2000 <- tracts(state.id[[i]], county = NULL, cb = TRUE, year = 2000, refresh = TRUE)
    
     tracts1990 <- tracts(state.id[[i]], county = NULL, cb = TRUE, year = 1990, refresh = TRUE)
    
    tracts2010 <- tracts(state.id[[i]], county = NULL, cb = TRUE, year = 2010, refresh = TRUE)
    
    
   tracts1990$TRACTCE90 <- paste(tracts1990$TRACTBASE,tracts1990$TRACTSUF , sep="")

    for (j in 1:length(year.id)) {
      
      print("YEAR.ID and STATE.ID are")
      print(paste(year.id[[j]]," and ", state.id[[i]]), sep = "")
      
      # Pull in data needed to run code

      # Zillow Transaction data (two parts, plus a merge)
      zillowSQL1 <- (sqlQuery(database, paste("
                                              SELECT 
                                              TransId, FIPS, RecordingDate, OccupancyStatusStndCode, LenderName, 
                                              SalesPriceAmount, LoanAmount, InitialInterestRate
                                              FROM dbo.transMain
                                              WHERE RecordingDate >= '", year.id[j], "-01-01'", "
                                              AND RecordingDate <= '", year.id[j], "-12-31' 
                                              AND State = ", "'", state.id[i], "'", 
                                              sep = "")))
      

      if(dim(zillowSQL1)[1] > 100 ){

        #If Statement that loanamount is not all zeros
        test1 <- zillowSQL1[which(zillowSQL1$LoanAmount > 0 
                                & zillowSQL1$LoanAmount != "NA"), ]
         if(dim(test1)[1] > 100){ 
      
      zillowSQL3 <- (sqlQuery(database, paste("
                                              SELECT PropertyAddressLatitude,PropertyAddressLongitude, ImportParcelID, TransId
                                              FROM dbo.transPropertyInfo
                                              WHERE PropertyState = ", "'", state.id[i], "'", 
                                              sep = "")))

      rm(test1)
      # Census tract data

      

      
      
      #test33 <- na.omit(tracts1990$TRACTSUF)

      
      zillowSQL <- merge(zillowSQL1, zillowSQL3, by.x = c("TransId"),
                         by.y = c("TransId") , allow.cartesian = FALSE, all.x = TRUE)
      
      
      zillow.main <- zillowSQL[which(zillowSQL$PropertyAddressLatitude != "NA" & 
                                       ((zillowSQL$SalesPriceAmount!= "NA" &
                                           zillowSQL$SalesPriceAmount!= 0) | 
                                          (zillowSQL$LoanAmount > 0 
                                           & zillowSQL$LoanAmount != "NA"))) , ]
      

      
      rm(zillowSQL3)
      rm(zillowSQL1)
      rm(zillowSQL)

      zillow.main$year <-   year.id[[j]]

      
      
      # Zillow stuff
      
      # Drop rows that are not in the looped year j and rows that don't meet other criteria
      zillow <- zillow.main[which(zillow.main$PropertyAddressLatitude != "NA"), ]
      zillow <- zillow.main[which(zillow.main$PropertyAddressLongitude != "NA"), ]
      zillow <- distinct(zillow, 
                         ImportParcelID, SalesPriceAmount, RecordingDate, LoanAmount, 
                         .keep_all = TRUE)
      
      # Keep non-negative loan amounts and loan amouts that exist and properties that have lat/long
      zillow2 <- zillow[which(zillow$LoanAmount > 0 
                              & zillow$LoanAmount != "NA"), ]
      

      zillow2 <- distinct(zillow2, 
                          ImportParcelID, SalesPriceAmount, RecordingDate, LoanAmount, 
                          .keep_all = TRUE)
      
      # Make loan amount comparable to HMDA syntax
      zillow2$LoanAmount = round(zillow2$LoanAmount, digits=-3)
      
      # Set up coordinates to match with Census Data
      id <- as.data.frame(zillow2)
      
      
      coords <- as.data.frame(cbind(zillow2$PropertyAddressLongitude, zillow2$PropertyAddressLatitude ))
      cord.dec = SpatialPointsDataFrame(coords , 
                                        id ,proj4string = CRS("+proj=longlat"))
      
#tracts2000 <- california.tract
      # Get Census CRS projection
      census.CRS <- proj4string(tracts2000)
      
      #proj4string(tracts1990) = CRS("+proj=longlat")
      
      # Transform data to match CRS projection from Census data
      cord.UTM <- spTransform(cord.dec, census.CRS)
      
      
      tracts1990 <- spTransform(tracts1990, census.CRS)
      
      tracts2010 <- spTransform(tracts2010, census.CRS)
      
     #tracts1990$TRACTSUF[is.na(tracts1990$TRACTSUF)] <- "00"
      #for 1990 tracts. NA sub for 00
      if(year.id[j] > 2011){
        pts.poly <- over(cord.UTM, tracts2010[3:4]) 
        
        pts.poly2 <- cbind(id, pts.poly)
        
        pts.poly2$id_number <- paste(pts.poly2$COUNTY, pts.poly2$TRACT, sep = '')
      }
      else{
        if(year.id[j] > 2002){
          
          pts.poly <- over(cord.UTM, tracts2000[4:5]) 
          
          pts.poly2 <- cbind(id, pts.poly)
          
          pts.poly2$id_number <- paste(pts.poly2$COUNTY, pts.poly2$TRACT, sep = '')
          #pts.poly2$id_number <- paste(pts.poly2$county, pts.poly2$tract, sep = '')
        } 
        
        else{
          
          pts.poly <- over(cord.UTM, tracts1990[8:10]) 
          
          pts.poly2 <- cbind(id, pts.poly)
          
          #pts.poly2$id_number <- paste(pts.poly2$COUNTYFP, pts.poly2$TRACTCE90, sep = '')
          
          #just CA
          pts.poly2$id_number <- paste(pts.poly2$COUNTYFP, pts.poly2$TRACTCE90, sep = '')
          
          
        }
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
      pts.poly2$id_number <- sub("([[:digit:]]{2,2})$", ".\\1", pts.poly2$id_number)
      
      # Keep just necessary variabes
      zillow2 <- pts.poly2
      
      # Filter to unique transactions
      count.zillow <- count_(zillow2, vars = c("id_number", "LoanAmount"))
      
      zillow3 <- merge(zillow2, count.zillow, 
                       by.x = c("id_number", "LoanAmount"),
                       by.y = c("id_number", "LoanAmount"))
      rm(count.zillow)
      
      zillow3 <- zillow3[which(zillow3$n == 1), ]
      
      # HMDA Stuff

      
      if(year.id[j] > 2006){
      
      hmda <- hmda_final07_16[which(hmda_final07_16$as_of_year == year.id[j] 
                               & hmda_final07_16$state_abbr == state.id[i] ) , ] 
      
      } else if(year.id[j] > 2001){
        hmda <- hmda_final02_06[which(hmda_final02_06$as_of_year == year.id[j] 
                                      & hmda_final02_06$state_abbr == state.id[i] ) , ] 
      }  else{
      
        hmda <- hmda_final95_01[which(hmda_final95_01$as_of_year == year.id[j] 
                                      & hmda_final95_01$state_abbr == state.id[i] ) , ] 
        
      }
    
      
      # Keep only home purchases and convert loan amount to match Zillow
      hmda <- hmda[which(hmda$loan_purpose == 1
                         & hmda$action_taken == 1), ]
      hmda$loan_purpose_name <- hmda$action_taken <- NULL
      hmda$as_of_year  <- hmda$denial_reason_1 <- NULL
      
      hmda$LoanAmount <- hmda$loan_amount_000s * 1000
      
      # Create location identifiers to match Zillow data
      hmda$county_code <- as.numeric(hmda$county_code)
      
      hmda$county_code <- paste(formatC(hmda$county_code, width = 3, flag = "0"), sep = "")
      
      hmda$county_code <- as.character(hmda$county_code)
      
      hmda$id_number <- paste(hmda$county_code, hmda$census_tract_number, sep = '')
      
      # Drop loans without identifiers
      hmda <- hmda[which(hmda$id_number != 'NA'
                         & hmda$LoanAmount != 'NA'), ]
      
      
      
      
      # Filter to unique transactions
      count.hmda <- count_(hmda, vars = c("id_number", "LoanAmount"))
      
      
      hmda2 <- merge(hmda, count.hmda,
                     by.x = c("id_number", "LoanAmount"),
                     by.y = c("id_number", "LoanAmount"))
      
      rm(count.hmda)
      
      hmda2 <- hmda2[which(hmda2$n == 1), ]
      
      
      
      # 4) MERGES
      
      # FIRST: simple merge by id_number and LoanAmount
      
      merge1 <- merge(zillow3, hmda2, 
                      by.x = c("id_number", "LoanAmount"),
                      by.y = c("id_number", "LoanAmount"))
      
      # SECOND: merge by respondent_id, LoanAmount, and LenderName
      rm(hmda2)
      # Create database of LenderName using respondent_id
      lendername <- subset(merge1, select = c(LenderName, respondent_id))
      
      lendername <- distinct(lendername, .keep_all = TRUE)
      
      hmda3a <- inner_join(hmda, lendername, 
                           by.x = "respondent_id",
                           by.y = "respondent_id")
      
      # The merge by id_number, LoanAmount, and LenderName
      hmda3 <- hmda3a[which(hmda3a$id_number != 'NA' & 
                              hmda3a$LoanAmount != 'NA' &
                              hmda3a$LenderName != "NA"), ]
      
      if(dim(hmda3)[1] > 100 ){
        

      count.hmda.merge2 <- count_(hmda3, 
                                 vars = c("id_number", "LoanAmount", "LenderName"))
      
      hmda3 <- merge(hmda3, count.hmda.merge2,
                     by.x = c("id_number", "LoanAmount", "LenderName"),
                     by.y = c("id_number", "LoanAmount", "LenderName"))
      
      
      rm(count.hmda.merge2)
      
      hmda3 <- hmda3[which(hmda3$n == 1), ]
      
      count.zillow.merge2 <- count_(zillow2, 
                                   vars =  c("id_number", "LoanAmount", "LenderName"))
      
      zillow4 <- merge(zillow2, count.zillow.merge2,
                       by.x = c("id_number", "LoanAmount", "LenderName"),
                       by.y = c("id_number", "LoanAmount", "LenderName"))
      
      zillow4 <- zillow4[which(zillow4$n == 1), ]
      
      merge2 <- merge(zillow4, hmda3,
                      by.x = c("id_number", "LoanAmount", "LenderName"),
                      by.y = c("id_number", "LoanAmount", "LenderName"))
      
      # THIRD: same as last but with "fuzzy" names
      
      # Get rid of all observations without lender names
      hmda3 <-hmda3[which(hmda3$id_number != 'NA' 
                          & hmda3$LoanAmount != 'NA' 
                          & hmda3$LenderName != "NA"), ]
      
      # Get "fuzzy" names for HMDA and Zillow
      hmda3a$LenderName2 <- soundex(hmda3a$LenderName, maxCodeLen = 4L)
      
      hmda3a$LenderName <- NULL
      
      zillow2$LenderName2 <- soundex(zillow2$LenderName, maxCodeLen = 4L)
      
      zillow2$LenderName <- NULL
      
      # Do the other stuff
      count.hmda.merge3 <- count_(hmda3a, 
                                 vars =  c("id_number", "LoanAmount", "LenderName2"))
      
      hmda4 <- merge(hmda3a, count.hmda.merge3, 
                     by.x = c("id_number", "LoanAmount", "LenderName2"),
                     by.y = c("id_number", "LoanAmount", "LenderName2"))
      
      rm(hmda3a)
      rm(hmda3)
      
      rm(count.hmda.merge3)
      
      hmda4 <- hmda4[which(hmda4$n == 1), ]
      
      count.zillow.merge3 <- count_(zillow2, 
                                   vars = c("id_number", "LoanAmount", "LenderName2"))
      
      zillow5 <- merge(zillow2, count.zillow.merge3,
                       by.x = c("id_number", "LoanAmount", "LenderName2"),
                       by.y = c("id_number", "LoanAmount", "LenderName2"))
      
      zillow5 <- zillow5[which(zillow5$n == 1), ]
      
      merge3 <- merge(zillow5, hmda4,
                      by.x = c("id_number", "LoanAmount", "LenderName2"),
                      by.y = c("id_number", "LoanAmount", "LenderName2"))
      
      merge3$LenderName <- merge3$LenderName2
      
      merge3$LenderName2 <- NULL
      
      merge3$freq.y <- merge3$freq.x <- merge2$freq.y <- merge2$freq.x <- merge1$freq.y <- merge1$freq.x <- NULL
      
      # 5) The final merge
      
      merge1$match <- 1
      merge2$match <- 2
      merge3$match <- 3
      
      final.merge <- bind_rows(merge1, merge2, merge3)
      }
      else{
        
        final.merge <- merge1
      }
      
      
      rm(count.zillow)
      rm(count.zillow.merge2)
      rm(count.zillow.merge3)
      
      rm(hmda4)
      rm(hmda)
      rm(pts.poly)
      rm(pts.poly2)
      rm(merge1)
      rm(merge2)
      rm(merge3)
      rm(zillow3)
      rm(zillow4)
      rm(zillow5)
      
      # Filter out repeated values
      final.merge <- distinct(final.merge, 
                              ImportParcelID, TransId, SalesPriceAmount, 
                              RecordingDate, LoanAmount, .keep_all = TRUE)
      
      final.merge <- as.data.table(final.merge)
      final.merge$COUNTYFP10 <- final.merge$FIPS <- final.merge$id_number <- NULL
      final.merge$action_taken_name <- final.merge$TRACTCE00 <- final.merge$STATEFP00 <- NULL
      # Match to pre-defined list to merge all states and years into one data table
      #list[[i]][[j]] <- final.merge
      
      
      # NEED TO FIGURE OUT WHY THIS DATASET IS NOT RECORDING STATE
      numbtrans <- sum(with(zillow, year.id[j] == year))
      numbtransloans <- sum(with(zillow2, year.id[j] == year))
      numbtransloansmatch <- sum(with(final.merge, year.id[j] == year))
      
      mergestats_all_hmda2 <- data.frame(
        state = c(state.id[[i]]),
        year = c(year.id[[j]]),
        numb_trans = c(numbtrans),
        numb_trans_loans = c(numbtransloans ),
        numb_trans_loans_matched = c(numbtransloansmatch )
        
      )
      
      mergestats_all_hmda <- rbind(mergestats_all_hmda, mergestats_all_hmda2)
      
      
      
      final <- smartbind(final, final.merge )
      #list <- do.call("rbind", list)
      #final <- dplyr::bind_rows(list)
      final$STATEFP00 <- final$COUNTYFP00 <- final$TRACTCE00  <- NULL
      
      final$STATEFP <- final$COUNTYFP <- final$TRACTCE90  <- NULL
      final$COUNTY <- final$TRACT <- final$n.x <- final$n.y <- NULL
      }
      }
    else {
      
    }
      
        mergestats_hmda <- data.table(mergestats_all_hmda)
  write.dta(mergestats_hmda, "C:/HMDA_R_Files/mergestats_hmda.dta")
  final <- data.frame(final,stringsAsFactors = FALSE)
  #final$RecordingDate2 = as.character(final$RecordingDate, "%Y/%m/%d")
  final$rate_spread <- final$freq.x <-  final$freq.y <- NULL
  write_dta(final, "C:/HMDA_R_Files/final_hmda.dta")  
      
    
    }
  }

  
  
  
  


