
# Preamble

library(pacman)
p_load(here, readxl, chunkR, tidyverse, broom, future, inline, pbmcapply, data.table)

#-----------------------------------------
## Cfcn to read lines of a file before it hits R

linecount_code <- "
uintmax_t linect = 0; 
uintmax_t tlinect = 0;

int fd, len;
u_char *p;

struct statfs fsb;

static off_t buf_size = SMALL_BUF_SIZE;
static u_char small_buf[SMALL_BUF_SIZE];
static u_char *buf = small_buf;

PROTECT(f = AS_CHARACTER(f));

if ((fd = open(CHAR(STRING_ELT(f, 0)), O_RDONLY, 0)) >= 0) {

  if (fstatfs(fd, &fsb)) {
    fsb.f_iosize = SMALL_BUF_SIZE;
  }

  if (fsb.f_iosize != buf_size) {
    if (buf != small_buf) {
      free(buf);
    }
    if (fsb.f_iosize == SMALL_BUF_SIZE || !(buf = malloc(fsb.f_iosize))) {
      buf = small_buf;
      buf_size = SMALL_BUF_SIZE;
    } else {
      buf_size = fsb.f_iosize;
    }
  }

  while ((len = read(fd, buf, buf_size))) {

    if (len == -1) {
      (void)close(fd);
      break;
    }

    for (p = buf; len--; ++p)
      if (*p == '\\n')
        ++linect;
  }

  tlinect += linect;

  (void)close(fd);

}
SEXP result;
PROTECT(result = NEW_INTEGER(1));
INTEGER(result)[0] = tlinect;
UNPROTECT(2);
return(result);
";

setCMethod("wc",
           signature(f="character"), 
           linecount_code,
           includes=c("#include <stdlib.h>", 
                      "#include <stdio.h>",
                      "#include <sys/param.h>",
                      "#include <sys/mount.h>",
                      "#include <sys/stat.h>",
                      "#include <ctype.h>",
                      "#include <err.h>",
                      "#include <errno.h>",
                      "#include <fcntl.h>",
                      "#include <locale.h>",
                      "#include <stdint.h>",
                      "#include <string.h>",
                      "#include <unistd.h>",
                      "#include <wchar.h>",
                      "#include <wctype.h>",
                      "#define SMALL_BUF_SIZE (1024 * 8)"),
           language="C",
           convention=".Call")

numtrans = wc(here("Raw_data/zillow/ZTrans/Main.txt"))

## informational printout
print(paste0(paste0("There are: ", numtrans), " lines of data in transaction data"))
#-----------------------------------------

plan(multiprocess, workers = availableCores() - 1)

## Boolean to attach square footage data or not
buildingareasind = FALSE

options(scipen = 999) # Do not print scientific notation
options(stringsAsFactors = FALSE) ## Do not load strings as factors

# Change directory to where you've stored ZTRAX
dir <- here("/Raw_data/zillow")


#  Pull in layout information
layoutZAsmt <- read_excel(file.path(dir, 'layout.xlsx'), sheet = 1)
layoutZTrans <- read_excel(file.path(dir, 'layout.xlsx'), 
                           sheet = 2,
                           col_types = c("text", "text", "numeric", "text", "text", "text"))


######################################################################
###  Create nonhistoric assessment data
#    Need 3 tables
#    1) Main table in assessor database
#    2) Building table


col_namesMain <- layoutZAsmt[layoutZAsmt$TableName == 'utMain', 'FieldName']
col_namesBldg <- layoutZAsmt[layoutZAsmt$TableName == 'utBuilding', 'FieldName']
col_namesBldgA <- layoutZAsmt[layoutZAsmt$TableName == 'utBuildingAreas', 'FieldName']
col_namesPropInfo <- layoutZAsmt[layoutZAsmt$TableName == 'utAdditionalPropertyAddress', 'FieldName']

#-----------------------------------------
#ZTrans Important tables
col_namesPropTr <- layoutZTrans[layoutZTrans$ TableName == 'utPropertyInfo', 'FieldName']
col_namesMainTr <- layoutZTrans[layoutZTrans$ TableName == 'utMain', 'FieldName']

######################################################################

## Strategy overview - in general, vector memory (even expanded) can't handle the full extent of this dataset, so procedurally

### Build streamlined assessment data with location, value, variety of timing implements

#-----------------------------------------

## chunkR into chunks for transaction data, limit to timing and those fields required to ensure each transaction is unique, plus loan amount and name of lender. 

## For now, we'll keep non-AL transactions.

### Use assessment data above to attach relevant (modern) assessment data to our transaction. In practice, we'll want different datasets for each annual assessment -
### This mimics the HMDA dataset style.

basecolsint = c(1,2,5,6,20:30,32,33,37,39,40, 52 , 80:84, 86, 87, 89)

### Print columns used?
print_cols_used <- readline(prompt="Print Columns used? (y/n)")

if(print_cols_used == "y") print(col_namesMain[basecolsint,]$FieldName)

base_dt = fread(here("Raw_data/zillow/Zasmt/Main.txt"),
              select= basecolsint,
              sep = '|',
              header = FALSE,
              stringsAsFactors = FALSE,             
              quote = "",
              col.names = t(col_namesMain[basecolsint,]$FieldName)
)
                                          

print(paste0("There are no duplicate assessment records: " ,length(unique(base_dt[,ImportParcelID])) == dim(base_dt)[1]))                         
              
          if( length(unique(base$ImportParcelID)) != dim(base_dt)[1] ){

            #Example: Print all entries for parcels with at least two records.
             base[ImportParcelID %in% base[duplicated(ImportParcelID), ImportParcelID], ][order(ImportParcelID)]
            
             setkeyv(base, c("ImportParcelID", "LoadID"))  # Sets the index and also orders by ImportParcelID, then LoadID increasing
             keepRows <- base[ ,.I[.N], by = c("ImportParcelID")]   # Creates a table where the 1st column is ImportParcelID and the second column 
                                                       # gives the row number of the last row that ImportParcelID appears.
             base <- base[keepRows[[2]], ] # Keeps only those rows identified in previous step
  
           }

#-----------------------------------------
#### Property table

# For efficiency, I am going to limit what is read in rather than read in the entire tables. This means to add columns, we need to identify their integer identifier

bldg_colint = c(1, 2, 3, 6, 15, 16, 17, 18:27, 30:31, 36, 41)

if(print_cols_used == "y") print(col_namesBldg[bldg_colint,]$FieldName)

bldg = fread(here('Raw_data/zillow/Zasmt/Building.txt'),
  select = bldg_colint,
   sep = '|',
   header = FALSE,
   stringsAsFactors = FALSE,             
   quote = "",
  col.names = t(col_namesBldg[bldg_colint,]$FieldName))


#  Reduce bldg dataset to Single-Family Residence, Condo's, Co-opts (or similar)

bldg <- bldg[PropertyLandUseStndCode %in% c('RR101',  # SFR
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
             ]



#-----------------------------------------

             ### PROPERTY ADDRESS GEO-INFO

# Was initially going to load extra property address info - now it only makes sense to really load geo-checkers to be sure our data is good. I'll leave this for a future
# step. At the moment, only CBSA really adds anything

## TODO: load geo-chekcer columns here. Stored in utAdditionalPropertyAdress.

             # Important: PropertyAddressDPV (able to receive delivery)
             #            PropertyGeocodeQualityCode
             #            PropertyAddressQualityCode
             #            Most of these are in main - this would only be to check for consistency

######################################################################
#### Load building squarefoot data

             ## In script so I have a reference - in practice, I am going to keep this out.




if(buildingareasind){

sqft <- read.table(file.path(dir, "ZAsmt/BuildingAreas.txt"),                   
                   sep = '|',
                   header = FALSE,
                   stringsAsFactors = FALSE,             
                   skipNul = TRUE,                            
                   comment.char="",                           
                   quote = "",                                
                   col.names = t(col_namesBldgA)
)


sqft <- as.data.table(sqft)

# Counties report different breakdowns of building square footage and/or call similar concepts by different names.
# The structure of this table is to keep all entries reported by the county as they are given. See 'Bldg Area' table in documentation.
# The goal of this code is to determine the total square footage of each property. 
# We assume a simple logic to apply across all counties here. Different logic may be as or more valid.
# The logic which generates square footage reported on our sites is more complex, sometimes county specific, and often influenced by user interaction and update. 

sqft <- sqft[BuildingAreaStndCode %in% c('BAL',  # Building Area Living
                                         'BAF',  # Building Area Finished
                                         'BAE',  # Effective Building Area
                                         'BAG',  # Gross Building Area
                                         'BAJ',  # Building Area Adjusted
                                         'BAT',  # Building Area Total
                                         'BLF'), # Building Area Finished Living
             ]

table(sqft$BuildingOrImprovementNumber)  # BuildingOrImprovementNumber > 1  refers to additional buildings on the parcel. 

sqft <- sqft[ , list(sqfeet = max(BuildingAreaSqFt, na.rm = T)), by = c("RowID", "BuildingOrImprovementNumber")]

## end of sqrft conditional statement
}

###############################################################################
#   Merge previous three datasets together to form attribute table

attr <- merge(base_dt, bldg, by = "RowID")
#attr <- merge(attr, vals, by = "RowID")
#attr <- merge(attr, AddtlPropAddress, by = "RowID")

#If needed for sqft -
if(buildingareasind){
attr <- merge(attr, sqft, by = c("RowID", "BuildingOrImprovementNumber"))
}


###############################################################################
###############################################################################
#  Load transaction dataset.
#     Need two tables
#      1) PropertyInfo table provided ImportParcelID to match transaction to assessor data loaded above
#      2) Main table in Ztrans database provides information on real estate events

col_namesProp <- layoutZTrans[layoutZTrans$TableName == 'utPropertyInfo', 'FieldName']
col_namesMainTr <- layoutZTrans[layoutZTrans$TableName == 'utMain', 'FieldName']

# Through painful trial and error, this data is too large for memory (even a single column). Instead, we will use an outside method with a c-based peeper function that
# counts lines in external files

###############################################################################
#   Load PropertyInfo table for later merge

col_namesMainTrInt = c(1:4, 7,19:20, 25)
col_namesPropTrInt = c(1,65, 45, 47)

#######################################################################################
#  Load main table in Ztrans database, which provides information on real estate events

counties = c("SAN DIEGO","ORANGE")

longway = F

if(longway){
print("Don't worry - this part takes quite a while. Get some coffee :)")


countypicker <- function(data, pos) subset(data, County %in% counties)
filtereddata = read_delim_chunked(here("Raw_data/zillow/ZTrans/Main.txt"), 
  delim ='|', 
  col_names = t(col_namesMainTr),
  DataFrameCallback$new(countypicker),
  chunk_size = 10000)
}

trdata=NULL
nrows = 10000

chunkerfast = function(skipnumber, datapath = here("Raw_data/zillow/ZTrans/Main.txt"), 
  numtransactions = numtrans, 
  numrows = 10000, 
  varlist = counties,
  filtermain = T){

  if(filtermain){
   filtereddata=fread(datapath,
    sep = '|',
    header = FALSE,
    stringsAsFactors = FALSE, 
    nrow=numrows, 
    select = col_namesMainTrInt,
    col.names = col_namesMainTr[col_namesMainTrInt,]$FieldName,
    skip = skipnumber)
    out = filtereddata[County == varlist[1] |County == varlist[2]]
    return(out)
} else{
    filtereddata=fread(datapath,
    sep = '|',
    header = FALSE,
    stringsAsFactors = FALSE, 
    nrow=numrows, 
    select = col_namesPropTrInt,
    col.names = col_namesProp[col_namesPropTrInt,]$FieldName,
    skip = skipnumber)
    out = filtereddata
    return(out)
}
}

#set of chunk startpoints to search the data.table for counties
`%nin%` = Negate(`%in%`)
chunks = c(1:10)
endpoint = numtrans - (numtrans %% 10000)
for(ch in chunks){
  
  if(ch == 10){
      chunklength = numtrans %/% (length(chunks))
      remainder = numtrans %% (length(chunks))
  }
  else{
      chunklength = numtrans %/% (length(chunks))
      remainder = 0
  }
  

  skipnumbers = seq(chunklength*(ch-1), 
    (chunklength+remainder)*ch, 
    by = 10000)

  outof = paste0(ch, " of 10")
  print(paste0(
    "This part takes quite some time. Go get some coffee for a minute. WARNING: Default uses all cores but 1, so other operations may run slowly. Starting chunk ", outof))

  #pbmclapply is simply an extension to mclapply except it adds an ETA tracker which is useful for something this time intensive.
  transdata = pbmclapply(skipnumbers, chunkerfast, mc.cores = (availableCores() - 1), mc.preschedule = F) %>% rbindlist()
  
  #DO NOT REMOVE mc.preschedule = F. Without it, the loop will run and then error out at the end. See...

  # https://stackoverflow.com/questions/18330274/an-error-in-one-job-contaminates-others-with-mclapply

  # for an explanation. This means any data generation, to be reproducible, must be done outside of this loop.

  # Drop transactions of multiple parcels (transIDs associated with PropertySequenceNumber > 1)


  proptransdata = pbmclapply(skipnumbers, chunkerfast, filtermain = F, mc.cores = (availableCores() - 1), mc.preschedule = F) %>% rbindlist()
  
  dropTrans <- unique(proptransdata[PropertySequenceNumber > 1, TransId])

  proptransdata <- proptransdata[(TransId %nin% dropTrans), ]
  transdata <- transdata[(TransId %nin% dropTrans), ] 

  out = merge(transdata, proptransdata, by = "TransId")
  filename = paste0("chunk", ch, ".csv")
  filepath = paste0("Raw_data/zillow/chunk", filename)
  write_csv(out, filepath)
  rm(transdata, proptransdata, out)
  }

# Keep only one record for each TransID. 
# TransID is the unique identifier of a transaction. 
# Multiple entries for the same TransID are due to updated records.
# The most recent record is identified by the greatest LoadID. 

setkeyv(trans, c("TransId", "LoadID"))
keepRows <- trans[ ,.I[.N], by = "TransId"]
trans <- trans[keepRows[[2]], ]
trans[ , LoadID:= NULL]

#Keep only events which are deed transfers (excludes mortgage records, foreclosures, etc. See documentation.)

trans <- trans[DataClassStndCode %in% c('D', 'H'), ]

###############################################################################
#   Merge previous two datasets together to form transaction table

transComplete <- merge(propTrans, trans, by = "TransId")
