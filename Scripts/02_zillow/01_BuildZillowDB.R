#-----------------------------------------
#File to build main zillow and miscellaneous other data database

#Built using SQLite, dbplyr, and tidyverse functions to keep things simple.

#-----------------------------------------
#packages
#REQUIRES: R 4+
#Make sure you have zillow layout data properly installed in the directory of the folder you are using to store ZAsmt and ZTrans folders
#install.packages('pacman')
library(pacman)

p_load(DBI, dbplyr, RSQLite, tidyverse, here, inline)

`%nin%` = Negate(`%in%`)

#-----------------------------------------
#read in column names for transaction data, assessors data

layoutZAsmt <- read_excel(here('layout.xlsx'), sheet = 1)
layoutZTrans <- read_excel(here('layout.xlsx'), 
                           sheet = 2,
                           col_types = c("text", "text", "numeric", "text", "text", "text"))

#Assessors tables of interest
col_namesMain <- layoutZAsmt[layoutZAsmt$TableName == 'utMain', 'FieldName']
col_namesBldg <- layoutZAsmt[layoutZAsmt$TableName == 'utBuilding', 'FieldName']
col_namesBldgA <- layoutZAsmt[layoutZAsmt$TableName == 'utBuildingAreas', 'FieldName']
col_namesPropInfo <- layoutZAsmt[layoutZAsmt$TableName == 'utAdditionalPropertyAddress', 'FieldName']


#Transaction Important tables
col_namesPropTr <- layoutZTrans[layoutZTrans$TableName == 'utPropertyInfo', 'FieldName']
col_namesMainTr <- layoutZTrans[layoutZTrans$TableName == 'utMain', 'FieldName']


#-----------------------------------------
#build initial database

sorting_wui_db = dbConnect(RSQLite::SQLite(), here('Raw_data/databases/sorting_wui.sqlite'))


#-----------------------------------------
#start with modern assessment data, then transactions, then historic assessment data
#Set up current assessment data

mainAssmt = base_dt = fread(here("Raw_data/zillow/Zasmt/Main.txt"),
              sep = '|',
              header = FALSE,
              stringsAsFactors = FALSE,             
              quote = "",
              col.names = t(col_namesMain$FieldName)
)

dbWriteTable(sorting_wui_db, 'mainAssmt', mainAssmt)

rm(mainAssmt)

#read in building data
bldg = fread(here('Raw_data/zillow/Zasmt/Building.txt'),
   sep = '|',
   header = FALSE,
   stringsAsFactors = FALSE,             
   quote = "",
  col.names = t(col_namesBldg$FieldName))

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

dbWriteTable(sorting_wui_db, 'bldg', bldg)

rm(bldg)

#read in building areas for square footage

sqft <- fread(here("Raw_data/ZAsmt/BuildingAreas.txt"),                   
                   sep = '|',
                   header = FALSE,
                   stringsAsFactors = FALSE,                         
                   quote = "",                                
                   col.names = t(col_namesBldgA)
)

sqft <- sqft[BuildingAreaStndCode %in% c('BAL',  # Building Area Living
                                         'BAF',  # Building Area Finished
                                         'BAE',  # Effective Building Area
                                         'BAG',  # Gross Building Area
                                         'BAJ',  # Building Area Adjusted
                                         'BAT',  # Building Area Total
                                         'BLF'), # Building Area Finished Living
									]
table(sqft$BuildingOrImprovementNumber)

sqft <- sqft[ , list(sqfeet = max(BuildingAreaSqFt, na.rm = T)), by = c("RowID", "BuildingOrImprovementNumber")]

dbWriteTable(sorting_wui_db, 'bldgSqFt', sqft)

rm(sqft)

#-----------------------------------------
#-----------------------------------------
## Instead of attempting to re-read this massive file in chunks, I opted to use a more-appropriate SQL backend to manage filtering, and then create
## tables with useful combinations of observations to access for specific tasks. This also makes this code much more user-friendly, and CONSIDERABLY more flexible.

#Transactions

transMain = fread(here("Raw_data/zillow/ZTrans/Main.txt"),
    sep = '|',
    header = FALSE,
    stringsAsFactors = FALSE,
    col.names = t(col_namesMainTr$FieldName)

dbWriteTable(sorting_wui_db, 'transMain', transMain)

rm(transMain)

propTrans=fread(here('Raw_data/zillow/ZTrans/PropertyInfo.txt'),
    sep = '|',
    header = FALSE,
    stringsAsFactors = FALSE,
    col.names = t(col_namesProp$FieldName))

dbWriteTable(sorting_wui_db, 'propTrans', propTrans)

rm(propTrans)

#short and sweet. The really difficult part comes next...

#-----------------------------------------
#-----------------------------------------
## historic assessment data

#step 1 is to find out just how big our historic assessment data is. This lets me still use my hard-earned C-code for counting lines in external files.

#For someone using another computer - this seems to be a perfect utilization of disk_frame, but for some reason that package will crash for me. Theoretically it can
#handle this size of a data processing procedure.

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

numb_obs_historic = wc(here("Raw_data/zillow/historic/ZAsmt/Main.txt"))
print(paste0('The historic data contains ', numb_obs_historic, ' main assessment table observations'))
## trying to read something in of this size is an excercise in futility. Let's create a series of cuts in the data. You'll want to adjust this to fit your
## working memory capacity.

breakpoints = seq(from = 1, to = numb_obs_historic, length.out = 180)
breakpoints_end = breakpoints[2:length(breakpoints)]
breakpoints_start = breakpoints[1:(length(breakpoints) - 1)] - 1

tmpfile = propTrans=fread(here("Raw_data/zillow/historic/ZAsmt/Main.txt"),
		sep = '|',
    	header = FALSE,
    	stringsAsFactors = FALSE, 
		nrow= breakpoints_end[1] - breakpoints_start[1],
    	col.names = t(col_namesMain$FieldName),
   		skip = breakpoints_start[1])

## In order to use the date column for this, you need to format it in a very weird way. See commented-out line below.

#tmpfile[,ExtractDate := paste0(ExtractDate%%10000, '-', ifelse(ExtractDate%/%10000 %in% c(1:9), paste0(0,ExtractDate%/%10000),ExtractDate%/%10000), '-', '01')]


#write substantial subset of columns initially so SQL can reasonably interpret classes, then directly write remainder of file
dbWriteTable(sorting_wui_db, 'histAsmtMain', tmpfile)
dbWriteTable(sorting_wui_db, 'histAsmtMain', here("Raw_data/zillow/historic/ZAsmt/Main.txt"), 
	append = TRUE, 
	sep = '|', 
	skip = breakpoints_end[1] - breakpoints_start[1])

numb_obs_historic_bldg = wc(here("Raw_data/zillow/historic/ZAsmt/Building.txt"))
print(paste0('The historic data contains ', numb_obs_historic_bldg, ' building assessment table observations'))
## trying to read something in of this size is an excercise in futility. Let's create a series of cuts in the data. You'll want to adjust this to fit your
## working memory capacity.

breakpoints = seq(from = 1, to = numb_obs_historic_bldg, length.out = 180)
breakpoints_end = breakpoints[2:length(breakpoints)]
breakpoints_start = breakpoints[1:(length(breakpoints) - 1)] - 1

tmpfile = propTrans=fread(here("Raw_data/zillow/historic/ZAsmt/Building.txt"),
		sep = '|',
    	header = FALSE,
    	stringsAsFactors = FALSE, 
    	nrow= breakpoints_end[1] - breakpoints_start[1],
    	col.names = col_namesBldg$FieldName,
   		skip = breakpoints_start[1])

#write substantial subset of columns initially so SQL can reasonably interpret classes, then directly write remainder of file
dbWriteTable(sorting_wui_db, 'histAsmtBldg', tmpfile)
dbWriteTable(sorting_wui_db, 'histAsmtBldg', here("Raw_data/zillow/historic/ZAsmt/Building.txt"), 
	append = TRUE, 
	sep = '|', 
	skip = breakpoints_end[1] - breakpoints_start[1])


numb_obs_historic_bldgA = wc(here("Raw_data/zillow/historic/ZAsmt/BuildingAreas.txt"))
print(paste0('The historic data contains ', numb_obs_historic_bldg, ' building square footage assessment table observations'))
## trying to read something in of this size is an excercise in futility. Let's create a series of cuts in the data. You'll want to adjust this to fit your
## working memory capacity.

breakpoints = seq(from = 1, to = numb_obs_historic_bldgA, length.out = 180)
breakpoints_end = breakpoints[2:length(breakpoints)]
breakpoints_start = breakpoints[1:(length(breakpoints) - 1)] - 1


tmpfile = propTrans=fread(here("Raw_data/zillow/historic/ZAsmt/BuildingAreas.txt"),
		sep = '|',
    	header = FALSE,
    	stringsAsFactors = FALSE, 
    	nrow= breakpoints_end[1] - breakpoints_start[1],
    	col.names = col_namesBldgA$FieldName,
    	skip = breakpoints_start[1])

#this transformation is IMPORTANT. If you read in historic data, do not forget this.
#tmpfile <- tmpfile[ , list(sqfeet = max(BuildingAreaSqFt, na.rm = T)), by = c("RowID", "BuildingOrImprovementNumber")]

#write substantial subset of columns initially so SQL can reasonably interpret classes, then directly write remainder of file
dbWriteTable(sorting_wui_db, 'histAsmtBldgA', tmpfile)
dbWriteTable(sorting_wui_db, 'histAsmtBldgA', here("Raw_data/zillow/historic/ZAsmt/BuildingAreas.txt"), 
	append = TRUE, 
	sep = '|', 
	skip = breakpoints_end[1] - breakpoints_start[1])