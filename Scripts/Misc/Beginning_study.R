#Preamble
setwd('/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/')
library(pacman)
p_load(tidyverse, tidymodels, tidycensus, raster, sf, viridis, 
       rgdal, tmap, data.table, broom, shiny, leaflet, tigris, magrittr, ggmap, RColorBrewer)

## Custom map theme

theme_map <- function(...) {
  theme_minimal() +
    theme(
      text = element_text(family = "Ubuntu Regular", color = "#22211d"),
      axis.line = element_blank(),
      axis.text.x = element_blank(),
      axis.text.y = element_blank(),
      axis.ticks = element_blank(),
      axis.title.x = element_blank(),
      axis.title.y = element_blank(),
      # panel.grid.minor = element_line(color = "#ebebe5", size = 0.2),
      panel.grid.major = element_line(color = "#ebebe5", size = 0.2),
      panel.grid.minor = element_blank(),
      plot.background = element_rect(fill = "#f5f5f2", color = NA), 
      panel.background = element_rect(fill = "#f5f5f2", color = NA), 
      legend.background = element_rect(fill = "#f5f5f2", color = NA),
      panel.border = element_blank(),
      ...
    )
}

## Practice with space and Raster Data to shake the rust, 
## using WFP, tidycensus, HMDA and 

## Interim goal - ztrax data spans California and the years 2002-2017. To balance home sales
## on either side, ideally an event for study would occur between years

###################

## Tidycensus Census Tracts

###################

## Getting tidycensus tract-level spatial data is relatively easy. It requires tidycensus

california_tracts <- get_acs(state = 'CA', geography = "tract", 
                  variables = "B19013_001", geometry = TRUE)

## Canyon fire shape.

### Background:

#Canyon fire, in the SW region of Kern County California destroyed 100 structures in 2012. This fire destroyed the most properties  
# out of any wildfire in California that year, though did not make many national headlines. 

#An alternate initial fire would be the 2013 fire 'Powerhouse' and occurred in/around LA.
#this fire damaged many structures (inc. 24 homes) and threatened ~ 1000. 30,000 acres burned.
firepal = brewer.pal(4, 'YlOrRd')
canyon = read_sf('/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/Raw_data/CanyonWildfireShapefiles/ca_canyon_20110907_2039_dd83/ca_canyon_20110907_2039_dd83.shp')
canyon = st_transform(canyon, crs = crs(california_tracts))

FHSZ = read_sf('/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/Raw_data/Fire_Hazard_Severity_Zones/Fire_Hazard_Severity_Zones-shp/Fire_Hazard_Severity_Zones.shp')
sra = read_sf('/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/Raw_data/StateResponsibilityAreas20/sra20_1/SRA20_1.shp')

## extent of 2011, Kern county fire 'Canyon'. 
ggplot() +
  geom_sf(data = canyon, fill = 'orange', size = .001) +
  geom_sf(data = kern, alpha = .2, size = .2) +
  theme_map()



ggplot() + geom_sf(data = california_tracts, alpha = .2, size = .02) + 
  geom_sf(data = FHSZ['HAZ_CODE']) +
  theme_map()

ggplot() + geom_sf(data = sra) + geom_sf(data = california_tracts, alpha = .2, size = .02)

## read in 2018 wildfire potential data

## The second of these two methods seems to work better with ESRI Grid style file formats

# Method 1: raster directly
wfp_us_rast = raster(
  'Raw_data/WFP_Layers/RDS-2015-0047-2/Data/whp_2018_continuous/whp2018_cnt/w001001.adf'
  )

#Method 2: raster using GDAL and then rip that into a spatial dataframe
wfp_us_rast_GDAL = readGDAL(
  'Raw_data/WFP_Layers/RDS-2015-0047-2/Data/whp_2018_continuous/whp2018_cnt/w001001.adf'
  )

## sanity check
is.raster(wfp_us_rast_GDAL)

## This step takes a while - may be worthwhile to crop/mask down to relevant
# region and then transform into our ggplot friendly format

wfp_us_spdf = as(wfp_us_rast_GDAL, "SpatialPixelsDataFrame")

#only contains SRA for california - LRA areas are contained in individual 
fhsz = read_sf('/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/Raw_data/Fire_Hazard_Severity_Zones/Fire Hazard Severity Zones (Adopted in 2017)/data/commondata/fire_hazard_severity_zones_2017/fhszs06_3.shp')
fhszvent = read_sf('/Users/connor/Desktop/GithubProjects/propval-wui/sorting-wui/Raw_data/LocalResponsibilityAreas/VenturaLRA/c56fhszl06_3.shp')

venturacensus = california_tracts[str_detect(california_tracts$NAME, 'Ventura'),]

#ventbb = st_as_sfc(st_bbox(venturacensus, n = 1))
ventcounty_outline = st_combine(venturacensus)
venturacensus$area = st_area(venturacensus)
ventcounty_outline = venturacensus %>% summarise(area = sum(area))

vhsz = st_transform(fhsz, st_crs(ventbb))
fhsz = st_transform(fhsz, st_crs(venturacensus))
fhsz %<>% st_transform(st_crs(venturacensus))
ggplot(data = ventcounty_outline) + geom_sf()

fhszcrs = st_crs(fhsz)
ventcounty_outline = st_transform(ventcounty_outline, fhszcrs)
sravent = st_crop(st_make_valid(fhsz), ventcounty_outline)

map = get_googlemap("Ventura, CA", zoom = 10, maptype = 'terrain')
ggplot(data = sravent) + geom_sf(aes(fill = HAZ_CLASS), size = .1) + geom_sf(data = venturacensus, fill = NA, size = .3) + geom_sf(data = fhszvent, fill = 'darkred', size = .1, color = 'black')

st_crs(map)

ggmap(map, alpha = .2)  + 
  geom_sf(data = sravent, aes(fill = HAZ_CLASS), size = .1, inherit.aes = FALSE, alpha = .3) + 
  geom_sf(data = venturacensus, fill = NA, size = .3, inherit.aes = F, alpha = .3) + 
  geom_sf(data = fhszvent, fill = 'darkred', size = .1, color = 'black', inherit.aes = F, alpha = .3) +
  coord_sf(crs = st_crs(4326))
select(sravent, any_of(c('HAZ_CLASS', 'SRA')))
intermediary = fhszvent %>% mutate(HAZ_CLASS = 'Very High, LRA')

<- factor(iris$Species, levels = c("virginica", "versicolor", "setosa"))

ggmap(map, alpha = .2)  + 
  geom_sf(data = venturacensus, fill = NA, size = .3, inherit.aes = F, alpha = .3) + 
  geom_sf(data = fullventfhsz_goog, aes(fill = HAZ_CLASS), size = .1, color = 'black', inherit.aes = F, alpha = .3) +
  coord_sf(crs = st_crs(4326)) +
  scale_fill_manual(values = firepal)

fullventfhsz = rbind(sravent %>% dplyr::select(any_of(c('HAZ_CLASS', 'SRA'))), intermediary %>% dplyr::select(any_of(c('HAZ_CLASS', 'SRA'))))
fullventfhsz_goog = st_transform(fullventfhsz, crs = 3857)
