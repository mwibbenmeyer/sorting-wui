  
#!/bin/bash

#LAR URL list for data downloading
lar_2019_url="https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states=CA&years=2019"
lar_2018_url= "https://ffiec.cfpb.gov/v2/data-browser-api/view/csv?states=CA&years=2018"
lar_2017_url="https://s3.amazonaws.com/cfpb-hmda-public/prod/snapshot-data/2017_public_lar_txt.zip"
lar_2016_url="https://www.ffiec.gov/hmdarawdata/LAR/National/2016HMDALAR%20-%20National.zip"
lar_2015_url="https://www.ffiec.gov/hmdarawdata/LAR/National/2015HMDALAR%20-%20National.zip"
lar_2014_url="https://www.ffiec.gov/hmdarawdata/LAR/National/2014HMDALAR%20-%20National.zip"
lar_2013_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/2013/Lars.ultimate.2013.dat.zip"
lar_2012_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/2012/Lars.ultimate.2012.dat.zip"
lar_2011_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/UTL11/Lars.ultimate.2011.dat.zip"
lar_2010_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/UTL10/Lars.ultimate.2010.dat.zip"
lar_2009_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/ULAR09/2009_Ultimate_PUBLIC_LAR.dat.zip"
lar_2008_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/ULAR0708/lars.ultimate.2008.dat.zip"
lar_2007_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/ULAR0708/lars.ultimate.2007.dat.zip"
lar_2006_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/ULAR0506/LARS.ULTIMATE.2006.DAT.zip"
lar_2005_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/ULAR0506/LARS.ULTIMATE.2005.DAT.zip"
lar_2004_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/ULAR04/u2004lar.public.dat.zip"
lar_2003_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/HMS.U2003.LARS.zip"
lar_2002_url="https://catalog.archives.gov/catalogmedia/lz/electronic-records/rg-082/hmda/HMS.U2002.LARS.zip"

#declaration of variables used in later logic
FORCE="false" #set force download (overwrite) to false
LAR="false" #set download of LAR files to false
NC="-nc" #part of a wget call construction for omitting downloads of files already present, can be overridden by -F
SPECIFIC_FILE=""

#option setting: declare variables based on option flags passed
#this section defines what options are available in this script and provides basic instructions for use

while getopts "asptlFh" OPTION; do
        case ${OPTION} in

        		s) 		SPECIFIC_FILE=${2}
						;;
				p)		PANEL="true"
						;;
				t)		TS="true"
						;;
				l)		LAR="true"
						;;

        		F)		FORCE="true"
						NC=""
						;;

				a)
						echo "LAR Files:"
						echo "lar_2017"
						echo "lar_2016"
						echo "lar_2015"
						echo "lar_2014"
						echo "lar_2013"
						echo "lar_2012"
						echo "lar_2011"
						echo "lar_2010"
						echo "lar_2009"
						echo "lar_2008"
						echo "lar_2007"
						echo "lar_2006"
						echo "lar_2005"
						echo "lar_2004"
						exit 0
						;;
                h)
                        echo "Usage:"
                        echo "download_hmda.sh -h "
                        echo "download_hmda.sh -F "
                        echo "download_hmda.sh -s "
                        echo "download_hmda.sh -p "
                        echo "download_hmda.sh -t "
                        echo "download_hmda.sh -l "
                        echo "Options may be combined, use the following format:"
                        echo "download_options.sh -Fpt"
                        echo ""
                        echo " "
                        echo "   -F 	force download files and overwrite existing"
                        echo "   -l 	download all LAR files 2002 through 2019, 2018 and 2019 only include California"
                        echo "   -s     download specific file using name format:"
                        echo " 	  l for LAR, p for panel and t for timeseries options removed for cleanliness"
                        echo " 	  and a four digit number indicating the desired year, such as 2017."
                        echo " 	  example: panel_2017 for the 2017 panel file, lar_2017 for the 2017 LAR file"
                        echo "    or ts_2017 for the 2017 Transmittal Sheet file"
                        echo "   -a 	show all files available for download"
                        echo "   -h     help (this output)"
                        exit 0
                        ;;

        esac
done

#create directories to store downloaded files
echo "making data storage directories for LAR"
mkdir hmdadata
mkdir hmdadata/lar


if [ "${LAR}" = "true" ]; then
	if [ "${NC}" = "-nc" ]; then
		echo "downloading LAR files if not present"
	elif [ "${NC}" = "" ]; then
		echo "Force downloading all LAR files"
	fi
	#iterate over LAR URL array
	YEAR=2002 #set start year to 2002, existing code does not support downloads prior to 2004 or after 2017
	for i in "${lar_url_list[@]}"
	do #wget each URL in LAR array
		#echo
		#echo
		#specify file type for filename by year
		LAR_FILENAME="lar_${YEAR}.zip"
		YEAR=$((YEAR+1))
		if [ "${FORCE}" = "true" ]; then
			rm data/lar/${LAR_FILENAME}
		fi
		wget -q ${NC} -c -t 10 --show-progress -O  data/lar/${LAR_FILENAME} "${i}"
	done
fi

#option -s behavoir: download specific named file, requires second parameter as file key
#if -F is also passed force download of file
#check if file in list of available files to download
if [ "$SPECIFIC_FILE" != "" ]; then
	if [ "${NC}" = "-nc" ]; then
		echo "downloading ${SPECIFIC_FILE} if not exists"
	elif [ "${NC}" = "" ]; then
		echo "Force downloading ${SPECIFIC_FILE}"
	fi

	#set file type .DAT, .TXT, .ZIP for the $SPECIFC_FILE variable
	#set $URL for specific file by checking arrays by dataset, use year - 2004 for index reference

	NO_EXT="${SPECIFIC_FILE%.*}"
	YEAR=${NO_EXT:(-4)}
	URL_INDEX="$((YEAR - 2004))"

	if [ "${SPECIFIC_FILE:0:1}" = "p" ]; then
		URL=${panel_url_list[$URL_INDEX]}
		FOLDER="panel"
		if [ $YEAR = 2017 ]; then
			FILE_EXT=".zip"
		elif [ $YEAR -gt 2013 ]; then
			FILE_EXT=".zip"
		else
			FILE_EXT=".dat"
		fi

	elif [ "${SPECIFIC_FILE:0:1}" = "t" ]; then
		URL=${ts_url_list[$URL_INDEX]}
		FOLDER="ts"
		if [ $YEAR -gt 2013 ] && [ $YEAR -lt 2017 ]; then
			FILE_EXT=".zip"
		elif [ $YEAR -eq 2017 ]; then
			FILE_EXT=".zip"
		else
			FILE_EXT=".dat"
		fi

	elif [ "${SPECIFIC_FILE:0:1}" = "l" ]; then
		URL=${lar_url_list[URL_INDEX]}
		FOLDER="lar"
		FILE_EXT=".zip"
	fi
	#remove specific file if force flag was passed
	if [ "${FORCE}" = "true" ]; then
		rm data/$FOLDER/"${SPECIFIC_FILE}${FILE_EXT}"
	fi
	#download the specific file using passed force parameter
	wget -q ${NC} -c -t 10 --show-progress -O data/$FOLDER/"${SPECIFIC_FILE}${FILE_EXT}" "${URL}"
fi