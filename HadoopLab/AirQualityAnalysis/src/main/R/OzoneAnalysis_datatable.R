# *************************************************************************************
# This is the Solution in R (version using data.table for data query)
# to be compared in design time & execution duration with earlier Hadoop solutions
# in https://github.com/NeuroNex/UG/tree/master/HadoopLab
#
# Analyse Ozone measurements of National Air Pollution Surveillance Program (NAPS)
# Source Data: http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en
# - Across Canada, for the entire year 2012 
#   (why 2012 in 2015? to make results comparable to Hadoop solutions which were designed earlier)
# - Each NAPS Station has 1 Ozone record per day, each day has 24 reading of Ozone
# - Compute the Average Ozone daily value (avg of 24 readings)
# - Per City, for the entire year (i.e. the entire source data): Calculate the aggregates Max(DailyAvgOzone), Avg(DailyAvgOzone)
# - Make a Report "Most poluted cities", ranking by MaxAvgOzone descending
#
# 2015-07-06 - Tri.Nguyen
# *************************************************************************************

library(data.table)
library(magrittr)
library(readr)
library(stringi)

startProcTime <- proc.time()

#--------------------------------------------
# 1. Load Station data
#--------------------------------------------
message("==========| Load Station data |==========")

# parse CSV superfast by data.table::fread()
dtStationORIG <- data.table::fread(input="~/Documents/IntelliJProjects/UG/HadoopLab/AirQualityAnalysis/src/main/resources/Stations_v28012014.csv",
	colClasses=c("integer","character","character","character","character","character","character","character","character","character","character","character","numeric","numeric","numeric","integer","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character","character"),
	header=TRUE, sep = ",") # na.strings="NA"

setnames(dtStationORIG, names(dtStationORIG), c("StationID", "StationName", "Type", "Status", "TOXIC", "Designated", "Province", "STREET_ADDRESS", "CityName", "COUNTRY", "FSA", "PostalCode", "TimeZone", "Lat_Decimal", "Long_Decimal", "Elevation_m", "SO2", "CO", "NO2", "NO", "NOX", "O3", "PM_10_continuous", "PM_25_015_continuous", "PM_25_017_dryer", "PM_25_018_BAM_RH45", "PM_25_019_gravimetric40/50", "PM_25_020_gravimetric30/50", "PM_25_021_gravimetric30", "PM_25_022__FDMS", "PM_1_GRIMM180", "PM_25_026_GRIMM180", "PM_10_GRIMM180", "PM_25_030_BAM_RH3", "PM_10_031_BAM_RH35", "PM_25_032_SHARP", "VOC", "ALDEH_KETO", "DICHOTX_METALS", "PCB", "PCDD", "PBDE"))

# Normalize Province Name
dtStationORIG$Province <- dtStationORIG$Province %>%
	toupper() %>%
	gsub("NEWFOUNDLAND-LABRADOR", "NEWFOUNDLAND AND LABRADOR", ., ignore.case=TRUE) %>%
	gsub("NEWFOUNLAND AND LABRADOR" , "NEWFOUNDLAND AND LABRADOR", ., ignore.case=TRUE)

# Title Case CityName
dtStationORIG$CityName <- stringi::stri_trans_totitle(dtStationORIG$CityName)

# We only need a few properties of the Station Info
dtStations <- dtStationORIG[, .(StationID, StationName, Province=as.factor(Province), CityName=as.factor(CityName))]


#--------------------------------------------
# 2. Load Ozone measures
#--------------------------------------------
message("==========| dtOzone: Load Ozone measures |==========")

# data.table::fread() doesn't parse fixed length, use readr::read_fwf() then convert it to datatable
ozoneColumns <- readr::fwf_widths(
	widths = c(3,6,8, 4,4,4, 4,4,4,4,4,4,4,4,4,4,4,4, 4,4,4,4,4,4,4,4,4,4,4,4),
	col_names = c("PollutantCode", "StationID", "DateStr", "AVG", "MIN", "MAX", "H01", "H02", "H03", "H04", "H05", "H06", "H07", "H08", "H09", "H10", "H11", "H12", "H13", "H14", "H15", "H16", "H17", "H18", "H19", "H20", "H21", "H22", "H23", "H24"))

dtOzone <- data.table(readr::read_fwf("~/Documents/IntelliJProjects/UG/HadoopLab/AirQualityAnalysis/src/main/resources/2012O3.hly",
	skip=1, na="-999", # n_max=10,
	col_positions = ozoneColumns, col_types = "ciciiiiiiiiiiiiiiiiiiiiiiiiiii"))

# discard records which have all 24 readings = NA (b/c that will upset the calc of min/max/avg, even when na.rm=TRUE)
# NOTE: there are 1398 records havinbg all 24 readings = NA
message("==========| Discard records having all 24 readings = NA |==========")
dtOzone <- dtOzone[
	  is.na(H01) + is.na(H02) + is.na(H03) + is.na(H04) + is.na(H05) + is.na(H06) + is.na(H07) + is.na(H08) +
	  is.na(H09) + is.na(H10) + is.na(H11) + is.na(H12) + is.na(H13) + is.na(H14) + is.na(H15) + is.na(H16) +
	  is.na(H17) + is.na(H18) + is.na(H19) + is.na(H10) + is.na(H21) + is.na(H22) + is.na(H23) + is.na(H24) < 24, ]


message("==========| Calc Avg, Min, Max on H01..H24 columns |==========")
dtOzone[, `:=`
  (AvgCalc = mean(c(H01,H02,H03,H04,H05,H06,H07,H08,H09,H10,H11,H12,H13,H14,H15,H16,H17,H18,H19,H20,H21,H22,H23,H24), na.rm=TRUE),
   MinCalc = min (c(H01,H02,H03,H04,H05,H06,H07,H08,H09,H10,H11,H12,H13,H14,H15,H16,H17,H18,H19,H20,H21,H22,H23,H24), na.rm=TRUE),
   MaxCalc = max (c(H01,H02,H03,H04,H05,H06,H07,H08,H09,H10,H11,H12,H13,H14,H15,H16,H17,H18,H19,H20,H21,H22,H23,H24), na.rm=TRUE)),
   by=1:nrow(dtOzone)]

message("==========| Avg Ozone by City |==========")
setkey(dtOzone   , StationID)
setkey(dtStations, StationID)
dtAvgO3ByCity <- dtOzone[dtStations, .(StationID, Province, CityName, AvgCalc), nomatch = 0]

message("==========| Order Cities by YearMaxOzone |==========")
dtPollutedCities <- dtAvgO3ByCity[,
	.(YearAvgOzone = mean(AvgCalc), YearMaxOzone = max(AvgCalc)),
	by = list(Province, CityName)] [ order(-YearAvgOzone, -YearMaxOzone) ]

message("==========| Writing Report |==========")
write.table(dtPollutedCities[, .(Province, CityName, YearAvgOzone = round(YearAvgOzone,2), YearMaxOzone = round(YearMaxOzone,0))],
	file="~/AAA/OzoneByCities_R_datatable.txt", col.names=TRUE, row.names=TRUE, quote=FALSE, sep="\t")

#--------------------------------------------
# Stopwatch finished: 1.45 secs
#--------------------------------------------
elapsedTime <- proc.time() - startProcTime
message(sprintf("==========| Elapsed Time:%6.2f secs |==========", elapsedTime["elapsed"]))

# ---(end)---
