#======================================================================
# Introduction to R - A toolbox for Data Analysts
# Toronto Hadoop User Group - Feb 2015
# 
# 2015-02-23 - Tri.Nguyen
#======================================================================


#----------------------------------------------------------
# DEMO #1
# Starting from 1
# The sum of all consecutive odd numbers = perfect square
#----------------------------------------------------------

seq(from=1, to=9, by=2)

sum(seq(1,9, by=2))

sqrt(sum(seq(1,9, by=2)))
sqrt(sum(seq(1,2015, by=2)))
sqrt(sum(seq(1,2015^2, by=2)))
sqrt(sum(seq(1,37851825, by=2)))



#----------------------------------------------------------
# Showing Equivalence of SQL Statements in R Syntax
#----------------------------------------------------------
library(dplyr)
library(data.table)
library(magrittr)

# please customize the path to suit your own project directory structure
dtGDP <- data.table(read.csv("./data/Countries_Top30ByGDP.csv", stringsAsFactors=FALSE))


#------------------------------------------------------------------------
# SELECT Continent, Country, CurrencyCode, Population, GDP2013,
# 	GDPperCapita AS (1E6 * GDP2013/Population)
# FROM dtGDP
# WHERE Continent="Europe" AND GDP2013 > 1000000
# ORDER BY GDPperCapita DESC
#------------------------------------------------------------------------

#--dplyr syntax
dtGDP %>%
	filter(Continent=="Europe" & GDP2013 > 1E6) %>%
	transmute(Continent, Country, CurrencyCode, Population, GDP2013,
				 GDPperCapita = as.integer(1E6 * GDP2013/Population)) %>%
	arrange(desc(GDPperCapita))

#--data.table syntax
dtGDP[Continent=="Europe" & GDP2013 > 1E6,
	.(Continent, Country, CurrencyCode, Population, GDP2013)
	] [, GDPperCapita := as.integer(1E6 * GDP2013/Population)
	] [order(-GDPperCapita)]


#------------------------------------------------------------------------
# SELECT Continent, AVG(GPD) AS AvgGDP, COUNT() AS CountryCount
# FROM dtGPD
# GROUP BY Continent
# ORDER BY AvgGDP DESC
#------------------------------------------------------------------------

#--dplyr syntax
dtGDP %>%
	group_by(Continent) %>%
	summarize(CountryCount=n(), AvgGDP=mean(GDP2013)) %>%
	arrange(desc(AvgGDP))

#--data.table syntax
dtGDP[, .(AvgGDP = mean(GDP2013), CountryCount=.N), by=Continent
	] [, .(Continent, CountryCount, AvgGDP)] [order(-AvgGDP)]



#------------------------------------------------------------------------
# SELECT TOP 10 C.Country, C.Continent, ECO.GDPperCapita, ECO.PopuDensity
# FROM dtCountry C
# INNER JOIN dtEconomy ECO ON ECO.CountryCode = C.CountryCode
# ORDER BY Country
#------------------------------------------------------------------------

dtCountry <- dtGDP[, .(CountryCode=ISOCode, Country, Continent, CurrencyCode)]
dtEconomy <- dtGDP[, .(CountryCode=ISOCode, GDP2013, Population, SurfaceKM2,
								GDPperCapita = as.integer(1E6 * GDP2013/Population),
								PopuDensity  = as.integer(Population / SurfaceKM2))]

#--dplyr syntax
dtCountry %>%
	inner_join(dtEconomy, by="CountryCode") %>%
	select(Country, Continent, GDPperCapita, PopuDensity) %>%
	arrange(Country) %>%
	top_n(10, desc(Country))
	#filter(row_number() <= 10) # dplyr BUG! not working on data.table


#--data.table syntax
setkey(dtCountry, CountryCode)
setkey(dtEconomy, CountryCode)
dtCountry[dtEconomy, .(Country, Continent, GDPperCapita, PopuDensity)
	] [order(Country)] [1:10]

#---(end)---#

