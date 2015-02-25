#======================================================================
# DISCLAIMER: This exercise was inspired from a Math project in an engineering class at McMaster Dec 2014
# I have modified the goal and the initialization of the project so that this exercise
# is actually very different than the original project it was inspired from.
#
# PROJECT DESCRIPTION:
# A Plane flies at constant altitude above a geography area
# The plane measures periodically its distance from ground by a laser
# A Map is available, giving Position (a positionID) and elevation in meters
#
# QUESTION:
# From a sample of N altitude measurements given by the plane,
# Determine the location of the plane. In other words,
# Find the Map PositionID where the plane was likely flying over
#
# - Take in account Plane Speed and interval of time between two altitude measures
#   NOTE: is it possible that the interval is variable?
# 
# 2014-12-27 - Tri Nguyen
# 2015-02-22 - fixed error in normalizeFlyPath(), CalcMiniDistance():
#              add flyDirection param to force the flyPath estimation to follow a consitent direction
#======================================================================

library(data.table)
library(ggplot2)

# please customize the path to suit your own project directory structure
source('./PlaneLocatorFUNC.R')

#---STEP1: Define an arbitraty map
#   1000 points, Elevation=Random 1:5000 meters
set.seed(20141227)
dtZMap <- data.table(
	Mpos = c(1L:1000L),
	ZElevation = sample(1L:5000L, 1000, replace=TRUE), # unit = meters
	key="Mpos"
)

# Review Entire Map
ggplot(dtZMap, aes(x=Mpos, y=ZElevation)) +
	labs(title = "Simulated Mountain Map", x = "Map Position", y = "Elevation (m)") +
	geom_area(fill="steelblue", alpha=0.6)

# Map: zoom on the first 100 points
ggplot(dtZMap[1:100,], aes(x=Mpos, y=ZElevation)) +
	labs(title = "Simulated Mountain Map (first 100 points)", x = "Map Position", y = "Elevation (m)") +
	geom_area(color="steelblue4", fill="steelblue", alpha=0.4)


#---STEP2: Generate a serie of Altitude Measurements supposedly made along a Fly Path
dtAlti <- generateFlyPath() # default = 10 measures
#dtAlti <- generateFlyPath(nbPoints = 20)

ggplot(dtAlti, aes(x=CopyFromMpos, y=ZElevation, ymax=max(ZElevation)*1.05)) +
	labs(title = "Altitude Measurements (+ Error in meters)", x = "Map Position", y = "Elevation (m)") +
	geom_area(size=2, color="steelblue4", fill="steelblue", alpha=0.4) +
	geom_point(aes(y=Altitude), shape=21, size=6, fill="mediumvioletred", alpha=0.6) +
	geom_text(aes(label=round(Altitude - ZElevation,2), vjust=-2), position = position_dodge(width=0.9), size=4) +
	scale_x_continuous(breaks = scales::pretty_breaks()) + # force X tick labels to display integer only
	theme(axis.text.x = element_text(color="black", size=rel(1.2), face="bold"))


#---STEP3: Estimate map points matching Altitudes measurements
# (a sequence of all possible Map Points having same Elevation than the altitudes of the Fly Path)
dtEstimate <- estimateFlyPath(dtAlti)


#---STEP4: Optimize the estimated path to get a consistent path
# (a 1-to-1 match between Altitude measure and Map Point)

# dtCalcPath <- normalizeFlyPath(dtEstimate, showPlot=TRUE)
dtCalcPath <- normalizeFlyPath(dtEstimate, showPlot=FALSE)


#---STEP5: compare CalcPath with initial simulated data
setkey(dtAlti, SeqID)
setkey(dtCalcPath, SeqID)
dtAlti[dtCalcPath, .(SeqID, CopyFromMpos, MposCalc, Diff=abs(CopyFromMpos - MposCalc), ZElevation, Altitude)]


#---STEP6: compare visually CalcPath vs Altitude Measures

ggplot(data=dtAlti, aes(x=CopyFromMpos, y=Altitude)) +
	labs(title = "Altitude Measurements vs Map Points Found", x = "Map Position", y = "Elevation (m)") +
	#geom_point(size=6, shape=19, color="palevioletred3") +
	geom_line(size=3, color="palevioletred3") +
	geom_point(data=dtCalcPath, aes(x=MposCalc, y=ZElevation), shape=21, size=8, fill="springgreen4", alpha=0.6) +
	scale_x_continuous(breaks = scales::pretty_breaks()) + # force X tick labels to display integer only
	theme(axis.text.x = element_text(color="black", size=rel(1.2), face="bold")) +
	annotate("text", x=-Inf, y=Inf, hjust=0, vjust=1.5, fontface="bold", col="palevioletred3", size=5, label="Altitude Measures") +
	annotate("text", x=-Inf, y=Inf, hjust=0, vjust=3.5, fontface="bold", col="springgreen4", size=5, label="Map Points Found")


#---(end)---#

