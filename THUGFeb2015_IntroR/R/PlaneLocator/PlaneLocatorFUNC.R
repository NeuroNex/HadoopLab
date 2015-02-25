#======================================================================
# Function used by PlaneLocator.R
#
# 2014-12-27 - Tri Nguyen
#======================================================================

library(data.table)
library(grid) ; library(gridExtra) # multiplots


#----------------------------------------------------------------------------
# Simulate a Fly Path: a sequence of altitude measurements along the mountain map
# The measures must progress in the same direction along the mountain map
# - measure M1 corresponds to Map Position Mpos1
# - measure M2 corresponds to Map Position Mpos2
#   with Mpos2 > Mpos1 if direction is forward
#        Mpos2 < Mpos1 if direction is backward
# TEST:
# generateFlyPath()
# generateFlyPath(nbPoints = 16, startingMapPos=555L)
#----------------------------------------------------------------------------
generateFlyPath <- function(
	nbPoints = 10L, # the number of altitude measurements
	startingMapPos = sample(100L:900L, 1), # the position in the mountain map used as starting point of the fly path
	direction = integer() # +1: forward, -1 backward
	)
{
	if (missing(direction)) {
		# choose randomly a direction: either forward or backward
		direction <- ifelse(rbinom(1, 4, 0.5) < 2, -1L, 1L)
	}

	# Simulate a small random gap between each map point
	randomGap <- sample(1:4, 1)
	simulPoints <- seq(from=startingMapPos, by=randomGap * direction, length.out=nbPoints)
	
	# This is rather crazy: the interval between 2 measures is variable
	#simulPoints <- rbinom(nbPoints, 3, 0.4) +1 +startingMapPos #+1 to avoid getting 0 from rbinom()
	
	message(sprintf("DEBUG: nbPoints:%d, startingMapPos:%d - direction:%d, randomGap:%d", nbPoints, startingMapPos, direction, randomGap))
	
	# Simulate altitude measurement imprecision by adding a small jitter to the real ZElevation
	# error has a mean of 5 metters, and Std Deviation of 1 meters
	dtSimulPath <- cbind(SeqID = c(1L:nbPoints),
		dtZMap[Mpos %in% simulPoints,
				 .(CopyFromMpos=Mpos, ZElevation, Altitude = ZElevation + rnorm(nbPoints, mean=5, sd=1))])
	
	return (dtSimulPath)	
}


#----------------------------------------------------------------------------
# Estimate a Fly Path based on Altitude measures
# For each altitude, find the closest map positions having ZElevation = altitude
# It is very possible that the number of positions found on the map exceeds
# the number of altitude measures. B/C there maybe several ZElevation corresponding to one altitude
#----------------------------------------------------------------------------
estimateFlyPath <- function(
	dtAltiMeasures = data.table()	# altitude measures, ordered chonologically (by SeqID)
)
{
	dtMapPoints <- data.table()
	for (kk in seq_along(dtAltiMeasures$SeqID) ) {
		posFound <- getMountainPos(kk, dtAltiMeasures[SeqID==kk, Altitude])
		# message(sprintf("SeqID: %d, Found: %d position", kk, nrow(posFound)))
		
		if (nrow(posFound) > 0) {
			dtMapPoints <- rbind(dtMapPoints, posFound)
			#startingPos <- min(posFound$Mpos)
		}
	}
	
	message(sprintf("Map Points Found:%d - Nb Measures:%d, Diff = %d",
						 nrow(dtMapPoints), length(dtAltiMeasures$SeqID),
						 nrow(dtMapPoints) - length(dtAltiMeasures$SeqID)))
	
	return(dtMapPoints)
}


#----------------------------------------------------------------------------
# Search possible mountain positions matching a given altitude
# results ordered by Mountain PositionID 
#----------------------------------------------------------------------------
getMountainPos <- function(
	SeqID = integer(), # SeqID, readonly, to be returned in result datatabl for tracking purpose
	altiToSearch = numeric(), # the altitude to match
	filterMiniMpos = 0L, # the minimum Mountain Position, 0: any MPos found is OK, >0 only Mpos greater tha given pos is selected
	filterMaxMpos  = 0L  # the maximum Mountain Position
) 
{
	# Some hardcoded stuffs, to be tidied up later
	# Measurement Errors simulated in generateFlyPath(): rnorm(nbPoints, mean=5, sd=1)
	# Measurement Error tolerance: Average Error + 3 standard deviations
	# NOTE: Mean + 2*SD was not enough, got some incorrect estimation
	errTolerance <- 5+3
	
	maybePos <- dtZMap[abs(ZElevation - altiToSearch) < errTolerance, .SD]
	maybePos[, ':=' (SeqID = SeqID, Altitude = altiToSearch, AbsDiff = abs(ZElevation - altiToSearch))]	

	if (filterMiniMpos > 0) {
		maybePos <- maybePos[Mpos > filterMiniMpos, .SD]
	}
	if (filterMaxMpos > 0) {
		maybePos <- maybePos[Mpos < filterMaxMpos, .SD]
	}
	
	return (maybePos)
}


#----------------------------------------------------------------------------
# Normalize an estimated Fly Path: 
# - Eliminate duplicated positions within the same SeqID
# - Ensure number of map point found == number of altitude measures
# Method: find the position-pair between two adjacent SeqID having minimum distance
#----------------------------------------------------------------------------
normalizeFlyPath <- function(
	dtMapPoints = data.table(), # the estimated path to be optimized
	showPlot = FALSE # TRUE=display plot showing the progression of the path optimization
)
{
	setkey(dtMapPoints, SeqID) # make 100% sure measures are sequenced in chronological order
	
	# nb of unique SeqID
	uniqSeqIDCount <- length(unique(dtMapPoints$SeqID))

	if (showPlot) {
		estimatedPlot <- ggplot() +
			geom_line(data=dtMapPoints, aes(x = Mpos, y = ZElevation), size=2, color="grey60") +
			labs(title = "Normalization of Estimated Map Points", x = "Map Position", y = "Elevation (m)")
	}
	
	finalPath <- data.table()
	flyDirection <- 0L #0: unknown, -1:backward ; +1:forward
	for (kk in seq(1, uniqSeqIDCount-1, by=2) ) {
		#message(sprintf("kk:%d, flyDirection:%d", kk, flyDirection))
		posPair <- CalcMiniDistance(kk, kk+1, dtMapPoints, flyDirection)
		# now that we know the pair of map point, we can deduct the fly direction
		# increasing MPos value:  forward (flyDirection = +1L)
		# decreasing MPos value: backward (flyDirection = -1L)
		flyDirection <- ifelse(posPair$Mpos[2] > posPair$Mpos[1], 1L, -1L)
		# message(sprintf("kk:%d, Mpos1:%d, Mpos2:%d, flyDirection:%d", kk, posPair$Mpos[1], posPair$Mpos[2], flyDirection))
		# print(posPair)
	
		finalPath <- rbind(finalPath, posPair)
		#print(finalPath)
		
		if (showPlot) {
			message (sprintf("Show Plot, SeqID: %d - %d", kk, kk+1))
			
			estimatedPlot <- estimatedPlot + geom_line(data=finalPath, aes(x = Mpos, y = ZElevation), size=1, color="midnightblue")
			optimPlot <- ggplot(finalPath, aes(x = Mpos, y = ZElevation)) + geom_line(size=1, color="midnightblue")
			
			gridExtra::grid.arrange(estimatedPlot, optimPlot, nrow=2, ncol=1)
			Sys.sleep(2) # duration in seconds
		}
	}
	
	return (finalPath[, .(SeqID, MposCalc=Mpos, ZElevation)])
}


#----------------------------------------------------------------------------
# Determine the position-pair giving the minimum distance between two sequences of measures
# Each sequence may have 1..N point positions
# TEST
# SeqID=21 has two positions, SeqID=22 has 3 positions -> 6 possible position-pairs
#
# CalcMiniDistance(21, 22, data.table(SeqID = c(21 , 21, 22, 22, 22), Mpos = c(127,240,  9,246,468)))
#
#    SeqID Mpos
# 1:    21  127
# 2:    21  240
# 3:    22    9
# 4:    22  246
# 5:    22  468
#
# RESULT
#    SeqID Mpos ZElevation
# 1:    21  240       1137
# 2:    22  246       4903
#----------------------------------------------------------------------------
CalcMiniDistance <- function(seq1 = integer(), seq2 = integer(),
	dtPath = data.table(), # the Path table containing all the positions
	flyDirection = 0L # 0:Unknown, -1:backward, +1:forward
	) 
{
	if (missing(flyDirection)) {
		# choose randomly a direction: either forward or backward
		flyDirection <- 0L
	}
	
	# make two subsets of positions, one for each sequence
	dt1 <- dtPath[SeqID == seq1, .(SeqID, Mpos)]
	dt2 <- dtPath[SeqID == seq2, .(SeqID = seq1, Mpos)] # NOTE: substitute seq2 by seq1 to allow join

	setkey(dt1, SeqID)
	setkey(dt2, SeqID)
	dtPair <- dt1[dt2, .(Mpos1=Mpos, Mpos2=i.Mpos, Distance=abs(Mpos - i.Mpos)), allow.cartesian=TRUE]
	
	# explore min distance calc only when there are at least more than 1 position-pair
	if (nrow(dt1) > 1 | nrow(dt2) > 1) {
		# the most probable path is the one giving minimum distance between positions of the two sequences
		if (flyDirection == 0L)
			dtPair <- dtPair[which.min(Distance), .SD]
		else if (flyDirection < 0L) {
			# select pair-point having the minimum distance
			# if several pairs are available, the pair having their MPos closest to each other is the right one
			# Example
			#         Mpos1   Mpos2  Distance
			#         678     677    35       <-- this is the correct pair
			#         678     451    35  
			dtPair <- dtPair[Mpos2 < Mpos1, .SD] [which.min(Distance), .SD]
		}
		else {
			dtPair <- dtPair[Mpos2 > Mpos1, .SD] [which.min(Distance), .SD]
		}
	}
	
	# Pivot a result like:
	#   Mpos1 Mpos2 Distance
	#     240   246        6
	# To
	# SeqID  Mpos
	#    21   240
	#    22   246
	
	dtPair <- rbind(dtPair[, .(SeqID=seq1, Mpos=Mpos1)], dtPair[, .(SeqID=seq2, Mpos=Mpos2)] )
	
	# Add ZElevation by Joining to Map
	# SeqID  Mpos  ZElevation
	#    21   240  2546
	#    22   246  1920
	
	setkey(dtPair, Mpos)
	# MUST order return datatable by SeqID to facilitate the detection of the flyDirection
	# dtReturnPair <- dtZMap[dtPair, .(SeqID, Mpos, ZElevation), nomatch=0L]
	# setkey(dtReturnPair, SeqID)
	
	return(dtZMap[dtPair, .(SeqID, Mpos, ZElevation), nomatch=0L][order(SeqID)])
}

