## Description
2014-04-19 - Tri Nguyen, inital release

This is the Solution in Java Map Reduce which analyse Ozone measurements
of National Air Pollution Surveillance Program (NAPS)
Source Data: <http://maps-cartes.ec.gc.ca/rnspa-naps/data.aspx?lang=en>

- Across Canada, for the entire year 2012 
  (why 2012 in 2015? to make results comparable to Hadoop solutions which were designed earlier)
- Each NAPS Station has 1 Ozone record per day, each day has 24 reading of Ozone
- Compute the Average Ozone daily value (avg of 24 readings)
- Per City, for the entire year (i.e. the entire source data): Calculate the aggregates Max(DailyAvgOzone), Avg(DailyAvgOzone)
- Make a Report "Most poluted cities", ranking by MaxAvgOzone descending

## Setup and Execute
Read the "HOW TO TEST" comment section in src/main/java/CompositeKeyDriver.java

## WARNING: Java MapReduce solution is no longer maintained
This Java MapReduce solution is no longer maintained since July 2015. For the following reasons:

- JavaMR code is heavy in maintenance.
- The Spark solution is better than JavaMR in speed and ease of maintenance.

Nevertheless, if you are interested in JavaMR, this solution is still pretty well designed and is far better than the ususal "words count" tutorial.
It implements the following techniques:

- Sequencing 2 MR jobs
- Use of Distribute Cache for client side Join
- Serializable custom class
- Custom record parser
- JUnit


--- (end) ---
