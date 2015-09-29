// build the /13_AirQualityAnalysis/src/main/scala/*.scala
// (see the scala source on how to execute)
name := "SparkOzoneAnalysis"

version := "1"

//force specific Scala version
// ATTN: avoid Scala version conflict between SBT and the Spark libs. Reason, error
// Exception in thread "main" java.lang.NoSuchMethodError: scala.Predef$.ArrowAssoc(Ljava/lang/Object;)Ljava/lang/Object;
//scalaVersion := "2.11.6"

// exclude src/main/java from SBT build
// javaSource and scalaSource are inputs to unmanagedSourceDirectories.
// Set unmanagedSourceDirectories to be scalaSource only:
unmanagedSourceDirectories in Compile <<= (scalaSource in Compile)( _ :: Nil)

// additional libraries: http://www.scala-sbt.org/0.13/tutorial/Library-Dependencies.html
// Syntax1: groupID % artifactID % revision
// Syntax2: groupID % artifactID % revision % configuration
// NOTE: If you use groupID %% artifactID % revision rather than groupID % artifactID % revision
// (the difference is the double %% after the groupID), sbt will add your project’s Scala version to the artifact name
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.2.1" % "provided"
  //"org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"
)

