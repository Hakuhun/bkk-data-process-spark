name := "bkk-data-process-spark"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.4"
libraryDependencies += "ml.combust.mleap" %% "mleap-spark" % "0.14.0"