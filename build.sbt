name := "bkk-data-process-spark"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.4"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.4"

libraryDependencies += "org.jpmml" % "jpmml-sparkml" % "1.5.4" exclude("com.google.guava", "guava")
libraryDependencies += "org.mongodb.scala" %% "mongo-scala-driver" % "2.6.0"

resolvers += "sxfcode Bintray Repo" at "https://dl.bintray.com/sfxcode/maven/"

libraryDependencies += "com.sfxcode.nosql" %% "simple-mongo" % "1.6.5"
// https://mvnrepository.com/artifact/databricks/spark-deep-learning
libraryDependencies += "databricks" % "spark-deep-learning" % "1.5.0-spark2.4-s_2.11"
