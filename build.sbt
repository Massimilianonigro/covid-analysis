name := "covid-analysis"

version := "0.1"

scalaVersion := "2.12.13"

val sparkVersion = "3.1.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)