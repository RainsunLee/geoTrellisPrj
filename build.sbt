name := "geoTrellisPrj"

version := "0.1"

scalaVersion := "2.11.0"


libraryDependencies ++= Seq(
  "org.locationtech.geotrellis" %% "geotrellis-raster" % "2.0.0",
  "org.locationtech.geotrellis" %% "geotrellis-spark" % "2.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-spark-etl" % "2.1.0" exclude("com.fasterxml.jackson.core","jackson-databind"),
  "org.locationtech.geotrellis" %% "geotrellis-hbase" % "2.1.0",
  "org.locationtech.geotrellis" %% "geotrellis-cassandra" % "2.1.0",
  "org.apache.spark" %% "spark-core" % "2.1.0",
  "com.typesafe.akka" %% "akka-http" % "10.1.7",
  "com.typesafe.akka" %% "akka-stream" %"2.5.19"
)
