name := "spark-table-compare"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.2.3" % "provided",
  "com.typesafe" % "config" % "1.3.4",
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.4.2" % "provided"
)

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
mainClass in assembly := Some("TableCompare")
assemblyOutputPath in assembly := new File(System.getProperty("user.dir")+"/bin/spark-table-compare.jar")