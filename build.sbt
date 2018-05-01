
name := "sparkles"

version := "1.0"

scalaVersion := "2.12.5"

libraryDependencies ++= Seq(
  "com.google.cloud.bigdataoss" % "bigquery-connector" % "0.10.0-hadoop2" % "provided",
  "com.google.cloud" % "google-cloud-bigquery" % "0.20.0-beta" % "provided",
  "com.github.scopt" % "scopt_2.11" % "3.3.0" % "provided",
  "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.4" % "provided",
  "com.datastax.spark" % "spark-cassandra-connector_2.11" % "2.0.5"  % "provided",
  "org.apache.cassandra" % "cassandra-all" % "3.11.2",
  "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided",
  "org.apache.spark" % "spark-mllib_2.11" % "2.2.0" % "provided",
  "org.apache.zookeeper" % "zookeeper" % "3.4.6" % "provided",
  "com.101tec" % "zkclient" % "0.4" % "provided"
)

updateOptions := updateOptions.value.withCachedResolution(true)

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)

libraryDependencies ~= {_
  .map(_.exclude("javax.jms", "jms"))
  .map(_.exclude("com.sun.jdmk", "jmxtools"))
  .map(_.exclude("com.sun.jmx", "jmxri"))
}

assemblyShadeRules in assembly := Seq(
  ShadeRule.rename("com.google.common.**" -> "shaded.google.common.@1").inAll
)

assemblyMergeStrategy in assembly := {
  case x => MergeStrategy.first
}