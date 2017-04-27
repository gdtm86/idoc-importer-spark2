import _root_.sbtassembly.AssemblyPlugin.autoImport._
import _root_.sbtassembly.PathList

name := "idoc-importer-spark2"

version := "1.0"

scalaVersion := "2.11.10"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-client" % "2.6.0-cdh5.10.0" % "provided" excludeAll ExclusionRule(organization = "javax.servlet") ,
  "org.apache.spark" %% "spark-streaming" % "2.1.0.cloudera1" % "provided",
  "org.apache.spark" %% "spark-core" % "2.1.0.cloudera1" % "provided" excludeAll ExclusionRule(organization = "javax.servlet"),
  "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.1.0.cloudera1" % "provided",
  "org.apache.spark" %% "spark-sql" % "2.1.0.cloudera1" % "provided",
  "org.apache.spark" %% "spark-hive" % "2.1.0.cloudera1" % "provided",
  "org.apache.kudu" % "kudu-client" % "1.2.0-cdh5.10.0" % "provided", //excludeAll ExclusionRule(organization = "org.slf4j"),
  "org.apache.kudu" %% "kudu-spark2" % "1.2.0-cdh5.10.0",
  "com.typesafe.play" %% "play-json" % "2.3.8",
  "org.apache.hive" % "hive-serde" % "1.1.0-cdh5.10.0" % "provided",
  "org.apache.hive" % "hive-metastore" % "1.1.0-cdh5.10.0" % "provided",
  "com.twitter" % "parquet-avro" % "1.5.0-cdh5.10.0" % "provided",
  "com.twitter" % "parquet-hadoop" % "1.5.0-cdh5.10.0" % "provided",
  "com.twitter" % "parquet-column" % "1.5.0-cdh5.10.0" % "provided",
  "com.twitter" % "parquet-common" % "1.5.0-cdh5.10.0" % "provided",
  "com.twitter" % "parquet-encoding" % "1.5.0-cdh5.10.0" % "provided",
  "org.apache.avro" % "avro-mapred" % "1.7.6-cdh5.10.0" % "provided",
  "org.apache.avro" % "avro-ipc" % "1.7.6-cdh5.10.0" % "provided"
  //"org.scala-lang" % "scala-reflect" % "2.11.10" % "provided",
  //"org.scala-lang" % "scala-compiler" % "2.11.10" % "provided"
)


assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs @ _*) => MergeStrategy.last
  case PathList("javax", "activation", xs @ _*) => MergeStrategy.last
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case PathList("com", "google", xs @ _*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs @ _*) => MergeStrategy.last
  case PathList("com", "codahale", xs @ _*) => MergeStrategy.last
  case PathList("com", "yammer", xs @ _*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

resolvers ++= Seq(
  "Akka Repository" at "http://repo.akka.io/releases/",
  "Maven Central Server" at "http://repo1.maven.org/maven2",
  "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos",
  "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/"
)