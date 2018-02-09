name := "dmon-api"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "com.typesafe.akka"       %% "akka-http" % "10.0.4",
  "com.github.nscala-time"  %% "nscala-time" % "2.16.0",
  "com.typesafe.akka"       %% "akka-http-spray-json" % "10.0.4",
  "org.apache.spark"        %% "spark-core" % "2.0.2" % "provided"
    exclude ("org.scalatest", "scalatest_2.11"),
  "org.apache.spark"        %% "spark-sql" % "2.0.2" % "provided",
  "org.apache.spark"        %% "spark-hive" % "2.0.2" % "provided",
  "org.clapper"             %% "grizzled-slf4j" % "1.3.0",
  "org.scalatest"           %% "scalatest"     % "3.0.1" % "test",
  "org.scalamock"           %% "scalamock-scalatest-support" % "3.5.0" % "test",
  "com.typesafe.akka"       %% "akka-http-testkit" % "10.0.4" % "test",
  "com.databricks"          %% "spark-csv" % "1.5.0" % "test",
  "com.enragedginger"       %% "akka-quartz-scheduler" % "1.6.0-akka-2.4.x"
)

assemblyMergeStrategy in assembly := {
  case PathList("javax", "inject", xs @ _*) => MergeStrategy.first
  case PathList("org", "aopalliance", xs @ _*) => MergeStrategy.first
  case PathList("org", "apache", xs @ _*) => MergeStrategy.first
  case PathList("org", "objenesis", xs @ _*) => MergeStrategy.first
  case "parquet.thrift" => MergeStrategy.first
  case "plugin.xml" => MergeStrategy.first
  case x => {
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
  }
}

parallelExecution in Test := false