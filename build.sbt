ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.15"

libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "7.0.1" % Test

libraryDependencies ++= Seq(
  // "org.mockito" % "mockito-core" % "3.5.13" % Test
  "org.apache.spark" %% "spark-sql" % "3.5.5",
  "org.apache.spark" %% "spark-core" % "3.5.5",
   "org.apache.hadoop" %% "hadoop-hdfs" % "3.3.6",
  "org.apache.hadoop" %% "hadoop-client" % "3.3.6",
  "com.amazonaws" % "aws-java-sdk" % "1.12.160", // AWS SDK for S3
  // https://mvnrepository.com/artifact/za.co.absa.cobrix/spark-cobol
  "za.co.absa.cobrix" %% "spark-cobol" % "2.9.1",
  // https://mvnrepository.com/artifact/com.softwaremill.sttp.client3/core
  "com.softwaremill.sttp.client3" %% "core" % "3.11.0",
  "com.softwaremill.sttp.client3" %% "okhttp-backend" % "3.11.0",
  "org.scalatest" %% "scalatest" % "3.2.19",
  "org.scalamock" %% "scalamock" % "6.0.0"
)


lazy val root = (project in file("."))
  .settings(
    name := "spark-minio"
  )
