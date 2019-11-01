name := "Spark Project"

version := "0.1"

scalaVersion := "2.12.8"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.0"

libraryDependencies += "com.typesafe" % "config" % "1.2.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"

resolvers ++= Seq("Akka Repository" at "http://repo.akka.io/releases/")

resolvers ++= Seq("Sonatype Releases" at "https://oss.sonatype.org/content/repositories/releases")