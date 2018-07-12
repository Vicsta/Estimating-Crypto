name := """play-scala-starter-example"""

version := "1.0-SNAPSHOT"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

resolvers += Resolver.sonatypeRepo("snapshots")

scalaVersion := "2.12.6"

crossScalaVersions := Seq("2.11.12", "2.12.6")

libraryDependencies += guice
libraryDependencies += "org.scalatestplus.play" %% "scalatestplus-play" % "3.1.2" % Test
libraryDependencies += "com.h2database" % "h2" % "1.4.197"
libraryDependencies += "org.apache.spark" % "spark-core_2.12" % "2.1.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.12" % "2.1.1"
libraryDependencies += "com.typesafe.akka" %% "akka-actor" % "2.2.3"
libraryDependencies += "com.typesafe.akka" %% "akka-slf4j" % "2.2.3"
