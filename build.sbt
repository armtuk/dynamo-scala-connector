
// The simplest possible sbt build file is just one line:

scalaVersion := "2.13.8"

name := "dynamo-scala-connector"
organization := "org.plexq"
version := "0.1"

libraryDependencies ++= Seq(
    "org.scala-lang.modules" %% "scala-parser-combinators" % "2.1.1",
    "com.amazonaws" % "aws-java-sdk" % "1.11.889",
    "org.scalatest" %% "scalatest" % "3.0.8" % Test,
    "com.typesafe" % "config" % "1.4.2",
    "com.typesafe.play" %% "play-json" % "2.9.4",
    "org.slf4j" % "slf4j-api" % "2.0.7",
    "com.google.inject" % "guice" % "5.1.0",
    "net.codingwell" %% "scala-guice" % "5.1.1"
)

