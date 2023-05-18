// The simplest possible sbt build file is just one line:
import sbt.Credentials

val repoPass = sys.env.getOrElse("CODEARTIFACT_AUTH_TOKEN", "")

credentials += Credentials("main", "plexq-269378281721.d.codeartifact.us-west-2.amazonaws.com", "aws", repoPass)

//logLevel := Level.Debug

resolvers += "plexq repo" at "https://plexq-269378281721.d.codeartifact.us-west-2.amazonaws.com/maven/main/"

publishMavenStyle := true
publishTo := Some("aws" at "https://plexq-269378281721.d.codeartifact.us-west-2.amazonaws.com/maven/main/")

scalaVersion := "2.13.8"

name := "dynamo-scala-connector"
organization := "org.plexq"
version := "0.1"

libraryDependencies ++= Seq(
    "com.amazonaws" % "aws-java-sdk" % "1.11.889",
    "org.scalatest" %% "scalatest" % "3.0.8" % Test,
    "com.typesafe" % "config" % "1.4.2",
    "com.typesafe.play" %% "play-json" % "2.9.4",
    "org.slf4j" % "slf4j-api" % "2.0.7",
    "com.google.inject" % "guice" % "5.1.0",
    "net.codingwell" %% "scala-guice" % "5.1.1"
)
