ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "3.3.6"

lazy val akkaVersion = settingKey[String]("The version of Akka used throughout the build.")

ThisBuild / akkaVersion := "2.10.3"
resolvers += "Akka library repository".at("https://repo.akka.io/maven")
lazy val root = (project in file("."))
  .settings(
    name := "akka-raft"
  )

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor-typed" % akkaVersion.value,
  "ch.qos.logback" % "logback-classic" % "1.5.18",
  "com.typesafe.akka" %% "akka-actor-testkit-typed" % akkaVersion.value % Test,
  "org.scalatest" %% "scalatest" % "3.2.15" % Test,
  "com.typesafe.akka" %% "akka-http" % "10.5.3",
  "com.typesafe.akka" %% "akka-remote" % akkaVersion.value
)
