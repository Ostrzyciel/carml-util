ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.1"

lazy val pekkoV = "1.0.1"
lazy val carmlV = "0.4.9"
// lazy val carmlV = "0.3.2"

lazy val root = (project in file("."))
  .settings(
    name := "carml-util",
    libraryDependencies ++= Seq(
      "io.carml" % "carml-engine" % carmlV,
      "io.carml" % "carml-logical-source-resolver-jsonpath" % carmlV,
//      "com.taxonic.carml" % "carml-engine" % carmlV,
//      "com.taxonic.carml" % "carml-logical-source-resolver-jsonpath" % carmlV,
      "org.apache.pekko" %% "pekko-actor-typed" % pekkoV,
      "org.apache.pekko" %% "pekko-stream-typed" % pekkoV,
    )
  )
