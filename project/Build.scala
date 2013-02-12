import sbt._
import Keys._
import com.typesafe.sbt.SbtScalariform._

object Build extends Build {


  val buildSettings = Seq(
    organization := "com.sclasen",
    version := "0.1.1-SNAPSHOT",
    scalaVersion := "2.10.0",
    crossScalaVersions := Seq("2.10.0"),
    resolvers ++= Seq("TypesafeMaven" at "http://repo.typesafe.com/typesafe/maven-releases", "whydoineedthis" at "http://repo.typesafe.com/typesafe/releases")
  ) ++ Defaults.defaultSettings  ++ publishSettings

  val stub = Project(
    id = "spray-aws-httpclient-stub",
    base = file("spray-httpclient-stub"),
    settings = buildSettings
  )

  val spray_aws = Project(
    id = "spray-aws",
    base = file("spray-aws"),
    settings = buildSettings ++ Seq(libraryDependencies ++= dependencies)
  ).dependsOn(stub)

  val spray_dyanmodb = Project(
    id = "spray-dynamodb",
    base = file("spray-dynamodb"),
    dependencies = Seq(spray_aws),
    settings = buildSettings ++ Seq(libraryDependencies ++= dependencies)
  ).dependsOn(spray_aws)

  val root = Project(id = "top", base = file("."), settings = buildSettings ++ parentSettings).aggregate(stub,spray_aws,spray_dyanmodb)


  def publishSettings: Seq[Setting[_]] = Seq(
    // If we want on maven central, we need to be in maven style.
    publishMavenStyle := true,
    publishArtifact in Test := false,
    // The Nexus repo we're publishing to.
    publishTo <<= version {
      (v: String) =>
        val nexus = "https://oss.sonatype.org/"
        if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
        else Some("releases" at nexus + "service/local/staging/deploy/maven2")
    },
    // Maven central cannot allow other repos.  We're ok here because the artifacts we
    // we use externally are *optional* dependencies.
    pomIncludeRepository := {
      x => false
    },
    // Maven central wants some extra metadata to keep things 'clean'.
    pomExtra := (

      <url>http://github.com/sclasen/spray-aws</url>
        <licenses>
          <license>
            <name>The Apache Software License, Version 2.0</name>
            <url>http://www.apache.org/licenses/LICENSE-2.0.html</url>
            <distribution>repo</distribution>
          </license>
        </licenses>
        <scm>
          <url>git@github.com:sclasen/spray-aws.git</url>
          <connection>scm:git:git@github.com:sclasen/spray-aws.git</connection>
        </scm>
        <developers>
          <developer>
            <id>sclasen</id>
            <name>Scott Clasen</name>
            <url>http://github.com/sclasen</url>
          </developer>
        </developers>)


  )

  lazy val parentSettings = Seq(
    publishArtifact in Compile := false
  )

  def dependencies = Seq(aws, spray, metrics, akka, scalaTest, akka_testkit)

  val aws = "com.amazonaws" % "aws-java-sdk" % "1.3.30" % "compile" exclude("org.apache.httpcomponents", "httpclient") exclude("org.apache.httpcomponents", "httpcore")
  val spray = "io.spray" % "spray-client" % "1.1-M7" % "compile"
  val metrics = "com.yammer.metrics" % "metrics-core" % "2.2.0" % "compile"
  val akka = "com.typesafe.akka" %% "akka-actor" % "2.1.0" % "compile"
  val akka_testkit = "com.typesafe.akka" %% "akka-testkit" % "2.1.0" % "test"
  val scalaTest   = "org.scalatest"     %% "scalatest"                 % "1.9.1" % "test"



}

