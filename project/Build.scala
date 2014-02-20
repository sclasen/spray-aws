import sbt._
import Keys._
import com.typesafe.sbt.SbtScalariform._

object Build extends Build {


  val buildSettings = Seq(
    organization := "com.sclasen",
    version := "0.2.5-SNAPSHOT",
    scalaVersion := "2.10.2",
    crossScalaVersions := Seq("2.10.2"),
    scalacOptions ++= Seq("-feature", "-deprecation", "-language:implicitConversions", "-language:postfixOps"),
    resolvers ++= Seq("TypesafeMaven" at "http://repo.typesafe.com/typesafe/maven-releases",
      "whydoineedthis" at "http://repo.typesafe.com/typesafe/releases",
      "spray nightlies" at "http://nightlies.spray.io/",
      "spray repo" at "http://repo.spray.io", "oss" at "https://oss.sonatype.org/")
  ) ++ Defaults.defaultSettings  ++ publishSettings  ++ scalariformSettings

  val spray_aws = Project(
    id = "spray-aws",
    base = file("spray-aws"),
    settings = buildSettings ++ Seq(libraryDependencies ++= deps)
  )

  val spray_dynamodb = Project(
    id = "spray-dynamodb",
    base = file("spray-dynamodb"),
    settings = buildSettings ++ Seq(libraryDependencies ++= deps)
  ).dependsOn(spray_aws)

  val spray_sqs = Project(
    id = "spray-sqs",
    base = file("spray-sqs"),
    settings = buildSettings ++ Seq(libraryDependencies ++= deps)
  ).dependsOn(spray_aws)

  val spray_kinesis = Project(
    id = "spray-kinesis",
    base = file("spray-kinesis"),
    settings = buildSettings ++ Seq(libraryDependencies ++= deps)
  ).dependsOn(spray_aws)

  val root = Project(id = "spray-aws-project", base = file("."), settings = buildSettings ++ parentSettings).aggregate(spray_aws,spray_dynamodb,spray_kinesis,spray_sqs)



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

  def deps = Seq(aws, spray, akka, scalaTest, akka_testkit)

  val aws = "com.amazonaws" % "aws-java-sdk" % "1.7.1" % "compile"
  val spray = "io.spray" % "spray-client" % "1.3.0" % "compile"
  val akka = "com.typesafe.akka" %% "akka-actor" % "2.3.0" % "compile"
  val akka_testkit = "com.typesafe.akka" %% "akka-testkit" % "2.3.0" % "test"
  val scalaTest   = "org.scalatest"     %% "scalatest"                 % "2.0" % "test"
}
