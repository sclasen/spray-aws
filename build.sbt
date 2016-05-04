name := "spray-aws"

organization := "com.sclasen"

version := "0.3.7-SNAPSHOT"

scalaVersion := "2.11.2"

crossScalaVersions := Seq("2.10.4", "2.11.2")

scalacOptions ++= Seq("-feature", "-deprecation", "-language:implicitConversions", "-language:postfixOps")

resolvers ++= Seq(
  "TypesafeMaven" at "http://repo.typesafe.com/typesafe/maven-releases",
  "whydoineedthis" at "http://repo.typesafe.com/typesafe/releases",
  "oss" at "https://oss.sonatype.org/"
)

def deps = Seq(aws, akka, scalaTest, akka_testkit, akka_http, akka_http_core, akka_http_spray)

val akkaVersion = "2.3.5"
val akkaHttpVersion = "1.0"

val aws = "com.amazonaws" % "aws-java-sdk" % "1.8.9.1" % "compile"
val akka = "com.typesafe.akka" %% "akka-actor" % akkaVersion % "compile"
val akka_http = "com.typesafe.akka" %% "akka-http-experimental" % akkaHttpVersion
val akka_http_core = "com.typesafe.akka" %% "akka-http-core-experimental" % akkaHttpVersion
val akka_http_spray = "com.typesafe.akka" %% "akka-http-spray-json-experimental" % akkaHttpVersion
val akka_testkit = "com.typesafe.akka" %% "akka-testkit" % akkaVersion % "test"
val scalaTest   = "org.scalatest"     %% "scalatest"   % "2.2.1" % "test"

libraryDependencies ++= deps

lazy val root = project
  .in(file("."))
  .settings(libraryDependencies ++= deps)
  .aggregate(spray_aws, spray_dynamodb, spray_kinesis, spray_sqs, spray_route53, spray_s3)

lazy val spray_aws = project
  .in(file("spray-aws"))
  .settings(libraryDependencies ++= deps)

lazy val spray_dynamodb = project
  .in(file("spray-dynamodb"))
  .settings(libraryDependencies ++= deps)
  .dependsOn(spray_aws)

lazy val spray_sqs = project
  .in(file("spray-sqs"))
  .settings(libraryDependencies ++= deps)
  .dependsOn(spray_aws)

lazy val spray_kinesis = project
  .in(file("spray-kinesis"))
  .settings(libraryDependencies ++= deps)
  .dependsOn(spray_aws)

lazy val spray_route53 = project
  .in(file("spray-route53"))
  .settings(libraryDependencies ++= deps)
  .dependsOn(spray_aws)

lazy val spray_s3 = project
  .in(file("spray-s3"))
  .settings(libraryDependencies ++= deps)
  .dependsOn(spray_aws)

// If we want on maven central, we need to be in maven style.
publishMavenStyle := true

publishArtifact in Test := false

// The Nexus repo we're publishing to.
publishTo <<= version {
  (v: String) =>
    val nexus = "https://oss.sonatype.org/"
    if (v.trim.endsWith("SNAPSHOT")) Some("snapshots" at nexus + "content/repositories/snapshots")
    else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

// Maven central cannot allow other repos.  We're ok here because the artifacts we
// we use externally are *optional* dependencies.
pomIncludeRepository := {
  x => false
}

// Maven central wants some extra metadata to keep things 'clean'.
pomExtra :=
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
    </developers>

publishArtifact in Compile := false

fork in Test := true
