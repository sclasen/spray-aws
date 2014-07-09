package com.sclasen.spray.aws.s3

import akka.actor.{ ActorRefFactory, ActorSystem }

import com.sclasen.spray.aws._

case class S3ClientProps(key: String, secret: String, operationTimeout: Timeout, system: ActorSystem, factory: ActorRefFactory, endpoint: String = "https://s3.amazonaws.com") extends SprayAWSClientProps {
  val service = "s3"
}

class S3Client {



}
