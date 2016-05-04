package com.sclasen.spray.aws.route53

import org.scalatest.{ MustMatchers, WordSpec }
import akka.stream.ActorMaterializer
import akka.actor.ActorSystem
import akka.util.Timeout
import concurrent.duration._
import scala.concurrent.Await
import com.amazonaws.services.route53.model.{ ListHostedZonesRequest, DeleteHostedZoneRequest, HostedZoneConfig, CreateHostedZoneRequest }

class Route53ClientSpec extends WordSpec with MustMatchers {

  "A Route53Client" must {
    "Work" in {
      implicit val system = ActorSystem("test")
      val materializer = ActorMaterializer()
      val props = Route53ClientProps(sys.env("AWS_ACCESS_KEY_ID"), sys.env("AWS_SECRET_ACCESS_KEY"), Timeout(100 seconds), system, system, materializer)
      val client = new Route53Client(props)
      val ref = "testing" + System.currentTimeMillis()
      val req = new CreateHostedZoneRequest().withName("www.ticktock.com.")
        .withCallerReference(ref)
      val result = Await.result(client.sendCreateHostedZone(req), 100 seconds)
      result.getHostedZone.getName must be("www.ticktock.com.")

      val list = Await.result(client.sendListHostedZones(new ListHostedZonesRequest()), 10 seconds)
      list.getHostedZones.size() must be > 1

      val dreq = new DeleteHostedZoneRequest().withId(result.getHostedZone.getId)
      val dresult = Await.result(client.sendDeleteHostedZone(dreq), 100 seconds)

    }
  }
}
